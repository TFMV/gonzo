package flight

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/TFMV/gonzo/auth"
	"github.com/TFMV/gonzo/db"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// IngestionAuditLog holds metadata about record ingestion.
type IngestionAuditLog struct {
	Timestamp     time.Time
	RecordCount   int
	Checksum      string
	ClientID      string
	SchemaVersion int
	Status        string // "success" or "failed"
}

// SchemaManager handles schema registration and validation.
type SchemaManager struct {
	schemas map[string]*arrow.Schema
	mu      sync.RWMutex
}

// NewSchemaManager returns a new SchemaManager.
func NewSchemaManager() *SchemaManager {
	return &SchemaManager{
		schemas: make(map[string]*arrow.Schema),
	}
}

// RegisterSchema stores the expected schema for a given endpoint.
func (sm *SchemaManager) RegisterSchema(endpoint string, schema *arrow.Schema) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.schemas[endpoint] = schema
}

// Validate checks that the record’s schema matches the registered schema.
func (sm *SchemaManager) Validate(rec arrow.Record, endpoint string) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	expected, exists := sm.schemas[endpoint]
	if !exists {
		return fmt.Errorf("unregistered endpoint: %s", endpoint)
	}
	if !rec.Schema().Equal(expected) {
		return fmt.Errorf("schema mismatch for endpoint %s", endpoint)
	}
	return nil
}

// StreamConfig controls Flight stream behavior.
type StreamConfig struct {
	Compression  compress.Compression
	BatchSize    int
	MaxRetries   int
	SchemaPolicy SchemaManager
}

// RetryPolicy defines retry logic for operations that may fail transiently.
type RetryPolicy struct {
	MaxAttempts     int
	Backoff         time.Duration
	RetryableErrors map[codes.Code]bool
}

// Execute runs the operation with retry logic.
func (p *RetryPolicy) Execute(op func() error) error {
	var err error
	for attempt := 0; attempt < p.MaxAttempts; attempt++ {
		err = op()
		if err == nil {
			return nil
		}
		st, ok := status.FromError(err)
		if !ok || !p.RetryableErrors[st.Code()] {
			return err
		}
		time.Sleep(p.Backoff * (1 << attempt))
	}
	return err
}

// ColumnProcessor applies processing functions to individual columns.
type ColumnProcessor struct {
	processors map[string]func(arrow.Array) error
}

// NewColumnProcessor returns a new ColumnProcessor.
func NewColumnProcessor() *ColumnProcessor {
	return &ColumnProcessor{
		processors: make(map[string]func(arrow.Array) error),
	}
}

// AddProcessor registers a processor function for a column.
func (cp *ColumnProcessor) AddProcessor(column string, fn func(arrow.Array) error) {
	cp.processors[column] = fn
}

// Process applies all registered processors to their corresponding columns.
func (cp *ColumnProcessor) Process(rec arrow.Record) error {
	for i := 0; i < int(rec.NumCols()); i++ {
		field := rec.Schema().Field(i)
		if processor, exists := cp.processors[field.Name]; exists {
			if err := processor(rec.Column(i)); err != nil {
				return fmt.Errorf("processing column %s: %w", field.Name, err)
			}
		}
	}
	return nil
}

// GonzoFlightService implements the Flight service.
type GonzoFlightService struct {
	flight.BaseFlightServer
	gonzoDB       *db.DB
	roleManager   auth.RoleManager
	dlq           db.DeadLetterQueue
	schemaManager *SchemaManager
	pool          memory.Allocator
	retryPolicy   *RetryPolicy
	processor     *ColumnProcessor
}

// NewGonzoFlightService constructs a new GonzoFlightService.
func NewGonzoFlightService(gonzoDB *db.DB, roleManager auth.RoleManager, dlq db.DeadLetterQueue) *GonzoFlightService {
	return &GonzoFlightService{
		gonzoDB:       gonzoDB,
		roleManager:   roleManager,
		dlq:           dlq,
		schemaManager: NewSchemaManager(),
		pool:          memory.NewGoAllocator(),
		retryPolicy: &RetryPolicy{
			MaxAttempts:     3,
			Backoff:         1 * time.Second,
			RetryableErrors: map[codes.Code]bool{codes.Unavailable: true},
		},
		processor: NewColumnProcessor(),
	}
}

// decodeQuery decodes the query from the ticket bytes.
// This is a stub and should be replaced with your actual query parsing.
func decodeQuery(data []byte) (*db.Query, error) {
	// For example purposes only.
	return &db.Query{Window: 0}, nil
}

// DoGet handles Flight DoGet requests.
func (s *GonzoFlightService) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	q, err := decodeQuery(ticket.GetTicket())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid ticket: %v", err)
	}

	records, err := s.gonzoDB.Query(q)
	if err != nil {
		return status.Errorf(codes.Internal, "query failed: %v", err)
	}
	if len(records) == 0 {
		return status.Error(codes.NotFound, "no records found")
	}

	// Ensure that the stream supports writing.
	writer, ok := stream.(flight.DataStreamWriter)
	if !ok {
		return status.Error(codes.Internal, "stream does not support DataStreamWriter")
	}

	recordWriter := flight.NewRecordWriter(writer, ipc.WithSchema(records[0].Schema()))
	defer recordWriter.Close()

	for _, rec := range records {
		if err := recordWriter.Write(rec); err != nil {
			return status.Errorf(codes.Internal, "failed to write record: %v", err)
		}
		rec.Release() // release resources after writing
	}
	return nil
}

// DoPut handles Flight DoPut requests for ingesting records.
func (s *GonzoFlightService) DoPut(stream flight.FlightService_DoPutServer) error {
	// Authorize the ingest operation.
	if err := s.authorizeIngest(stream.Context()); err != nil {
		return err
	}

	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return status.Errorf(codes.Internal, "error reading records: %v", err)
	}
	defer reader.Release()

	// Process each record from the stream.
	for reader.Next() {
		rec := reader.Record()
		if err := s.ingestRecord(rec); err != nil {
			// Write to the dead-letter queue on failure.
			s.dlq.Write(rec)
			continue
		}
	}
	if err := reader.Err(); err != nil {
		return status.Errorf(codes.Internal, "error processing records: %v", err)
	}
	return nil
}

// authorizeIngest verifies that the client is permitted to ingest data.
func (s *GonzoFlightService) authorizeIngest(ctx context.Context) error {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "no peer info")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return status.Error(codes.Unauthenticated, "no client certificate")
	}
	clientCert := tlsInfo.State.PeerCertificates[0]
	if !s.roleManager.HasRole(clientCert.Subject.CommonName, "data_ingester") {
		return status.Error(codes.PermissionDenied, "insufficient privileges")
	}
	return nil
}

// validateRecord checks that the record meets basic criteria.
func (s *GonzoFlightService) validateRecord(rec arrow.Record) error {
	expectedSchema := s.gonzoDB.GetSchema()
	if !rec.Schema().Equal(expectedSchema) {
		return fmt.Errorf("schema mismatch: got %v, want %v", rec.Schema(), expectedSchema)
	}
	if rec.NumRows() == 0 {
		return errors.New("empty record ingestion prohibited")
	}
	return nil
}

// ingestRecord validates and asynchronously ingests the record.
func (s *GonzoFlightService) ingestRecord(rec arrow.Record) error {
	// Validate the record.
	if err := s.validateRecord(rec); err != nil {
		return err
	}

	// Optionally, process columns (with retries).
	if err := s.retryPolicy.Execute(func() error {
		return s.processor.Process(rec)
	}); err != nil {
		return err
	}

	// Ingest the record asynchronously.
	s.gonzoDB.AsyncIngest(rec)
	return nil
}

// ProcessStream demonstrates how to read, process, and write records
// using both a DataStreamReader and DataStreamWriter. This helper is
// optional and illustrates additional streaming logic with retries.
func (s *GonzoFlightService) ProcessStream(reader flight.DataStreamReader, writer flight.DataStreamWriter) error {
	recordReader, err := flight.NewRecordReader(reader)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create record reader: %v", err)
	}
	defer recordReader.Release()

	recordWriter := flight.NewRecordWriter(writer, ipc.WithSchema(recordReader.Schema()))
	defer recordWriter.Close()

	for recordReader.Next() {
		rec := recordReader.Record()
		// Process the record with retry logic.
		if err := s.retryPolicy.Execute(func() error {
			return s.processor.Process(rec)
		}); err != nil {
			s.dlq.Write(rec)
			continue
		}
		if err := recordWriter.Write(rec); err != nil {
			return status.Errorf(codes.Internal, "failed to write record: %v", err)
		}
		rec.Release()
	}
	if err := recordReader.Err(); err != nil {
		return status.Errorf(codes.Internal, "error reading records: %v", err)
	}
	return nil
}

// SliceRecord creates a new record containing only the specified row range.
// It returns an error if the offset or length is invalid.
func SliceRecord(rec arrow.Record, offset, length int64) (arrow.Record, error) {
	if offset < 0 || length < 0 || offset+length > rec.NumRows() {
		return nil, fmt.Errorf("invalid slice range: offset %d, length %d, total rows %d",
			offset, length, rec.NumRows())
	}
	slices := make([]arrow.Array, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		// Note: array.NewSlice creates a shallow slice of the data.
		slices[i] = array.NewSlice(rec.Column(i), offset, offset+length)
	}
	return array.NewRecord(rec.Schema(), slices, length), nil
}

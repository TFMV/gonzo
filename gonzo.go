// main.go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/docopt/docopt.go"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------
// Asset Embedding (using go-bindata)
// ---------------------------------------------------------------------
// In a real project you would run something like:
//
//	//go:generate go-bindata -o bindata.go assets/
//
// and then load embedded files via Asset("assets/query.sql").
// For demonstration purposes, we embed a SQL query as a string:
var embeddedQuerySQL = `
-- Embedded SQL Query to compute total purchases
SELECT SUM(purchase_amount) AS total_purchases FROM transactions;
`

// ---------------------------------------------------------------------
// Arrow schema and helper to simulate streaming records.
// ---------------------------------------------------------------------

var (
	// Use the GoAllocator to allocate Arrow buffers.
	pool = memory.NewGoAllocator()

	// Define a simple schema with three fields.
	schema = arrow.NewSchema([]arrow.Field{
		{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "purchase_amount", Type: arrow.PrimitiveTypes.Float64},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_s},
	}, nil)
)

// GenerateStream creates an Arrow record batch simulating a stream
// of transactions. (In production, record batches might be ingested
// from an external source or client.)
func GenerateStream() arrow.Record {
	// Create a RecordBuilder for our schema.
	builder := array.NewRecordBuilder(pool, schema)
	// Note: Each field builder must be released *after* the record is created.
	// We defer the release of the builder (which releases underlying buffers)
	// once we have extracted the record.
	defer builder.Release()

	// Get builders for each column.
	userIDs := builder.Field(0).(*array.Int64Builder)
	purchaseAmounts := builder.Field(1).(*array.Float64Builder)
	timestamps := builder.Field(2).(*array.TimestampBuilder)

	// Simulated transaction data.
	users := []int64{101, 102, 101, 103, 102}
	amounts := []float64{15.5, 20.0, 50.0, 30.0, 10.0}
	now := time.Now().Unix()
	// Append values to each column.
	for i := range users {
		userIDs.Append(users[i])
		purchaseAmounts.Append(amounts[i])
		timestamps.Append(arrow.Timestamp(now))
	}
	// Create and return the record batch.
	record := builder.NewRecord()
	return record
}

// ---------------------------------------------------------------------
// ArrowDB: An in-memory embedded database for Arrow record batches.
// ---------------------------------------------------------------------

// ArrowDB holds ingested Arrow record batches and exposes query methods.
type ArrowDB struct {
	mu      sync.RWMutex
	records []arrow.Record
}

// NewArrowDB initializes a new ArrowDB instance.
func NewArrowDB() *ArrowDB {
	return &ArrowDB{
		records: make([]arrow.Record, 0),
	}
}

// Ingest adds an Arrow record batch to the database.
// (In production, consider validation and schema checking.)
func (db *ArrowDB) Ingest(record arrow.Record) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.records = append(db.records, record)
}

// QueryTotalPurchases aggregates the total purchase amounts from all ingested records.
// For simplicity we iterate the purchase_amount column (assumed to be at index 1).
func (db *ArrowDB) QueryTotalPurchases(ctx context.Context) (float64, error) {
	var total float64
	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, record := range db.records {
		// Retrieve the purchase_amount column.
		arr := record.Column(1)
		floatArr, ok := arr.(*array.Float64)
		if !ok {
			return 0, fmt.Errorf("unexpected column type: %T", arr)
		}
		// Iterate over the column values.
		for i := 0; i < floatArr.Len(); i++ {
			if floatArr.IsNull(i) {
				continue
			}
			total += floatArr.Value(i)
		}
	}
	return total, nil
}

// Close releases all Arrow record batches held by the database.
func (db *ArrowDB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, record := range db.records {
		record.Release()
	}
	db.records = nil
}

// streamQueryResults periodically computes the total purchases and
// sends the result over the provided channel. This simulates a streaming
// query engine that pushes results to clients.
func streamQueryResults(ctx context.Context, db *ArrowDB, interval time.Duration, results chan<- float64, logger *zap.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Info("streamQueryResults: context cancelled")
			return
		case <-ticker.C:
			sum, err := db.QueryTotalPurchases(ctx)
			if err != nil {
				logger.Error("failed to query total purchases", zap.Error(err))
				continue
			}
			results <- sum
		}
	}
}

// ---------------------------------------------------------------------
// Main: CLI parsing, ingestion, streaming query, and graceful shutdown.
// ---------------------------------------------------------------------

func main() {
	// Define CLI usage.
	usage := `ArrowDB Embedded Database.

Usage:
  arrowdb serve [--ingest-interval=<seconds>] [--query-interval=<seconds>]
  arrowdb (-h | --help)
  arrowdb --version

Options:
  -h --help                    Show this screen.
  --version                    Show version.
  --ingest-interval=<seconds>  Interval in seconds for ingesting new data [default: 2].
  --query-interval=<seconds>   Interval in seconds for streaming query results [default: 2].
`

	// Parse CLI arguments using docopt.
	arguments, err := docopt.ParseDoc(usage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing arguments: %v\n", err)
		os.Exit(1)
	}
	if v, _ := arguments.Bool("--version"); v {
		fmt.Println("ArrowDB version 1.0.0")
		os.Exit(0)
	}
	ingestInterval, err := arguments.Int("--ingest-interval")
	if err != nil {
		ingestInterval = 2
	}
	queryInterval, err := arguments.Int("--query-interval")
	if err != nil {
		queryInterval = 2
	}

	// Initialize zap logger.
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Log the embedded asset (e.g. a SQL query) to demonstrate asset embedding.
	logger.Info("Loaded embedded asset", zap.String("query", embeddedQuerySQL))

	// Create a new ArrowDB instance.
	db := NewArrowDB()
	defer db.Close()

	// Create a channel to stream query results.
	resultsCh := make(chan float64)

	// Create a context to control goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start streaming query results in a separate goroutine.
	go streamQueryResults(ctx, db, time.Duration(queryInterval)*time.Second, resultsCh, logger)

	// Simulate ingestion of new Arrow record batches periodically.
	ingestTicker := time.NewTicker(time.Duration(ingestInterval) * time.Second)
	defer ingestTicker.Stop()

	// Listen for OS signals for graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("Starting ArrowDB streaming engine",
		zap.Int("ingest_interval", ingestInterval),
		zap.Int("query_interval", queryInterval))

	// Main event loop.
	for {
		select {
		case <-ctx.Done():
			logger.Info("Main loop: context cancelled, shutting down")
			return
		case sig := <-sigCh:
			logger.Info("Received OS signal, shutting down", zap.String("signal", sig.String()))
			cancel()
			return
		case <-ingestTicker.C:
			// Generate and ingest a new record batch.
			record := GenerateStream()
			db.Ingest(record)
			logger.Info("Ingested new record batch", zap.Int("num_records", int(record.NumRows())))
			// In production, you might want to release or archive batches after processing.
		case total := <-resultsCh:
			logger.Info("Streaming query result", zap.Float64("total_purchases", total))
		}
	}
}

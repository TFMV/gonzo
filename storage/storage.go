package gonzo_storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"cloud.google.com/go/storage"
	"github.com/TFMV/gonzo/db"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/api/iterator"
)

// Storage wraps a DB instance to provide Save/Load functionality.
type Storage struct {
	db *db.DB
}

// NewStorage creates a new Storage instance for the given DB.
func NewStorage(db *db.DB) *Storage {
	return &Storage{
		db: db,
	}
}

// SaveToDisk writes the current contents of the DB to a file on disk
// in the Arrow IPC file format. The schema is written first, followed
// by each Record in the database.
func (s *Storage) SaveToDisk(filepath string) error {
	// 1. Open file for writing
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create file %q: %w", filepath, err)
	}
	defer func() {
		_ = file.Close()
	}()

	// 2. Create an Arrow IPC FileWriter with the DB's schema.
	writer, err := ipc.NewFileWriter(
		file,
		ipc.WithSchema(s.db.GetSchema()),
		ipc.WithAllocator(memory.NewGoAllocator()),
	)
	if err != nil {
		return fmt.Errorf("failed to create Arrow file writer: %w", err)
	}
	defer func() {
		_ = writer.Close()
	}()

	// 3. Retrieve all records from the DB and write them out.
	records := s.db.GetRecords()
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record to Arrow file: %w", err)
		}
	}

	return nil
}

// LoadFromDisk reads Arrow IPC file contents from disk and ingests them
// into the DB. This uses the DB's AsyncIngest, so new data is batched
// into the DB asynchronously.
func (s *Storage) LoadFromDisk(filepath string) error {
	// 1. Open file for reading
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("failed to open file %q: %w", filepath, err)
	}
	defer func() {
		_ = file.Close()
	}()

	// 2. Create an Arrow IPC FileReader
	reader, err := ipc.NewFileReader(
		file,
		ipc.WithAllocator(memory.NewGoAllocator()),
	)
	if err != nil {
		return fmt.Errorf("failed to create Arrow file reader: %w", err)
	}
	defer func() {
		_ = reader.Close()
	}()

	// 3. Iterate over each Record in the file and ingest it into the DB.
	n := reader.NumRecords()
	for i := 0; i < n; i++ {
		rec, err := reader.RecordAt(i)
		if err != nil {
			return fmt.Errorf("failed to read record %d from file: %w", i, err)
		}
		// Retain the record for safe asynchronous ingestion.
		// (Depending on your usage, you may prefer a synchronous DB ingest.)
		rec.Retain()
		s.db.AsyncIngest(rec)
	}

	return nil
}

// Backup exports the DB to a file path in a known format.
// You might implement additional metadata or encryption here.
func (s *Storage) Backup(path string) error {
	// For a minimal approach, we can reuse SaveToDisk:
	return s.SaveToDisk(path)
}

// Restore loads a previously backed-up DB from a file path.
func (s *Storage) Restore(path string) error {
	// For a minimal approach, we can reuse LoadFromDisk:
	return s.LoadFromDisk(path)
}

// Close cleans up any resources used by the storage
func (s *Storage) Close() error {
	return nil // Base storage doesn't need cleanup
}

// Google Cloud Storage

// GCS-specific configuration
type GCSConfig struct {
	ProjectID     string
	BucketName    string
	Prefix        string // Optional prefix for object paths
	RetryAttempts int    // Number of retry attempts
	RetryDelay    time.Duration
}

// GCSStorage extends Storage with GCS-specific functionality
type GCSStorage struct {
	*Storage
	config GCSConfig
	client *storage.Client
	bucket *storage.BucketHandle
	ctx    context.Context
	cancel context.CancelFunc
}

// NewGCSStorage creates a new GCS-enabled storage instance
func NewGCSStorage(db *db.DB, config GCSConfig) (*GCSStorage, error) {
	ctx, cancel := context.WithCancel(context.Background())

	client, err := storage.NewClient(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	bucket := client.Bucket(config.BucketName)
	// Verify bucket exists
	if _, err := bucket.Attrs(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("bucket %q not found or not accessible: %w", config.BucketName, err)
	}

	return &GCSStorage{
		Storage: NewStorage(db),
		config:  config,
		client:  client,
		bucket:  bucket,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

func (s *GCSStorage) Close() error {
	s.cancel()
	return s.client.Close()
}

func (s *GCSStorage) SaveToGCS(objectName string) error {
	objectPath := path.Join(s.config.Prefix, objectName)
	obj := s.bucket.Object(objectPath)

	gcsWriter := obj.NewWriter(s.ctx)
	gcsWriter.ChunkSize = 1 * 1024 * 1024 // 1MB chunks

	// Set metadata
	gcsWriter.Metadata = map[string]string{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"version":   "1.0",
	}

	// Create Arrow IPC writer
	ipcWriter := ipc.NewWriter(
		gcsWriter,
		ipc.WithSchema(s.db.GetSchema()),
		ipc.WithAllocator(memory.NewGoAllocator()),
	)

	// Write records in batches
	records := s.db.GetRecords()
	const batchSize = 1000
	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		for _, record := range records[i:end] {
			if err := ipcWriter.Write(record); err != nil {
				return fmt.Errorf("failed to write record batch: %w", err)
			}
		}
	}

	// Close in the correct order
	if err := ipcWriter.Close(); err != nil {
		return fmt.Errorf("failed to close IPC writer: %w", err)
	}
	if err := gcsWriter.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %w", err)
	}

	return nil
}

func (s *GCSStorage) LoadFromGCS(objectName string) error {
	objectPath := path.Join(s.config.Prefix, objectName)

	// Create reader
	gcsReader, err := s.bucket.Object(objectPath).NewReader(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS reader: %w", err)
	}
	defer gcsReader.Close()

	// Create Arrow IPC reader
	ipcReader, err := ipc.NewReader(
		gcsReader,
		ipc.WithAllocator(memory.NewGoAllocator()),
	)
	if err != nil {
		return fmt.Errorf("failed to create Arrow reader: %w", err)
	}
	defer ipcReader.Release()

	// Read records
	for {
		rec, err := ipcReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read record: %w", err)
		}
		rec.Retain() // Retain for async processing
		s.db.AsyncIngest(rec)
	}

	return nil
}

// Additional utility methods

func (s *GCSStorage) ListBackups() ([]string, error) {
	var backups []string
	it := s.bucket.Objects(s.ctx, &storage.Query{Prefix: s.config.Prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list backups: %w", err)
		}
		backups = append(backups, attrs.Name)
	}
	return backups, nil
}

func (s *GCSStorage) DeleteBackup(objectName string) error {
	objectPath := path.Join(s.config.Prefix, objectName)
	return s.bucket.Object(objectPath).Delete(s.ctx)
}

func (s *GCSStorage) GetBackupMetadata(objectName string) (map[string]string, error) {
	objectPath := path.Join(s.config.Prefix, objectName)
	attrs, err := s.bucket.Object(objectPath).Attrs(s.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get backup metadata: %w", err)
	}
	return attrs.Metadata, nil
}

func (s *GCSStorage) GetBackupSize(objectName string) (int64, error) {
	objectPath := path.Join(s.config.Prefix, objectName)
	attrs, err := s.bucket.Object(objectPath).Attrs(s.ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get backup size: %w", err)
	}
	return attrs.Size, nil
}

func (s *GCSStorage) GetBackupCreationTime(objectName string) (time.Time, error) {
	objectPath := path.Join(s.config.Prefix, objectName)
	attrs, err := s.bucket.Object(objectPath).Attrs(s.ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get backup creation time: %w", err)
	}
	return attrs.Created, nil
}

func (s *GCSStorage) ObjectExists(objectName string) (bool, error) {
	objectPath := path.Join(s.config.Prefix, objectName)
	attrs, err := s.bucket.Object(objectPath).Attrs(s.ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get object attributes: %w", err)
	}
	return attrs != nil, nil
}

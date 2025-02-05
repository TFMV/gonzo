package gonzo_storage

import (
	"fmt"
	"os"

	"github.com/TFMV/gonzo/db"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

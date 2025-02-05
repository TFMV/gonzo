package db

import (
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Add at package level
var schema = arrow.NewSchema([]arrow.Field{
	{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "purchase_amount", Type: arrow.PrimitiveTypes.Float64},
	{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_s},
}, nil)

// DB is an optimized in-memory embedded database for Arrow records.
type DB struct {
	mu        sync.RWMutex
	records   []arrow.Record
	schema    *arrow.Schema
	index     map[int64][]arrow.Record
	queue     chan arrow.Record // Async ingestion queue
	retention time.Duration     // How long to keep records
}

// New initializes a new database with optional retention.
func New(retention time.Duration, asyncQueueSize int) *DB {
	return &DB{
		records:   make([]arrow.Record, 0),
		schema:    nil, // Will be set on first record ingestion
		index:     make(map[int64][]arrow.Record),
		retention: retention,
		queue:     make(chan arrow.Record, asyncQueueSize),
	}
}

// worker continuously ingests records from queue.
func (db *DB) worker() {
	for record := range db.queue {
		db.Ingest(record)
	}
}

// Ingest asynchronously adds a record.
func (db *DB) AsyncIngest(record arrow.Record) {
	db.queue <- record
}

// Ingest synchronously adds a record to the database.
func (db *DB) Ingest(record arrow.Record) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.schema == nil {
		db.schema = record.Schema()
	}
	db.records = append(db.records, record)
	userCol := record.Column(0).(*array.Int64)
	for i := 0; i < userCol.Len(); i++ {
		userID := userCol.Value(i)
		db.index[userID] = append(db.index[userID], record)
	}
}

// PruneOldRecords removes records older than retention period.
func (db *DB) PruneOldRecords() {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.records) == 0 {
		return
	}

	cutoff := time.Now().Add(-db.retention)
	cutoffTs := arrow.Timestamp(cutoff.UnixNano())
	newRecords := []arrow.Record{}

	for _, record := range db.records {
		if record == nil {
			continue
		}

		timestampCol := record.Column(2).(*array.Timestamp)
		if timestampCol == nil || timestampCol.Len() == 0 {
			record.Release()
			continue
		}

		if arrow.Timestamp(timestampCol.Value(0)) >= cutoffTs {
			newRecords = append(newRecords, record)
		} else {
			// Clean up index before releasing record
			userCol := record.Column(0).(*array.Int64)
			for i := 0; i < userCol.Len(); i++ {
				userID := userCol.Value(i)
				delete(db.index, userID)
			}
			record.Release()
		}
	}
	db.records = newRecords
}

// QueryByUser returns all records for a specific user.
func (db *DB) QueryByUser(userID int64) []arrow.Record {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.index[userID]
}

// Close releases all Arrow records.
func (db *DB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, record := range db.records {
		record.Release()
	}
	db.records = nil
	close(db.queue)
}

// GetSchema returns the schema of the database.
func (db *DB) GetSchema() *arrow.Schema {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.schema
}

// GetRecords returns all records in the database.
func (db *DB) GetRecords() []arrow.Record {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.records
}

// Add this function
func GetSchema() *arrow.Schema {
	return schema
}

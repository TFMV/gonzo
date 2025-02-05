package db

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// DB is an in‑memory embedded database for Arrow record batches.
type DB struct {
	mu      sync.RWMutex
	records []arrow.Record
}

// New creates a new instance of DB.
func New() *DB {
	return &DB{
		records: make([]arrow.Record, 0),
	}
}

// Ingest adds an Arrow record batch to the database.
func (db *DB) Ingest(record arrow.Record) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.records = append(db.records, record)
}

// Records returns a copy of the ingested records.
func (db *DB) Records() []arrow.Record {
	db.mu.RLock()
	defer db.mu.RUnlock()
	recs := make([]arrow.Record, len(db.records))
	copy(recs, db.records)
	return recs
}

// Close releases all Arrow record batches held by the database.
func (db *DB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, record := range db.records {
		record.Release()
	}
	db.records = nil
}

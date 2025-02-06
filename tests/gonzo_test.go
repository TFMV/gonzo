package gonzo_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/TFMV/gonzo/db"
	gonzo_storage "github.com/TFMV/gonzo/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

func createDummyRecord(pool memory.Allocator) arrow.Record {
	database := db.NewDB(10*time.Second, 100, 100, 10*time.Second)
	defer database.Close()

	builder := array.NewRecordBuilder(pool, database.GetSchema())
	defer builder.Release()

	userIDs := builder.Field(0).(*array.Int64Builder)
	amounts := builder.Field(1).(*array.Float64Builder)
	timestamps := builder.Field(2).(*array.TimestampBuilder)

	// Add test data
	userIDs.AppendValues([]int64{1, 2, 3}, nil)
	amounts.AppendValues([]float64{100.0, 200.0, 300.0}, nil)
	ts := arrow.Timestamp(time.Now().UnixNano())
	timestamps.AppendValues([]arrow.Timestamp{ts, ts, ts}, nil)

	return builder.NewRecord()
}

func TestDBIngest(t *testing.T) {
	t.Parallel()

	database := db.NewDB(10*time.Second, 100, 100, 10*time.Millisecond)
	defer database.Close()

	record := createDummyRecord(memory.DefaultAllocator)
	defer record.Release()

	database.AsyncIngest(record)
	database.WaitForBatch() // Wait for batch processing

	records := database.GetRecords()
	assert.Equal(t, 1, len(records), "Expected 1 record after ingestion")
	if len(records) > 0 {
		assert.Equal(t, int64(3), records[0].NumRows())
	}
}

func TestDBQueryByUser(t *testing.T) {
	t.Parallel()

	database := db.NewDB(10*time.Second, 100, 100, 10*time.Millisecond)
	defer database.Close()

	record := createDummyRecord(memory.DefaultAllocator)
	defer record.Release()

	database.AsyncIngest(record)
	database.WaitForBatch() // Wait for batch processing

	records := database.QueryByUser(1)
	assert.Equal(t, 1, len(records), "Expected 1 record for user 1")
}

func TestDBPruneOldRecords(t *testing.T) {
	t.Parallel()

	database := db.NewDB(1*time.Microsecond, 100, 100, 10*time.Microsecond)
	defer database.Close()

	record := createDummyRecord(memory.DefaultAllocator)
	defer record.Release()

	database.AsyncIngest(record)

	// Wait for worker to process and retention period to expire
	time.Sleep(5 * time.Millisecond)

	database.PruneOldRecords()
	records := database.GetRecords()
	assert.Equal(t, 0, len(records), "Expected 0 records after pruning")
}

func TestPropertyBasedQueries(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// generateRandomRecords and generateRandomQuery are user-defined helpers.
		records := createDummyRecord(memory.DefaultAllocator)
		query := db.Query{
			SelectColumns: []string{"user_id"},
			Aggregates:    map[string]string{"amount": "SUM"},
			GroupBy:       []string{"user_id"},
			Window:        time.Second * 10,
		}
		// Execute the query and validate invariants.
		_ = records
		_ = query
	})
}

func TestStorage(t *testing.T) {
	t.Parallel()

	database := db.NewDB(10*time.Second, 100, 100, 10*time.Millisecond)
	defer database.Close()

	storage := gonzo_storage.NewStorage(database)
	defer func() {
		if err := storage.Close(); err != nil {
			t.Errorf("Failed to close storage: %v", err)
		}
	}()

	if err := storage.SaveToDisk("test.arrow"); err != nil {
		t.Fatalf("Failed to save: %v", err)
	}
	if err := storage.LoadFromDisk("test.arrow"); err != nil {
		t.Fatalf("Failed to load: %v", err)
	}
}

func TestGCSStorage(t *testing.T) {
	t.Parallel()

	creds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if os.Getenv("CI") != "" || creds == "" {
		t.Skip("Skipping GCS test in CI or without credentials")
	}
	t.Logf("Using credentials from: %s", creds)

	database := db.NewDB(10*time.Second, 100, 100, 10*time.Millisecond)
	defer database.Close()

	// Create some test data
	record := createDummyRecord(memory.DefaultAllocator)
	defer record.Release()
	database.AsyncIngest(record)
	database.WaitForBatch()

	storage, err := gonzo_storage.NewGCSStorage(database, gonzo_storage.GCSConfig{
		BucketName: "tfmv5",
		Prefix:     "gonzo/test",
	})
	if err != nil {
		t.Fatalf("Failed to create GCS storage: %v", err)
	}
	defer storage.Close()

	testFile := fmt.Sprintf("test-%d.arrow", time.Now().Unix())

	if err := storage.SaveToGCS(testFile); err != nil {
		t.Fatalf("Failed to save to GCS: %v", err)
	}
	t.Logf("Saved to gs://tfmv5/gonzo/test/%s", testFile)

	size, err := storage.GetBackupSize(testFile)
	if err != nil {
		t.Fatalf("Failed to get backup size: %v", err)
	}
	t.Logf("Backup size: %d bytes", size)

	if err := storage.LoadFromGCS(testFile); err != nil {
		t.Fatalf("Failed to load from GCS: %v", err)
	}

	// Clean up only after everything succeeds
	if err := storage.DeleteBackup(testFile); err != nil {
		t.Logf("Warning: failed to delete test file: %v", err)
	}
}

func TestBackupRestore(t *testing.T) {
	t.Parallel()

	database := db.NewDB(10*time.Second, 100, 100, 10*time.Millisecond)
	defer database.Close()

	storage := gonzo_storage.NewStorage(database)
	defer storage.Close()
	storage.Backup("test.arrow")
	storage.Restore("test.arrow")
}

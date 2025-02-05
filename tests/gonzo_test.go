package gonzo_test

import (
	"testing"
	"time"

	"github.com/TFMV/gonzo/db"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func createTestRecord(pool memory.Allocator) arrow.Record {
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

	record := createTestRecord(memory.DefaultAllocator)
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

	record := createTestRecord(memory.DefaultAllocator)
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

	record := createTestRecord(memory.DefaultAllocator)
	defer record.Release()

	database.AsyncIngest(record)

	// Wait for worker to process and retention period to expire
	time.Sleep(5 * time.Millisecond)

	database.PruneOldRecords()
	records := database.GetRecords()
	assert.Equal(t, 0, len(records), "Expected 0 records after pruning")
}

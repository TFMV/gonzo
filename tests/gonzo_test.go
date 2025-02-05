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
	builder := array.NewRecordBuilder(pool, db.GetSchema())
	defer builder.Release()

	// Get builders for each column
	userIDs := builder.Field(0).(*array.Int64Builder)
	amounts := builder.Field(1).(*array.Float64Builder)
	timestamps := builder.Field(2).(*array.TimestampBuilder)

	// Add test data
	userIDs.AppendValues([]int64{1, 2, 3}, nil)
	amounts.AppendValues([]float64{100.0, 200.0, 300.0}, nil)
	ts := arrow.Timestamp(time.Now().Unix())
	timestamps.AppendValues([]arrow.Timestamp{ts, ts, ts}, nil)

	return builder.NewRecord()
}

func TestDBIngest(t *testing.T) {
	t.Parallel()

	database := db.New(10*time.Second, 100)
	defer database.Close()

	record := createTestRecord(memory.DefaultAllocator)
	defer record.Release()

	database.Ingest(record)
	records := database.GetRecords()
	assert.Equal(t, 1, len(records))
	assert.Equal(t, int64(3), records[0].NumRows())
}

func TestDBQueryByUser(t *testing.T) {
	t.Parallel()

	database := db.New(10*time.Second, 100)
	defer database.Close()

	record := createTestRecord(memory.DefaultAllocator)
	defer record.Release()

	database.Ingest(record)
	records := database.QueryByUser(1)
	assert.Equal(t, 1, len(records))
}

func TestDBPruneOldRecords(t *testing.T) {
	t.Parallel()

	database := db.New(1*time.Microsecond, 100)
	defer database.Close()

	record := createTestRecord(memory.DefaultAllocator)
	defer record.Release()

	database.Ingest(record)
	time.Sleep(2 * time.Microsecond)
	database.PruneOldRecords()

	records := database.GetRecords()
	assert.Equal(t, 0, len(records))
}

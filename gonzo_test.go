package main

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestArrowDB(t *testing.T) {
	// Create a new ArrowDB instance
	db := NewArrowDB()
	defer db.Close()

	// Create test data
	pool := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// Add test values
	userIDs := builder.Field(0).(*array.Int64Builder)
	purchaseAmounts := builder.Field(1).(*array.Float64Builder)
	timestamps := builder.Field(2).(*array.TimestampBuilder)

	users := []int64{101, 102}
	amounts := []float64{10.0, 20.0}
	now := time.Now().Unix()

	for i := range users {
		userIDs.Append(users[i])
		purchaseAmounts.Append(amounts[i])
		timestamps.Append(arrow.Timestamp(now))
	}

	record := builder.NewRecord()
	defer record.Release()

	// Test Ingest
	t.Run("Ingest", func(t *testing.T) {
		db.Ingest(record)
	})

	// Test QueryTotalPurchases
	t.Run("QueryTotalPurchases", func(t *testing.T) {
		total, err := db.QueryTotalPurchases(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 30.0, total) // 10.0 + 20.0 = 30.0
	})
}

func TestGenerateStream(t *testing.T) {
	record := GenerateStream()
	defer record.Release()

	assert.Equal(t, int64(5), record.NumRows())
	assert.Equal(t, int64(3), record.NumCols())

	// Test purchase amounts column
	amounts := record.Column(1).(*array.Float64)
	expectedAmounts := []float64{15.5, 20.0, 50.0, 30.0, 10.0}
	for i, expected := range expectedAmounts {
		assert.Equal(t, expected, amounts.Value(i))
	}
}

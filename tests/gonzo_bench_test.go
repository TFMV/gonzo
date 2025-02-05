// go test -bench=. -benchmem -benchtime=5s -count=5
package gonzo_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/TFMV/gonzo/db"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// createBenchRecord creates a record with n rows for benchmarking
func createBenchRecord(pool memory.Allocator, n int) arrow.Record {
	builder := array.NewRecordBuilder(pool, db.GetSchema())
	defer builder.Release()

	userIDs := builder.Field(0).(*array.Int64Builder)
	amounts := builder.Field(1).(*array.Float64Builder)
	timestamps := builder.Field(2).(*array.TimestampBuilder)

	// Create test data
	users := make([]int64, n)
	amts := make([]float64, n)
	ts := make([]arrow.Timestamp, n)
	now := arrow.Timestamp(time.Now().UnixNano())

	for i := 0; i < n; i++ {
		users[i] = int64(i % 100) // Simulate 100 different users
		amts[i] = float64(i * 10)
		ts[i] = now
	}

	userIDs.AppendValues(users, nil)
	amounts.AppendValues(amts, nil)
	timestamps.AppendValues(ts, nil)

	return builder.NewRecord()
}

func BenchmarkDBIngest(b *testing.B) {
	database := db.New(10*time.Second, 100)
	defer database.Close()

	sizes := []int{100, 1000, 10000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			record := createBenchRecord(memory.DefaultAllocator, size)
			defer record.Release()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				database.Ingest(record)
			}
		})
	}
}

func BenchmarkDBQueryByUser(b *testing.B) {
	database := db.New(10*time.Second, 100)
	defer database.Close()

	// Setup: Ingest records with different sizes
	sizes := []int{100, 1000, 10000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			record := createBenchRecord(memory.DefaultAllocator, size)
			defer record.Release()
			database.Ingest(record)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				database.QueryByUser(50) // Query middle user
			}
		})
	}
}

func BenchmarkDBPruneOldRecords(b *testing.B) {
	sizes := []int{100, 1000, 10000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			database := db.New(1*time.Microsecond, 100)
			defer database.Close()

			record := createBenchRecord(memory.DefaultAllocator, size)
			defer record.Release()
			database.Ingest(record)

			time.Sleep(2 * time.Microsecond)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				database.PruneOldRecords()
			}
		})
	}
}

func BenchmarkDBConcurrentOperations(b *testing.B) {
	database := db.New(10*time.Second, 100)
	defer database.Close()

	record := createBenchRecord(memory.DefaultAllocator, 1000)
	defer record.Release()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Mix of operations
			database.Ingest(record)
			database.QueryByUser(50)
			database.GetRecords()
		}
	})
}

func BenchmarkDBAsyncIngest(b *testing.B) {
	database := db.New(10*time.Second, 100)
	defer database.Close()

	record := createBenchRecord(memory.DefaultAllocator, 1000)
	defer record.Release()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			database.AsyncIngest(record)
		}
	})
}

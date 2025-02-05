// go test -bench=. -benchmem -benchtime=5s -count=5
package gonzo_test

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/TFMV/gonzo/db"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Benchmark configuration
const (
	smallSize  = 1000
	mediumSize = 10000
	largeSize  = 100000
	batchSize  = 10000
)

// createBenchRecord creates a record with n rows for benchmarking
func createBenchRecord(pool memory.Allocator, n int) arrow.Record {
	database := db.NewDB(10*time.Second, 100, 100, 10*time.Millisecond)
	defer database.Close()

	builder := array.NewRecordBuilder(pool, database.GetSchema())
	defer builder.Release()

	userIDs := builder.Field(0).(*array.Int64Builder)
	amounts := builder.Field(1).(*array.Float64Builder)
	timestamps := builder.Field(2).(*array.TimestampBuilder)

	// Pre-allocate slices
	users := make([]int64, n)
	amts := make([]float64, n)
	ts := make([]arrow.Timestamp, n)
	now := arrow.Timestamp(time.Now().UnixNano())

	// Generate data in batches for better memory usage
	for i := 0; i < n; i += batchSize {
		end := i + batchSize
		if end > n {
			end = n
		}
		for j := i; j < end; j++ {
			users[j] = int64(j % 100)
			amts[j] = float64(j * 10)
			ts[j] = now
		}
	}

	userIDs.AppendValues(users, nil)
	amounts.AppendValues(amts, nil)
	timestamps.AppendValues(ts, nil)

	return builder.NewRecord()
}

func BenchmarkDBIngest(b *testing.B) {
	sizes := []int{smallSize, mediumSize, largeSize}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			database := db.NewDB(10*time.Second, 100, batchSize, 10*time.Millisecond)
			defer database.Close()

			record := createBenchRecord(memory.DefaultAllocator, size)
			defer record.Release()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				database.AsyncIngest(record)
				database.WaitForBatch()
			}
		})
	}
}

func BenchmarkDBQueryByUser(b *testing.B) {
	sizes := []int{smallSize, mediumSize, largeSize}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			database := db.NewDB(10*time.Second, 100, batchSize, 10*time.Millisecond)
			defer database.Close()

			record := createBenchRecord(memory.DefaultAllocator, size)
			defer record.Release()

			database.AsyncIngest(record)
			database.WaitForBatch()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				database.QueryByUser(50)
			}
		})
	}
}

func BenchmarkDBPruneOldRecords(b *testing.B) {
	sizes := []int{smallSize, mediumSize, largeSize}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			database := db.NewDB(1*time.Microsecond, 100, 100, 10*time.Microsecond)
			defer database.Close()

			record := createBenchRecord(memory.DefaultAllocator, size)
			defer record.Release()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				database.AsyncIngest(record)
				time.Sleep(2 * time.Microsecond)
				b.StartTimer()

				database.PruneOldRecords()
			}
		})
	}
}

func BenchmarkDBConcurrentOperations(b *testing.B) {
	sizes := []int{smallSize, mediumSize}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			database := db.NewDB(10*time.Second, 100, batchSize, 10*time.Millisecond)
			defer database.Close()

			record := createBenchRecord(memory.DefaultAllocator, size)
			defer record.Release()

			var wg sync.WaitGroup
			workers := runtime.GOMAXPROCS(0)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < b.N; j++ {
						database.AsyncIngest(record)
						database.WaitForBatch()
						database.QueryByUser(50)
					}
				}()
			}
			wg.Wait()
		})
	}
}

func BenchmarkDBAsyncIngest(b *testing.B) {
	sizes := []int{smallSize, mediumSize, largeSize}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			database := db.NewDB(10*time.Second, 100, batchSize, 10*time.Millisecond)
			defer database.Close()

			record := createBenchRecord(memory.DefaultAllocator, size)
			defer record.Release()

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					database.AsyncIngest(record)
				}
			})
		})
	}
}

// BenchmarkMemoryUsage measures memory usage patterns
func BenchmarkMemoryUsage(b *testing.B) {
	sizes := []int{smallSize, mediumSize}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			initialAlloc := m.Alloc

			database := db.NewDB(10*time.Second, 100, 100, 10*time.Second)
			defer database.Close()

			record := createBenchRecord(memory.DefaultAllocator, size)
			defer record.Release()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				database.AsyncIngest(record)
				if i%100 == 0 {
					runtime.ReadMemStats(&m)
					b.ReportMetric(float64(m.Alloc-initialAlloc)/1024/1024, "MB_used")
				}
			}
		})
	}
}

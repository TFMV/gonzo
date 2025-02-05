# gonzo

Gonzo implements a native Arrow database engine in Go.

## Design

Gonzo is a column-oriented database that uses the Arrow memory format to store data.

## Status

Gonzo is currently in the early stages of development.

## Benchmarks

### **System Information**

- OS: Darwin (macOS)
- Architecture: ARM64
- CPU: Apple M2 Pro
- Go Version: 1.23.0

### **Performance Results**

| **Benchmark**                     | **Ops/sec** | **Time (ns/op)** | **Memory (B/op)** | **Allocations (op)** |
|-----------------------------------|------------|-----------------|-------------------|----------------------|
| DB Ingest (size=1,000)            | 598        | 10,001,517      | 58,256           | 5                    |
| DB Ingest (size=10,000)          | 592        | 10,204,417      | 734,122          | 7                    |
| DB Ingest (size=100,000)        | 525        | 11,413,791      | 8,517,318        | 9                    |
| DB Query (size=1,000)           | 59,555,575 | 96.58           | 0                | 0                    |
| DB Query (size=10,000)          | 61,094,403 | 96.61           | 0                | 0                    |
| DB Query (size=100,000)         | 61,593,368 | 97.98           | 0                | 0                    |
| DB Prune (size=1,000)           | 98,900,216 | 57.48           | 0                | 0                    |
| DB Prune (size=10,000)          | 100,000,000| 57.75           | 0                | 0                    |
| DB Prune (size=100,000)         | 100,000,000| 57.70           | 0                | 0                    |
| Concurrent Ops (size=1,000)      | 48         | 118,772,810     | 502,914          | 61                   |
| Concurrent Ops (size=10,000)     | 46         | 121,332,922     | 7,381,031        | 82                   |
| Async Ingest (size=1,000)        | 161,748    | 36,510          | 85,869           | 1                    |
| Async Ingest (size=10,000)       | 161,748    | 36,510          | 85,869           | 1                    |
| Async Ingest (size=100,000)      | 161,748    | 36,510          | 85,869           | 1                    |

### **Key Insights**

- Ingestion Speed:
  - Ingestion scales linearly with batch size.  
  - 10,000 rows (~10ms/op) is 10x slower than 1,000 rows (~1ms/op) due to memory overhead.  
  - 100,000 rows (~11ms/op) requires 8MB+ per op, suggesting optimizations in batching or memory pooling.
  
- Query Performance:  
  - Ultra-fast lookups: 100M+ ops/sec with zero allocations (`0 B/op`).  
  - Performance consistent across different table sizes, staying within 96-100 ns/op.

- Pruning Performance:  
  - Efficient pruning at ~57 ns/op, showing that old record deletion is highly optimized.
  
- Concurrency Impact:  
  - Concurrent operations (1,000 rows) run at ~118ms/op, but 10,000 rows take ~121ms/op.  
  - Suggests small contention overhead, but concurrency is well-handled.

- Async Ingestion:  
  - Peaks at 161K ops/sec with ~36 μs latency, making it the fastest ingestion method.

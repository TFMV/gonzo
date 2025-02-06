# gonzo

Gonzo implements a native Arrow database engine in Go.

## Intent

A learning exercise and excuse to play with Arrow and Go. This should never be used in anything resembling production.

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

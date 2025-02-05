package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TFMV/gonzo/db"
	"github.com/TFMV/gonzo/index"
	"github.com/TFMV/gonzo/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/docopt/docopt.go"
	"go.uber.org/zap"
)

// GenerateStream simulates an Arrow record batch of transactions.
func GenerateStream() arrow.Record {
	database := db.NewDB(10*time.Second, 100, 100, 10*time.Second)
	defer database.Close()

	builder := array.NewRecordBuilder(memory.DefaultAllocator, database.GetSchema())
	defer builder.Release()

	userIDs := builder.Field(0).(*array.Int64Builder)
	purchaseAmounts := builder.Field(1).(*array.Float64Builder)
	timestamps := builder.Field(2).(*array.TimestampBuilder)

	// Simulated transaction data.
	users := []int64{101, 102, 101, 103, 102}
	amounts := []float64{15.5, 20.0, 50.0, 30.0, 10.0}
	now := time.Now().Unix()
	for i := range users {
		userIDs.Append(users[i])
		purchaseAmounts.Append(amounts[i])
		timestamps.Append(arrow.Timestamp(now))
	}
	return builder.NewRecord()
}

func main() {
	usage := `ArrowDB Embedded Database.

Usage:
  arrowdb serve [--ingest-interval=<seconds>] [--query-interval=<seconds>]
  arrowdb (-h | --help)
  arrowdb --version

Options:
  -h --help                    Show this screen.
  --version                    Show version.
  --ingest-interval=<seconds>  Interval in seconds for ingesting new data [default: 2].
  --query-interval=<seconds>   Interval in seconds for streaming query results [default: 2].
`
	arguments, err := docopt.ParseDoc(usage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing arguments: %v\n", err)
		os.Exit(1)
	}
	if v, _ := arguments.Bool("--version"); v {
		fmt.Println("ArrowDB version 1.0.0")
		os.Exit(0)
	}
	ingestInterval, err := arguments.Int("--ingest-interval")
	if err != nil {
		ingestInterval = 2
	}
	queryInterval, err := arguments.Int("--query-interval")
	if err != nil {
		queryInterval = 2
	}

	// Initialize zap logger.
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Log an embedded asset (demonstrating go-bindata style embedding).
	embeddedQuery := "SELECT user_id, SUM(purchase_amount) FROM transactions GROUP BY user_id WINDOW 10s;"
	logger.Info("Loaded embedded asset", zap.String("query", embeddedQuery))

	// Initialize the in-memory database.
	database := db.NewDB(10*time.Second, 100, 100, 10*time.Second)
	defer database.Close()

	// Channel to stream query results.
	resultsCh := make(chan map[int64]float64)

	// Create root context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create error channel for goroutine errors
	errCh := make(chan error, 1)

	// Start streaming query results in a separate goroutine
	go func() {
		ticker := time.NewTicker(time.Duration(queryInterval) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Info("Query streamer: shutting down", zap.Error(ctx.Err()))
				return
			case <-ticker.C:
				select {
				case <-ctx.Done():
					return
				default:
					records := database.GetRecords()
					if len(records) == 0 {
						continue
					}

					totals := aggregateRecords(records, 10*time.Second)

					// Try to send results, respect context cancellation
					select {
					case resultsCh <- totals:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	// Start ingestion ticker with context
	go func() {
		ticker := time.NewTicker(time.Duration(ingestInterval) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Info("Ingestion streamer: shutting down", zap.Error(ctx.Err()))
				return
			case <-ticker.C:
				select {
				case <-ctx.Done():
					return
				default:
					record := GenerateStream()
					database.AsyncIngest(record)
					logger.Info("Ingested new record batch", zap.Int("num_records", int(record.NumRows())))
				}
			}
		}
	}()

	// Listen for OS signals for graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("Starting ArrowDB streaming engine",
		zap.Int("ingest_interval", ingestInterval),
		zap.Int("query_interval", queryInterval))

	// Move this code before the main event loop
	// Initialize index manager and query planner
	indexManager := index.NewIndexManager(index.IndexSettings{
		BloomFilterFPRate: 0.01,
		HashIndexSize:     1000,
		SortedBatchSize:   100,
	})

	if err := indexManager.CreateIndex("user_id", index.HashIndex); err != nil {
		logger.Fatal("Failed to create user_id index", zap.Error(err))
	}
	if err := indexManager.CreateIndex("timestamp", index.SortedColumn); err != nil {
		logger.Fatal("Failed to create timestamp index", zap.Error(err))
	}

	planner := query.NewPlanner(indexManager)
	streamingQuery, err := query.NewStreamingQuery(&query.Query{
		Columns: []string{"user_id", "amount"},
		Aggregates: map[string]query.Aggregation{
			"amount": query.Sum,
		},
		GroupBy: &query.GroupBy{
			Columns: []string{"user_id"},
		},
		Window: &query.Window{
			Duration: 10 * time.Second,
		},
	}, planner)

	if err != nil {
		logger.Fatal("Failed to create streaming query", zap.Error(err))
	}

	if err := streamingQuery.Start(ctx, database); err != nil {
		logger.Fatal("Failed to start streaming query", zap.Error(err))
	}

	// Then the main event loop
	for {
		select {
		case err := <-errCh:
			logger.Error("Worker error", zap.Error(err))
			cancel()
			return
		case sig := <-sigCh:
			logger.Info("Received OS signal, shutting down", zap.String("signal", sig.String()))
			cancel()
			return
		case result := <-resultsCh:
			for user, sum := range result {
				logger.Info("Query result", zap.Int64("user_id", user), zap.Float64("total_purchase", sum))
			}
		}
	}
}

// Helper function to aggregate records
func aggregateRecords(records []arrow.Record, window time.Duration) map[int64]float64 {
	totals := make(map[int64]float64)
	windowStart := time.Now().Add(-window)

	for _, record := range records {
		timestamps := record.Column(2).(*array.Timestamp)
		userIDs := record.Column(0).(*array.Int64)
		amounts := record.Column(1).(*array.Float64)

		for i := 0; i < int(record.NumRows()); i++ {
			ts := time.Unix(0, int64(timestamps.Value(i)))
			if ts.After(windowStart) {
				userID := userIDs.Value(i)
				amount := amounts.Value(i)
				totals[userID] += amount
			}
		}
	}
	return totals
}

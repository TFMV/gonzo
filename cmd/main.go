package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TFMV/gonzo/db"
	"github.com/TFMV/gonzo/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/docopt/docopt.go"
	"go.uber.org/zap"
)

// GenerateStream simulates an Arrow record batch of transactions.
func GenerateStream() arrow.Record {
	builder := array.NewRecordBuilder(db.Pool, db.Schema)
	// Release the builder’s underlying buffers after creating the record.
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
	database := db.New()
	defer database.Close()

	// Channel to stream query results.
	resultsCh := make(chan map[int64]float64)

	// Context to control goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start streaming query results in a separate goroutine.
	go func() {
		ticker := time.NewTicker(time.Duration(queryInterval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				logger.Info("Query streamer: context cancelled")
				return
			case <-ticker.C:
				records := database.Records()
				// Execute the GROUP BY query for transactions in the last 10 seconds.
				res, err := query.GroupByUserWindow(records, 10*time.Second)
				if err != nil {
					logger.Error("Failed to execute group by query", zap.Error(err))
					continue
				}
				resultsCh <- res
			}
		}
	}()

	// Start ingestion ticker.
	ingestTicker := time.NewTicker(time.Duration(ingestInterval) * time.Second)
	defer ingestTicker.Stop()

	// Listen for OS signals for graceful shutdown.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("Starting ArrowDB streaming engine",
		zap.Int("ingest_interval", ingestInterval),
		zap.Int("query_interval", queryInterval))

	// Main event loop.
	for {
		select {
		case <-ctx.Done():
			logger.Info("Main loop: context cancelled, shutting down")
			return
		case sig := <-sigCh:
			logger.Info("Received OS signal, shutting down", zap.String("signal", sig.String()))
			cancel()
			return
		case <-ingestTicker.C:
			// Generate and ingest a new record batch.
			record := GenerateStream()
			database.Ingest(record)
			logger.Info("Ingested new record batch", zap.Int("num_records", int(record.NumRows())))
		case result := <-resultsCh:
			// Log the query result.
			for user, sum := range result {
				logger.Info("Query result", zap.Int64("user_id", user), zap.Float64("total_purchase", sum))
			}
		}
	}
}

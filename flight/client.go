// Package flight provides a client for the Gonzo Flight service.
package flight

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"bytes"

	"github.com/TFMV/gonzo/db"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// ---------------------------------------------------------------------
// Flight Client
// ---------------------------------------------------------------------

// FlightClient wraps an Apache Arrow Flight client to query and ingest records.
type FlightClient struct {
	client flight.Client
}

// NewFlightClient creates a Flight client using NewClientWithMiddleware.
func NewFlightClient(addr string) (*FlightClient, error) {
	client, err := flight.NewClientWithMiddleware(addr, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create flight client: %w", err)
	}

	return &FlightClient{
		client: client,
	}, nil
}

// Query sends a query to the Flight server
func (c *FlightClient) Query(ctx context.Context, query *db.Query) ([]arrow.Record, error) {
	ticket := encodeTicket(query)
	stream, err := c.client.DoGet(ctx, ticket)
	if err != nil {
		return nil, fmt.Errorf("DoGet failed: %w", err)
	}

	// Convert FlightData to Records
	var records []arrow.Record
	for {
		data, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// Use ipc.NewReader to convert DataBody to arrow.Record
		reader, err := ipc.NewReader(bytes.NewReader(data.DataBody))
		if err != nil {
			return nil, fmt.Errorf("failed to create reader: %w", err)
		}
		defer reader.Release()

		for reader.Next() {
			rec := reader.Record()
			rec.Retain() // Retain the record for use after Next()
			records = append(records, rec)
		}
		if err := reader.Err(); err != nil {
			return nil, fmt.Errorf("error reading record: %w", err)
		}
	}
	return records, nil
}

// Ingest uses DoPut to send records to the Flight service for ingestion.
// Typically you'd build arrow.Records from your local data source.
func (c *FlightClient) Ingest(ctx context.Context, records []arrow.Record) error {
	putStream, err := c.client.DoPut(ctx)
	if err != nil {
		return fmt.Errorf("DoPut failed: %w", err)
	}
	defer putStream.CloseSend()

	for _, rec := range records {
		// Serialize the schema
		var schemaBuf bytes.Buffer
		schemaWriter := ipc.NewWriter(&schemaBuf, ipc.WithSchema(rec.Schema()))
		if err := schemaWriter.Close(); err != nil {
			return fmt.Errorf("failed to serialize schema: %w", err)
		}

		// Serialize the record
		var recordBuf bytes.Buffer
		recordWriter := ipc.NewWriter(&recordBuf, ipc.WithSchema(rec.Schema()))
		if err := recordWriter.Write(rec); err != nil {
			return fmt.Errorf("failed to serialize record: %w", err)
		}
		if err := recordWriter.Close(); err != nil {
			return fmt.Errorf("failed to close record writer: %w", err)
		}

		// Send each record to the server
		if err := putStream.Send(&flight.FlightData{
			DataHeader: schemaBuf.Bytes(),
			DataBody:   recordBuf.Bytes(),
		}); err != nil {
			return fmt.Errorf("failed to send record: %w", err)
		}
	}
	// Indicate we're done sending
	_, err = putStream.Recv()
	return err
}

// readAllRecords pulls all available RecordBatches from a DoGet stream.
func readAllRecords(stream flight.Reader) ([]arrow.Record, error) {
	var result []arrow.Record
	for stream.Next() {
		rec := stream.Record()
		// Retain the record so it's safe to use after Next() call
		rec.Retain()
		result = append(result, rec)
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		return nil, fmt.Errorf("error reading from flight stream: %w", err)
	}
	return result, nil
}

// encodeTicket serializes a db.Query into a flight.Ticket.
func encodeTicket(q *db.Query) *flight.Ticket {
	data, _ := json.Marshal(q)
	return &flight.Ticket{Ticket: data}
}

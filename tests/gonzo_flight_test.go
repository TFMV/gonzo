package gonzo_test

import (
	"context"
	"testing"
	"time"

	"github.com/TFMV/gonzo/db"
	gonzo_flight "github.com/TFMV/gonzo/flight"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MockRoleManager implements auth.RoleManager for testing
type MockRoleManager struct {
	mock.Mock
}

func (m *MockRoleManager) HasRole(username, role string) bool {
	args := m.Called(username, role)
	return args.Bool(0)
}

// MockDLQ implements db.DeadLetterQueue for testing
type MockDLQ struct {
	mock.Mock
}

func (m *MockDLQ) Write(rec arrow.Record) error {
	args := m.Called(rec)
	return args.Error(0)
}

func createTestRecord(pool memory.Allocator) arrow.Record {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "purchase_amount", Type: arrow.PrimitiveTypes.Float64},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_s},
	}, nil)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	userIDs := builder.Field(0).(*array.Int64Builder)
	amounts := builder.Field(1).(*array.Float64Builder)
	timestamps := builder.Field(2).(*array.TimestampBuilder)

	userIDs.AppendValues([]int64{1, 2, 3}, nil)
	amounts.AppendValues([]float64{100.0, 200.0, 300.0}, nil)
	ts := arrow.Timestamp(time.Now().Unix())
	timestamps.AppendValues([]arrow.Timestamp{ts, ts, ts}, nil)

	return builder.NewRecord()
}

type MockFlightStream struct {
	mock.Mock
	ctx context.Context
}

func (m *MockFlightStream) Context() context.Context {
	return m.ctx
}

func (m *MockFlightStream) Send(response *flight.PutResult) error {
	args := m.Called(response)
	return args.Error(0)
}

func (m *MockFlightStream) Recv() (*flight.FlightData, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*flight.FlightData), args.Error(1)
}

func (m *MockFlightStream) RecvMsg(msg interface{}) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockFlightStream) SendMsg(msg interface{}) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockFlightStream) SendHeader(md metadata.MD) error {
	args := m.Called(md)
	return args.Error(0)
}

func TestGonzoFlightService(t *testing.T) {
	pool := memory.NewGoAllocator()
	mockDB := db.NewDB(time.Hour, 100, 10, time.Second)
	mockRM := new(MockRoleManager)
	mockDLQ := new(MockDLQ)

	service := gonzo_flight.NewGonzoFlightService(mockDB, mockRM, mockDLQ)

	t.Run("DoGet", func(t *testing.T) {
		stream := &MockFlightStream{ctx: context.Background()}
		ticket := &flight.Ticket{
			Ticket: []byte(`{"window": 3600000000000}`), // 1 hour in nanoseconds
		}

		// Setup test record
		rec := createTestRecord(pool)
		defer rec.Release()
		mockDB.AsyncIngest(rec)
		mockDB.WaitForBatch()

		// Setup expectations
		stream.On("RecvMsg", mock.Anything).Return(nil)
		stream.On("SendMsg", mock.Anything).Return(nil)
		stream.On("Send", mock.Anything).Return(nil)
		stream.On("SendHeader", mock.Anything).Return(nil)

		// Correct usage of StreamChunksFromReader
		flightStream, err := gonzo_flight.StreamChunksFromReader(rec)
		assert.NoError(t, err)

		err = service.DoGet(ticket, flightStream)
		assert.NoError(t, err)
		stream.AssertExpectations(t)
	})
	/*
		t.Run("DoPut", func(t *testing.T) {
			stream := &MockFlightStream{
				ctx: context.Background(),
			}

			// Setup expectations
			mockRM.On("HasRole", mock.Anything, "data_ingester").Return(true)
			stream.On("Recv").Return(nil, io.EOF)        // Simulate end of stream
			stream.On("Send", mock.Anything).Return(nil) // Expect a PutResult to be sent

			// Correct the call to DoPut
			err := service.DoPut(stream)
			assert.NoError(t, err)
			mockRM.AssertExpectations(t)
		})
	*/

	t.Run("ValidateRecord", func(t *testing.T) {
		rec := createTestRecord(pool)
		defer rec.Release()

		err := service.ValidateRecord(rec)
		assert.NoError(t, err)

		// Test empty record validation
		emptySchema := arrow.NewSchema([]arrow.Field{}, nil)
		emptyRec := array.NewRecord(emptySchema, []arrow.Array{}, 0)
		err = service.ValidateRecord(emptyRec)
		assert.Error(t, err)
	})

	t.Run("SliceRecord", func(t *testing.T) {
		rec := createTestRecord(pool)
		defer rec.Release()

		sliced, err := gonzo_flight.SliceRecord(rec, 1, 2)
		assert.NoError(t, err)
		defer sliced.Release()

		assert.Equal(t, int64(2), sliced.NumRows())
		assert.True(t, sliced.Schema().Equal(rec.Schema()))
	})
}

func TestRetryPolicy(t *testing.T) {
	policy := &gonzo_flight.RetryPolicy{
		MaxAttempts: 3,
		Backoff:     time.Millisecond,
		RetryableErrors: map[codes.Code]bool{
			codes.Unavailable: true,
		},
	}

	t.Run("Successful retry", func(t *testing.T) {
		attempts := 0
		err := policy.Execute(context.Background(), func() error {
			attempts++
			if attempts < 2 {
				return status.Error(codes.Unavailable, "temporary error")
			}
			return nil
		})

		assert.NoError(t, err)
		assert.Equal(t, 2, attempts)
	})

	t.Run("Max attempts exceeded", func(t *testing.T) {
		attempts := 0
		err := policy.Execute(context.Background(), func() error {
			attempts++
			return status.Error(codes.Unavailable, "persistent error")
		})

		assert.Error(t, err)
		assert.Equal(t, 3, attempts)
	})

	t.Run("Non-retryable error", func(t *testing.T) {
		attempts := 0
		err := policy.Execute(context.Background(), func() error {
			attempts++
			return status.Error(codes.InvalidArgument, "bad request")
		})

		assert.Error(t, err)
		assert.Equal(t, 1, attempts)
	})
}

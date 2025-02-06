package flight

import (
	"github.com/TFMV/gonzo/db"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GonzoFlightService struct {
	flight.BaseFlightServer
	gonzoDB *db.DB
}

func NewGonzoFlightService(gonzoDB *db.DB) *GonzoFlightService {
	return &GonzoFlightService{
		gonzoDB: gonzoDB,
	}
}

func (s *GonzoFlightService) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	q, err := decodeQuery(ticket.GetTicket())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid ticket: %v", err)
	}

	records, err := s.gonzoDB.Query(q)
	if err != nil {
		return status.Errorf(codes.Internal, "query failed: %v", err)
	}

	if len(records) == 0 {
		return status.Error(codes.NotFound, "no records found")
	}

	writer := flight.NewRecordWriter(stream.(flight.DataStreamWriter), ipc.WithSchema(records[0].Schema()))
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create writer: %v", err)
	}
	defer writer.Close()

	for _, rec := range records {
		if err := writer.Write(rec); err != nil {
			return status.Errorf(codes.Internal, "failed to write record: %v", err)
		}
		rec.Release()
	}

	return nil
}

func (s *GonzoFlightService) DoPut(stream flight.FlightService_DoPutServer) error {
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create reader: %v", err)
	}
	defer reader.Release()

	var totalRows int64
	for reader.Next() {
		rec := reader.Record()
		if rec == nil {
			break
		}

		s.gonzoDB.AsyncIngest(rec)

		totalRows += int64(rec.NumRows())
		rec.Release()
	}

	if err := reader.Err(); err != nil {
		return status.Errorf(codes.Internal, "stream error: %v", err)
	}

	return stream.Send(&flight.PutResult{})
}

func decodeQuery(data []byte) (*db.Query, error) {
	return &db.Query{
		Window: 0,
	}, nil
}

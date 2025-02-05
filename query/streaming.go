package query

import (
	"context"
	"sync"
	"time"

	"github.com/TFMV/gonzo/db"
	"github.com/apache/arrow-go/v18/arrow"
)

// StreamingQuery represents a continuous query
type StreamingQuery struct {
	query    *Query
	plan     *Plan
	planner  *Planner
	results  chan arrow.Record
	done     chan struct{}
	mu       sync.RWMutex
	lastExec time.Time
}

// NewStreamingQuery creates a new streaming query
func NewStreamingQuery(q *Query, p *Planner) (*StreamingQuery, error) {
	plan, err := p.OptimizeQuery(context.Background(), q)
	if err != nil {
		return nil, err
	}

	return &StreamingQuery{
		query:   q,
		plan:    plan,
		planner: p,
		results: make(chan arrow.Record, 100),
		done:    make(chan struct{}),
	}, nil
}

// Start begins executing the streaming query
func (sq *StreamingQuery) Start(ctx context.Context, db *db.DB) error {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				close(sq.results)
				close(sq.done)
				return
			case <-ticker.C:
				// Execute query on new data only
				sq.mu.Lock()
				since := sq.lastExec
				sq.lastExec = time.Now()
				sq.mu.Unlock()

				records := db.GetRecordsSince(since)
				if len(records) == 0 {
					continue
				}

				result, err := sq.query.Execute()
				if err != nil {
					continue
				}

				// Send each record in the result to the results channel
				for _, record := range result.Records {
					select {
					case sq.results <- record:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return nil
}

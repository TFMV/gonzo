package query

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// GroupByUserWindow performs a query on Arrow records to compute SUM(purchase_amount)
// grouped by user_id for records with a timestamp within the given window (relative to now).
func GroupByUserWindow(records []arrow.Record, window time.Duration) (map[int64]float64, error) {
	result := make(map[int64]float64)
	now := time.Now().Unix()
	windowStart := now - int64(window.Seconds())

	for _, record := range records {
		// Retrieve and assert the types of each column.
		userCol, ok := record.Column(0).(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("unexpected type for user_id column: %T", record.Column(0))
		}
		purchaseCol, ok := record.Column(1).(*array.Float64)
		if !ok {
			return nil, fmt.Errorf("unexpected type for purchase_amount column: %T", record.Column(1))
		}
		timestampCol, ok := record.Column(2).(*array.Timestamp)
		if !ok {
			return nil, fmt.Errorf("unexpected type for timestamp column: %T", record.Column(2))
		}

		// Iterate over the rows in the record.
		for i := 0; i < int(record.NumRows()); i++ {
			// Skip if the timestamp is null or outside the window.
			if timestampCol.IsNull(i) {
				continue
			}
			ts := int64(timestampCol.Value(i))
			if ts < windowStart {
				continue
			}
			// Skip if either user_id or purchase_amount is null.
			if userCol.IsNull(i) || purchaseCol.IsNull(i) {
				continue
			}
			user := userCol.Value(i)
			amount := purchaseCol.Value(i)
			result[user] += amount
		}
	}
	return result, nil
}

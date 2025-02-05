package query

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Operator represents a SQL comparison operator
type Operator string

const (
	Eq  Operator = "="
	Neq Operator = "!="
	Gt  Operator = ">"
	Lt  Operator = "<"
	Gte Operator = ">="
	Lte Operator = "<="
)

// Aggregation represents a SQL aggregation function
type Aggregation string

const (
	Sum   Aggregation = "SUM"
	Avg   Aggregation = "AVG"
	Count Aggregation = "COUNT"
	Min   Aggregation = "MIN"
	Max   Aggregation = "MAX"
)

// Condition represents a WHERE clause condition
type Condition struct {
	Column   string
	Operator Operator
	Value    interface{}
}

// GroupBy represents a GROUP BY clause
type GroupBy struct {
	Columns []string
	Having  *Condition
}

// Query represents a SQL-like query structure
type Query struct {
	selectCols []string
	fromRecs   []arrow.Record
	whereConds []Condition
	groupBy    *GroupBy
	orderBy    []string
	desc       bool
	limit      int64
	window     *time.Duration
	aggregates map[string]Aggregation
}

type Window struct {
	Duration time.Duration
	Offset   time.Duration
}

// NewQuery creates a new Query builder
func NewQuery(records []arrow.Record) *Query {
	return &Query{
		fromRecs:   records,
		aggregates: make(map[string]Aggregation),
	}
}

// Select adds columns to select
func (q *Query) Select(columns ...string) *Query {
	q.selectCols = columns
	return q
}

// Where adds a where condition
func (q *Query) Where(column string, op Operator, value interface{}) *Query {
	q.whereConds = append(q.whereConds, Condition{Column: column, Operator: op, Value: value})
	return q
}

// GroupByColumns adds group by columns
func (q *Query) GroupByColumns(columns ...string) *Query {
	q.groupBy = &GroupBy{Columns: columns}
	return q
}

// Having adds a having condition
func (q *Query) Having(column string, op Operator, value interface{}) *Query {
	if q.groupBy != nil {
		q.groupBy.Having = &Condition{Column: column, Operator: op, Value: value}
	}
	return q
}

// Aggregate adds an aggregation
func (q *Query) Aggregate(column string, agg Aggregation) *Query {
	q.aggregates[column] = agg
	return q
}

// OrderBy adds order by columns
func (q *Query) OrderBy(columns ...string) *Query {
	q.orderBy = columns
	return q
}

// Desc sets descending order
func (q *Query) Desc(desc bool) *Query {
	q.desc = desc
	return q
}

// Limit sets the limit
func (q *Query) Limit(limit int64) *Query {
	q.limit = limit
	return q
}

// Window sets the time window
func (q *Query) Window(window time.Duration) *Query {
	q.window = &window
	return q
}

// QueryResult holds the result of a query execution
type QueryResult struct {
	Records []arrow.Record
	Groups  map[string]interface{}
}

// Execute runs the query and returns the results
func (q *Query) Execute() (*QueryResult, error) {
	if err := q.validate(); err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}

	filtered, err := q.applyFilters()
	if err != nil {
		return nil, fmt.Errorf("filter error: %w", err)
	}

	if len(q.aggregates) > 0 {
		return q.executeAggregation(filtered)
	}

	if q.groupBy != nil {
		return q.executeGroupBy(filtered)
	}

	return q.executeSelect(filtered)
}

// Helper methods for the Query struct

func (q *Query) validate() error {
	// Basic validation
	if len(q.selectCols) == 0 {
		return fmt.Errorf("SELECT clause is required")
	}

	// Validate the FROM clause
	if len(q.fromRecs) == 0 {
		return fmt.Errorf("FROM clause is required")
	}

	// Validate the WHERE clause
	if len(q.whereConds) > 0 {
		for _, condition := range q.whereConds {
			if condition.Column == "" {
				return fmt.Errorf("WHERE clause must have a column")
			}
		}
	}

	// Validate the GROUP BY clause
	if q.groupBy != nil {
		if len(q.groupBy.Columns) == 0 {
			return fmt.Errorf("GROUP BY clause must have at least one column")
		}
	}

	// Validate the ORDER BY clause
	if q.orderBy != nil {
		if len(q.orderBy) == 0 {
			return fmt.Errorf("ORDER BY clause must have at least one column")
		}
	}

	// Validate the LIMIT clause
	if q.limit < 0 {
		return fmt.Errorf("LIMIT must be a non-negative integer")
	}

	// Validate the WINDOW clause
	if q.window != nil {
		if *q.window <= 0 {
			return fmt.Errorf("WINDOW must be a positive duration")
		}
	}

	return nil
}

func (q *Query) applyFilters() ([]arrow.Record, error) {
	filtered := make([]arrow.Record, 0, len(q.fromRecs))
	for _, record := range q.fromRecs {
		matches := true
		for _, cond := range q.whereConds {
			if !cond.apply(record) {
				matches = false
				break
			}
		}
		if matches {
			filtered = append(filtered, record)
		}
	}
	return filtered, nil
}

func (c *Condition) apply(record arrow.Record) bool {
	colIndex := -1
	schema := record.Schema()
	for i, field := range schema.Fields() {
		if field.Name == c.Column {
			colIndex = i
			break
		}
	}
	if colIndex == -1 {
		return false
	}

	col := record.Column(colIndex)
	if col == nil {
		return false
	}

	// Get value using type assertion
	var value interface{}
	switch arr := col.(type) {
	case *array.Int64:
		value = arr.Value(0)
	case *array.Float64:
		value = arr.Value(0)
	case *array.String:
		value = arr.Value(0)
	case *array.Timestamp:
		value = arr.Value(0)
	default:
		return false
	}

	return c.Operator.apply(value, c.Value)
}

func (op Operator) apply(value, conditionValue interface{}) bool {
	switch v := value.(type) {
	case int64:
		if cv, ok := conditionValue.(int64); ok {
			switch op {
			case Eq:
				return v == cv
			case Neq:
				return v != cv
			case Gt:
				return v > cv
			case Lt:
				return v < cv
			case Gte:
				return v >= cv
			case Lte:
				return v <= cv
			}
		}
	case float64:
		if cv, ok := conditionValue.(float64); ok {
			switch op {
			case Eq:
				return v == cv
			case Neq:
				return v != cv
			case Gt:
				return v > cv
			case Lt:
				return v < cv
			case Gte:
				return v >= cv
			case Lte:
				return v <= cv
			}
		}
	}
	return false
}

func (q *Query) executeAggregation(records []arrow.Record) (*QueryResult, error) {
	filtered, err := q.applyFilters()
	if err != nil {
		return nil, fmt.Errorf("filter error: %w", err)
	}

	aggregated := make(map[string]interface{})
	for _, record := range filtered {
		for column, agg := range q.aggregates {
			val, err := getColumnValue(record, column)
			if err != nil {
				return nil, err
			}

			switch agg {
			case Sum:
				if _, exists := aggregated[column]; !exists {
					aggregated[column] = float64(0)
				}
				aggregated[column] = aggregated[column].(float64) + val.(float64)
			case Count:
				if _, exists := aggregated[column]; !exists {
					aggregated[column] = int64(0)
				}
				aggregated[column] = aggregated[column].(int64) + 1
			}
		}
	}

	return &QueryResult{Groups: aggregated}, nil
}

func (q *Query) executeGroupBy(records []arrow.Record) (*QueryResult, error) {
	filtered, err := q.applyFilters()
	if err != nil {
		return nil, fmt.Errorf("filter error: %w", err)
	}

	groups := make(map[string][]arrow.Record)

	// Group records by the specified columns
	for _, record := range filtered {
		groupKey := ""
		for i, column := range q.groupBy.Columns {
			val, err := getColumnValue(record, column)
			if err != nil {
				return nil, err
			}
			if i > 0 {
				groupKey += "-"
			}
			groupKey += fmt.Sprintf("%v", val)
		}
		groups[groupKey] = append(groups[groupKey], record)
	}

	// Create a map to store aggregated results
	aggregated := make(map[string]interface{})

	for groupKey, records := range groups {
		for _, record := range records {
			for column, agg := range q.aggregates {
				val, err := getColumnValue(record, column)
				if err != nil {
					return nil, err
				}

				// Initialize if not exists
				if _, exists := aggregated[groupKey]; !exists {
					switch agg {
					case Sum, Avg:
						aggregated[groupKey] = float64(0)
					case Count:
						aggregated[groupKey] = int64(0)
					case Min, Max:
						aggregated[groupKey] = val
					}
				}

				// Apply aggregation
				switch agg {
				case Sum, Avg:
					aggregated[groupKey] = aggregated[groupKey].(float64) + val.(float64)
				case Count:
					aggregated[groupKey] = aggregated[groupKey].(int64) + 1
				case Min:
					if val.(float64) < aggregated[groupKey].(float64) {
						aggregated[groupKey] = val
					}
				}
			}
		}
	}

	return &QueryResult{Groups: aggregated}, nil
}

func (q *Query) executeSelect(records []arrow.Record) (*QueryResult, error) {
	selected := make(map[string]arrow.Record)
	for _, record := range records {
		selectKey := ""
		for i, column := range q.selectCols {
			val, err := getColumnValue(record, column)
			if err != nil {
				return nil, err
			}
			if i > 0 {
				selectKey += "-"
			}
			selectKey += fmt.Sprintf("%v", val)
		}
		selected[selectKey] = record
	}

	return &QueryResult{Records: records}, nil
}

func (q *Query) executeWindow(records []arrow.Record) (*QueryResult, error) {
	windowed := make(map[string][]arrow.Record)
	for _, record := range records {
		val, err := getColumnValue(record, "timestamp")
		if err != nil {
			return nil, err
		}
		ts := val.(arrow.Timestamp)
		windowKey := time.Unix(int64(ts), 0).Truncate(*q.window).String()
		windowed[windowKey] = append(windowed[windowKey], record)
	}

	return &QueryResult{Records: records}, nil
}

// Helper function to get column value
func getColumnValue(record arrow.Record, columnName string) (interface{}, error) {
	schema := record.Schema()
	for i, field := range schema.Fields() {
		if field.Name == columnName {
			col := record.Column(i)
			if col == nil {
				return nil, fmt.Errorf("nil column: %s", columnName)
			}

			switch arr := col.(type) {
			case *array.Int64:
				return arr.Value(0), nil
			case *array.Float64:
				return arr.Value(0), nil
			case *array.String:
				return arr.Value(0), nil
			case *array.Timestamp:
				return arr.Value(0), nil
			default:
				return nil, fmt.Errorf("unsupported column type: %T", col)
			}
		}
	}
	return nil, fmt.Errorf("column not found: %s", columnName)
}

// GroupByUserWindow performs a query on Arrow records to compute SUM(purchase_amount)
// grouped by user_id for records with a timestamp within the given window.
func GroupByUserWindow(records []arrow.Record, window time.Duration) (map[int64]float64, error) {
	query := NewQuery(records).
		Select("user_id", "purchase_amount").
		GroupByColumns("user_id").
		Aggregate("purchase_amount", Sum).
		Window(window)

	result, err := query.Execute()
	if err != nil {
		return nil, err
	}

	totals := make(map[int64]float64)
	for k, v := range result.Groups {
		userID, _ := strconv.ParseInt(strings.Split(k, "-")[0], 10, 64)
		totals[userID] = v.(float64)
	}

	return totals, nil
}

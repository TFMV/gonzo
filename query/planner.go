package query

import (
	"context"

	"github.com/TFMV/gonzo/index"
)

// Plan represents a query execution plan
type Plan struct {
	ColumnPruning   []string      // Columns needed
	Filters         []Filter      // Push-down filters
	Aggregations    []Aggregation // Early aggregations
	IndexStrategies map[string]index.Strategy
}

// Planner optimizes query execution
type Planner struct {
	indexManager *index.IndexManager
}

// NewPlanner creates a query planner
func NewPlanner(im *index.IndexManager) *Planner {
	return &Planner{indexManager: im}
}

// OptimizeQuery creates an execution plan for the query
func (p *Planner) OptimizeQuery(ctx context.Context, q *Query) (*Plan, error) {
	plan := &Plan{
		IndexStrategies: make(map[string]index.Strategy),
	}

	// Determine required columns
	plan.ColumnPruning = p.determineRequiredColumns(q)

	// Convert whereConds to []Filter
	filters := make([]Filter, len(q.whereConds))
	for i, cond := range q.whereConds {
		filters[i] = Filter{
			Column:   cond.Column,
			Operator: cond.Operator,
			Value:    cond.Value,
		}
	}

	// Push down filters
	plan.Filters = p.optimizeFilters(filters)

	// Convert map values to a slice
	aggregations := make([]Aggregation, 0, len(q.Aggregates))
	for _, agg := range q.Aggregates {
		aggregations = append(aggregations, agg)
	}

	// Plan early aggregations
	plan.Aggregations = p.planAggregations(aggregations)

	// Choose index strategies
	for _, col := range plan.ColumnPruning {
		strategy := p.chooseIndexStrategy(col, q)
		if strategy != index.RoaringBitmap {
			plan.IndexStrategies[col] = strategy
		}
	}

	return plan, nil
}

func (p *Planner) chooseIndexStrategy(col string, q *Query) index.Strategy {
	// Implement index strategy selection logic here
	return index.RoaringBitmap
}

func (p *Planner) determineRequiredColumns(q *Query) []string {
	required := make(map[string]bool)
	for _, col := range q.Columns {
		required[col] = true
	}

	// Collect keys into a slice
	columns := make([]string, 0, len(required))
	for col := range required {
		columns = append(columns, col)
	}

	return columns
}

func (p *Planner) optimizeFilters(filters []Filter) []Filter {
	for _, filter := range filters {
		if filter.Column == "" {
			return nil
		}
	}
	return filters
}

func (p *Planner) planAggregations(aggregations []Aggregation) []Aggregation {
	return aggregations
}

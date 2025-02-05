package db

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Pool is the Go memory allocator used by Arrow.
var Pool = memory.NewGoAllocator()

// Schema defines the schema for the transactions table.
var Schema = arrow.NewSchema([]arrow.Field{
	{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "purchase_amount", Type: arrow.PrimitiveTypes.Float64},
	{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_s},
}, nil)

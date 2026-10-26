package legacy

import (
	"fmt"
	"strings"
)

// VectorOps provides vector similarity search operations on top of ParticleDB.
type VectorOps struct {
	client *Client
}

// Search performs an approximate nearest-neighbor (ANN) search on the given
// table and vector column.
func (v *VectorOps) Search(table, column string, queryVector []float32, opts *VectorSearchOptions) ([]VectorSearchResult, error) {
	sqlStr, args := v.BuildSearchSQL(table, column, queryVector, opts)

	result, err := v.client.Query(sqlStr, args...)
	if err != nil {
		return nil, err
	}

	var results []VectorSearchResult
	for _, row := range result.Rows {
		dist := 0.0
		if d, ok := row["__distance"]; ok {
			if f, ok := d.(float64); ok {
				dist = f
			}
			delete(row, "__distance")
		}
		results = append(results, VectorSearchResult{
			Row:      row,
			Distance: dist,
		})
	}
	return results, nil
}

// Insert inserts a row with a vector column into the given table.
func (v *VectorOps) Insert(table string, data map[string]interface{}, vectorCol string, vector []float32) error {
	sqlStr, args := v.BuildInsertSQL(table, data, vectorCol, vector)
	_, err := v.client.Execute(sqlStr, args...)
	return err
}

// CreateIndex creates a vector index on the given table and column.
func (v *VectorOps) CreateIndex(table, column, indexType string, params map[string]interface{}) error {
	sqlStr := v.BuildCreateIndexSQL(table, column, indexType, params)
	_, err := v.client.Execute(sqlStr)
	return err
}

// --- SQL builders (exported for testing) ---

// BuildSearchSQL generates the SQL and arguments for a vector search query.
//
// Generated SQL:
//
//	SELECT {cols}, L2_DISTANCE({column}, VECTOR('[...]')) AS __distance
//	FROM {table}
//	[WHERE {filter}]
//	ORDER BY __distance
//	LIMIT {k}
func (v *VectorOps) BuildSearchSQL(table, column string, queryVector []float32, opts *VectorSearchOptions) (string, []interface{}) {
	vecLiteral := formatFloat32Slice(queryVector)

	cols := "id"
	if opts != nil && len(opts.Columns) > 0 {
		cols = strings.Join(opts.Columns, ", ")
	}

	k := 10
	if opts != nil && opts.K > 0 {
		k = opts.K
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("SELECT %s, L2_DISTANCE(", cols))
	b.WriteString(column)
	b.WriteString(", ")
	b.WriteString(vecLiteral)
	b.WriteString(") AS __distance FROM ")
	b.WriteString(table)

	if opts != nil && opts.Filter != "" {
		b.WriteString(" WHERE ")
		b.WriteString(opts.Filter)
	}
	b.WriteString(fmt.Sprintf(" ORDER BY __distance LIMIT %d", k))

	return b.String(), nil
}

// BuildInsertSQL generates the SQL and arguments for inserting a row with a
// vector column.
//
// Generated SQL:
//
//	INSERT INTO {table} (col1, col2, ..., vectorCol) VALUES ($1, $2, ..., VECTOR('[...]'))
func (v *VectorOps) BuildInsertSQL(table string, data map[string]interface{}, vectorCol string, vector []float32) (string, []interface{}) {
	vecLiteral := formatFloat32Slice(vector)

	// Collect keys in deterministic order for testability.
	keys := sortedKeys(data)

	var cols strings.Builder
	var vals strings.Builder

	for i, key := range keys {
		if i > 0 {
			cols.WriteString(", ")
			vals.WriteString(", ")
		}
		cols.WriteString(key)
		// Inline values to avoid extended protocol parameter binding issues
		val := data[key]
		switch v := val.(type) {
		case string:
			vals.WriteString(fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")))
		default:
			vals.WriteString(fmt.Sprintf("%v", v))
		}
	}

	// Append vector column (uses literal, not a parameter).
	if len(keys) > 0 {
		cols.WriteString(", ")
		vals.WriteString(", ")
	}
	cols.WriteString(vectorCol)
	vals.WriteString(vecLiteral)

	sqlStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, cols.String(), vals.String())
	return sqlStr, nil
}

// BuildCreateIndexSQL generates a CREATE INDEX statement for a vector index.
//
// Generated SQL:
//
//	CREATE INDEX idx_{table}_{column}_{indexType} ON {table} USING {indexType} ({column})
//	[WITH (param1 = 'val1', ...)]
func (v *VectorOps) BuildCreateIndexSQL(table, column, indexType string, params map[string]interface{}) string {
	name := fmt.Sprintf("idx_%s_%s_%s", table, column, indexType)
	sqlStr := fmt.Sprintf("CREATE INDEX %s ON %s USING %s (%s)", name, table, indexType, column)

	if len(params) > 0 {
		parts := make([]string, 0, len(params))
		for _, k := range sortedKeys(params) {
			parts = append(parts, fmt.Sprintf("%s = '%v'", k, params[k]))
		}
		sqlStr += " WITH (" + strings.Join(parts, ", ") + ")"
	}

	return sqlStr
}

// sortedKeys returns the keys of a map[string]interface{} in sorted order.
func sortedKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Simple insertion sort for typically small maps.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

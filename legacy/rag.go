package legacy

import (
	"errors"
	"fmt"
)

// RAGOps provides retrieval-augmented generation (RAG) query operations backed
// by ParticleDB's RAG SQL functions (RAG_QUERY, CREATE RAG PIPELINE).
type RAGOps struct {
	client *Client
}

// Query executes a RAG query using a named pipeline.
func (r *RAGOps) Query(question string, pipeline string) (string, error) {
	sql := r.BuildQuerySQL()
	row, err := r.client.QueryOne(sql, question, pipeline)
	if err != nil {
		return "", err
	}
	if row == nil {
		return "", nil
	}
	if ans, ok := row["answer"]; ok && ans != nil {
		return fmt.Sprintf("%v", ans), nil
	}
	return "", nil
}

// QueryWithOptions executes a RAG query with explicit table, column, and k
// parameters instead of a named pipeline.
func (r *RAGOps) QueryWithOptions(question, table, vectorCol, textCol string, k int) (string, error) {
	if table == "" || vectorCol == "" || textCol == "" {
		return "", errors.New("particledb: table, vectorCol, and textCol are required")
	}
	sql := r.BuildQueryWithOptionsSQL()
	row, err := r.client.QueryOne(sql, question, table, vectorCol, textCol, k)
	if err != nil {
		return "", err
	}
	if row == nil {
		return "", nil
	}
	if ans, ok := row["answer"]; ok && ans != nil {
		return fmt.Sprintf("%v", ans), nil
	}
	return "", nil
}

// CreatePipeline creates a named RAG pipeline with the given configuration.
func (r *RAGOps) CreatePipeline(name, table, vectorCol, textCol string, k int, mode string) error {
	sql := r.BuildCreatePipelineSQL(name, table, vectorCol, textCol, k, mode)
	_, err := r.client.Execute(sql)
	return err
}

// --- SQL builders (exported for testing) ---

// BuildQuerySQL returns the SQL for a pipeline-based RAG query.
func (r *RAGOps) BuildQuerySQL() string {
	return "SELECT RAG_QUERY($1, $2) AS answer"
}

// BuildQueryWithOptionsSQL returns the SQL for a RAG query with explicit options.
func (r *RAGOps) BuildQueryWithOptionsSQL() string {
	return "SELECT RAG_QUERY($1, $2, $3, $4, $5) AS answer"
}

// BuildCreatePipelineSQL generates a CREATE RAG PIPELINE DDL statement.
//
// Generated SQL:
//
//	CREATE RAG PIPELINE {name} TABLE {table} VECTOR_COLUMN {vectorCol}
//	TEXT_COLUMN {textCol} K {k} MODE '{mode}'
func (r *RAGOps) BuildCreatePipelineSQL(name, table, vectorCol, textCol string, k int, mode string) string {
	return fmt.Sprintf(
		"CREATE RAG PIPELINE %s TABLE %s VECTOR_COLUMN %s TEXT_COLUMN %s K %s MODE '%s'",
		name, table, vectorCol, textCol, itoa(k), mode,
	)
}

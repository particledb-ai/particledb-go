package legacy

import (
	"fmt"
	"strings"
)

// AIOps provides AI function operations backed by ParticleDB's AI SQL functions
// (EMBED, LLM_GENERATE, LLM_CLASSIFY, LLM_SUMMARIZE).
type AIOps struct {
	client *Client
}

// Embed generates an embedding vector for the given text using the specified
// model.
func (a *AIOps) Embed(text string, model string) ([]float32, error) {
	sql := a.BuildEmbedSQL(model)
	args := []interface{}{text}
	if model != "" {
		args = append(args, model)
	}
	row, err := a.client.QueryOne(sql, args...)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	emb, ok := row["embedding"]
	if !ok || emb == nil {
		return nil, nil
	}
	// Attempt to convert the result to []float32.
	switch v := emb.(type) {
	case []float32:
		return v, nil
	case []float64:
		result := make([]float32, len(v))
		for i, f := range v {
			result[i] = float32(f)
		}
		return result, nil
	case []interface{}:
		result := make([]float32, len(v))
		for i, item := range v {
			if f, ok := item.(float64); ok {
				result[i] = float32(f)
			}
		}
		return result, nil
	default:
		return nil, nil
	}
}

// EmbedBatch generates embedding vectors for a batch of texts using the
// specified model. Each text is embedded individually.
func (a *AIOps) EmbedBatch(texts []string, model string) ([][]float32, error) {
	results := make([][]float32, len(texts))
	for i, text := range texts {
		emb, err := a.Embed(text, model)
		if err != nil {
			return nil, fmt.Errorf("particledb: embed batch failed at index %d: %w", i, err)
		}
		results[i] = emb
	}
	return results, nil
}

// Generate produces text using an LLM with the given prompt and optional model.
func (a *AIOps) Generate(prompt string, model string) (string, error) {
	sql := a.BuildGenerateSQL(model)
	args := []interface{}{prompt}
	if model != "" {
		args = append(args, model)
	}
	row, err := a.client.QueryOne(sql, args...)
	if err != nil {
		return "", err
	}
	if row == nil {
		return "", nil
	}
	if result, ok := row["result"]; ok && result != nil {
		return fmt.Sprintf("%v", result), nil
	}
	return "", nil
}

// Classify classifies the given text into one of the provided labels.
func (a *AIOps) Classify(text string, labels []string) (string, error) {
	sql := a.BuildClassifySQL(labels)
	row, err := a.client.QueryOne(sql, text)
	if err != nil {
		return "", err
	}
	if row == nil {
		return "", nil
	}
	if label, ok := row["label"]; ok && label != nil {
		return fmt.Sprintf("%v", label), nil
	}
	return "", nil
}

// Summarize produces a summary of the given text.
func (a *AIOps) Summarize(text string) (string, error) {
	sql := a.BuildSummarizeSQL()
	row, err := a.client.QueryOne(sql, text)
	if err != nil {
		return "", err
	}
	if row == nil {
		return "", nil
	}
	if summary, ok := row["summary"]; ok && summary != nil {
		return fmt.Sprintf("%v", summary), nil
	}
	return "", nil
}

// --- SQL builders (exported for testing) ---

// BuildEmbedSQL returns the SQL for generating an embedding. If model is non-empty,
// the SQL includes a second parameter placeholder for the model name.
func (a *AIOps) BuildEmbedSQL(model string) string {
	if model != "" {
		return "SELECT EMBED($1, $2) AS embedding"
	}
	return "SELECT EMBED($1) AS embedding"
}

// BuildGenerateSQL returns the SQL for LLM text generation. If model is
// non-empty, includes a second parameter placeholder.
func (a *AIOps) BuildGenerateSQL(model string) string {
	if model != "" {
		return "SELECT LLM_GENERATE($1, $2) AS result"
	}
	return "SELECT LLM_GENERATE($1) AS result"
}

// BuildClassifySQL returns the SQL for LLM classification. Labels are embedded
// as a SQL ARRAY of string literals, each escaped by escapeLiteral (quotes
// doubled; the server runs with standard_conforming_strings on).
func (a *AIOps) BuildClassifySQL(labels []string) string {
	quoted := make([]string, len(labels))
	for i, l := range labels {
		quoted[i] = "'" + escapeLiteral(l) + "'"
	}
	labelsLiteral := "ARRAY[" + strings.Join(quoted, ",") + "]"
	return "SELECT LLM_CLASSIFY($1, " + labelsLiteral + ") AS label"
}

// BuildSummarizeSQL returns the SQL for LLM summarization.
func (a *AIOps) BuildSummarizeSQL() string {
	return "SELECT LLM_SUMMARIZE($1) AS summary"
}

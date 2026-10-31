package legacy

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

var injectionPayloads = []string{
	`'); DROP TABLE t; --`,
	`x' OR 1=1 --`,
	`\'); DROP TABLE t; --`,
	`it''s'''`,
	"héllo ☃ 𝄞 '; DROP TABLE t; --",
}

// outsideLiterals reports whether every quoted literal in sql closes, lexing
// standard-conforming strings (a doubled quote is the only escape), and returns
// the text outside literals, each literal shown as L.
func outsideLiterals(sql string) (string, bool) {
	var out strings.Builder
	for i := 0; i < len(sql); i++ {
		if sql[i] != '\'' {
			out.WriteByte(sql[i])
			continue
		}
		closed := false
		for i++; i < len(sql); i++ {
			if sql[i] == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
					continue
				}
				closed = true
				break
			}
		}
		if !closed {
			return out.String(), false
		}
		out.WriteString("L")
	}
	return out.String(), true
}

func TestBuildClassifySQLEscapesLabels(t *testing.T) {
	ops := &AIOps{}
	sql := ops.BuildClassifySQL(injectionPayloads)
	outside, ok := outsideLiterals(sql)
	if !ok || outside != "SELECT LLM_CLASSIFY($1, ARRAY[L,L,L,L,L]) AS label" {
		t.Fatalf("label escaped its literal: %s (outside literals: %s)", sql, outside)
	}
}

func TestBuildInsertSQLBindsValues(t *testing.T) {
	ops := &VectorOps{}
	data := map[string]interface{}{
		"a": injectionPayloads[0],
		"b": []string{`x'`, `'); DROP TABLE t; --`},
		"c": []byte(`{"k":"'); DROP TABLE t; --"}`),
	}
	sql, args := ops.BuildInsertSQL("docs", data, "embedding", []float32{1})
	if sql != "INSERT INTO docs (a, b, c, embedding) VALUES ($1, $2, $3, VECTOR('[1]'))" {
		t.Fatalf("sql %q", sql)
	}
	if len(args) != 3 || args[0] != injectionPayloads[0] {
		t.Fatalf("args %#v", args)
	}
}

func TestBuildCreateIndexSQLEscapesParams(t *testing.T) {
	ops := &VectorOps{}
	sql := ops.BuildCreateIndexSQL("docs", "v", "hnsw", map[string]interface{}{"m": injectionPayloads[0]})
	outside, ok := outsideLiterals(sql)
	if !ok || outside != "CREATE INDEX idx_docs_v_hnsw ON docs USING hnsw (v) WITH (m = L)" {
		t.Fatalf("param escaped its literal: %s", sql)
	}
}

func TestBuildCreatePipelineSQLEscapesMode(t *testing.T) {
	ops := &RAGOps{}
	sql := ops.BuildCreatePipelineSQL("p", "docs", "v", "body", 5, injectionPayloads[0])
	outside, ok := outsideLiterals(sql)
	if !ok || outside != "CREATE RAG PIPELINE p TABLE docs VECTOR_COLUMN v TEXT_COLUMN body K 5 MODE L" {
		t.Fatalf("mode escaped its literal: %s", sql)
	}
}

// TestLiveLegacyInjectionStoredAsData: hostile values through the legacy
// Client's Query/Execute (bound parameters) come back as data, and the
// canary table survives.
func TestLiveLegacyInjectionStoredAsData(t *testing.T) {
	portStr := os.Getenv("PDB_NATIVE_PORT")
	if portStr == "" {
		t.Skip("PDB_NATIVE_PORT not set")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = port
	db, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const tbl, canary = "legacy_inj", "legacy_inj_canary"
	for _, s := range []string{
		"DROP TABLE IF EXISTS " + tbl, "DROP TABLE IF EXISTS " + canary,
		"CREATE TABLE " + canary + " (id int8 PRIMARY KEY)", "INSERT INTO " + canary + " VALUES (1)",
		"CREATE TABLE " + tbl + " (id int8 PRIMARY KEY, s text)",
	} {
		if _, err := db.Execute(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	payloads := append(append([]string{}, injectionPayloads...), `'); DROP TABLE `+canary+`; --`, `0 OR 1=1 --`)
	for i, s := range payloads {
		if _, err := db.Execute("INSERT INTO "+tbl+" VALUES ($1, $2)", i, s); err != nil {
			t.Fatalf("insert %q: %v", s, err)
		}
	}
	for i, s := range payloads {
		res, err := db.Query("SELECT s FROM "+tbl+" WHERE s = $1 AND id = $2", s, i)
		if err != nil || len(res.Rows) != 1 || res.Rows[0]["s"] != s {
			t.Fatalf("payload %q: %v %v", s, res, err)
		}
	}
	// The S1 shape: a predicate-bypass argument must match nothing.
	res, err := db.Query("SELECT s FROM "+tbl+" WHERE id = $1 AND s = 'none'", "0 OR 1=1 --")
	if err == nil && len(res.Rows) != 0 {
		t.Fatalf("predicate bypass returned %d rows", len(res.Rows))
	}
	res, err = db.Query("SELECT count(*) AS n FROM " + canary)
	if err != nil || len(res.Rows) != 1 || res.Rows[0]["n"] != int64(1) {
		t.Fatalf("canary %v %v", res, err)
	}
}

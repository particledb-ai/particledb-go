package legacy

import (
	"strings"
	"testing"
)

// --- Config / DSN tests ---

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Host != "localhost" {
		t.Errorf("expected Host=localhost, got %q", cfg.Host)
	}
	if cfg.Port != 5440 {
		t.Errorf("expected Port=5440 (native wire), got %d", cfg.Port)
	}
	if cfg.Protocol != ProtocolNative {
		t.Errorf("expected Protocol=native, got %q", cfg.Protocol)
	}
	if cfg.User != "particledb" {
		t.Errorf("expected User=particledb, got %q", cfg.User)
	}
	if cfg.Password != "" {
		t.Errorf("expected Password='', got %q", cfg.Password)
	}
	if cfg.Database != "particledb" {
		t.Errorf("expected Database=particledb, got %q", cfg.Database)
	}
	if cfg.PoolSize != 10 {
		t.Errorf("expected PoolSize=10, got %d", cfg.PoolSize)
	}
	if cfg.SSLMode != "disable" {
		t.Errorf("expected SSLMode=disable, got %q", cfg.SSLMode)
	}
}

func TestConfigConnectionString(t *testing.T) {
	cfg := Config{
		Host:     "db.example.com",
		Port:     5433,
		User:     "admin",
		Password: "secret",
		Database: "mydb",
		PoolSize: 20,
		SSLMode:  "require",
	}
	dsn := cfg.DSN()

	expected := "host=db.example.com port=5433 user=admin password=secret dbname=mydb sslmode=require"
	if dsn != expected {
		t.Errorf("DSN mismatch\n  got:  %q\n  want: %q", dsn, expected)
	}
}

func TestConfigConnectionStringNoPassword(t *testing.T) {
	cfg := DefaultConfig()
	dsn := cfg.DSN()

	if strings.Contains(dsn, "password") {
		t.Errorf("DSN should not contain password when empty, got %q", dsn)
	}
	if !strings.Contains(dsn, "host=localhost") {
		t.Errorf("DSN should contain host=localhost, got %q", dsn)
	}
}

// --- Vector SQL tests ---

func TestVectorSearchSQL(t *testing.T) {
	ops := &VectorOps{}
	vec := []float32{1.0, 2.5, 3.0}
	opts := &VectorSearchOptions{K: 5, Columns: []string{"id", "title"}}

	sql, args := ops.BuildSearchSQL("documents", "embedding", vec, opts)

	if !strings.Contains(sql, "SELECT id, title, L2_DISTANCE(embedding, VECTOR('[1,2.5,3]'))") {
		t.Errorf("SQL should contain SELECT cols, L2_DISTANCE function, got %q", sql)
	}
	if !strings.Contains(sql, "FROM documents") {
		t.Errorf("SQL should contain FROM documents, got %q", sql)
	}
	if !strings.Contains(sql, "ORDER BY __distance") {
		t.Errorf("SQL should contain ORDER BY __distance, got %q", sql)
	}
	if !strings.Contains(sql, "LIMIT 5") {
		t.Errorf("SQL should contain LIMIT 5, got %q", sql)
	}
	if args != nil {
		t.Errorf("expected args=nil, got %v", args)
	}
}

func TestVectorSearchSQLWithFilter(t *testing.T) {
	ops := &VectorOps{}
	vec := []float32{0.1, 0.2}
	opts := &VectorSearchOptions{K: 3, Filter: "category = 'science'"}

	sql, _ := ops.BuildSearchSQL("papers", "vec", vec, opts)

	if !strings.Contains(sql, "WHERE category = 'science'") {
		t.Errorf("SQL should contain WHERE filter, got %q", sql)
	}
}

func TestVectorSearchSQLDefaultK(t *testing.T) {
	ops := &VectorOps{}
	vec := []float32{1.0}

	sql, args := ops.BuildSearchSQL("t", "c", vec, nil)

	if args != nil {
		t.Errorf("expected args=nil, got %v", args)
	}
	if !strings.Contains(sql, "LIMIT 10") {
		t.Errorf("expected default K=10 inlined in SQL, got %q", sql)
	}
}

func TestVectorInsertSQL(t *testing.T) {
	ops := &VectorOps{}
	data := map[string]interface{}{
		"title": "hello",
	}
	vec := []float32{0.5, 1.5}

	sql, args := ops.BuildInsertSQL("docs", data, "embedding", vec)

	if !strings.Contains(sql, "INSERT INTO docs") {
		t.Errorf("SQL should contain INSERT INTO docs, got %q", sql)
	}
	if !strings.Contains(sql, "title") {
		t.Errorf("SQL should contain column 'title', got %q", sql)
	}
	if !strings.Contains(sql, "embedding") {
		t.Errorf("SQL should contain column 'embedding', got %q", sql)
	}
	if !strings.Contains(sql, "VECTOR('[0.5,1.5]')") {
		t.Errorf("SQL should contain VECTOR literal, got %q", sql)
	}
	if len(args) != 1 || args[0] != "hello" {
		t.Errorf("expected args=[hello], got %v", args)
	}
	if strings.Contains(sql, "hello") || !strings.Contains(sql, "VALUES ($1, VECTOR(") {
		t.Errorf("value must be the bound parameter $1, got %q", sql)
	}
}

func TestVectorCreateIndexSQL(t *testing.T) {
	ops := &VectorOps{}
	params := map[string]interface{}{
		"m":                "24",
		"ef_construction":  "200",
	}

	sql := ops.BuildCreateIndexSQL("docs", "embedding", "hnsw", params)

	if !strings.Contains(sql, "CREATE INDEX idx_docs_embedding_hnsw") {
		t.Errorf("SQL should contain index name, got %q", sql)
	}
	if !strings.Contains(sql, "ON docs USING hnsw (embedding)") {
		t.Errorf("SQL should contain ON ... USING ..., got %q", sql)
	}
	if !strings.Contains(sql, "WITH (") {
		t.Errorf("SQL should contain WITH clause, got %q", sql)
	}
	if !strings.Contains(sql, "ef_construction = '200'") {
		t.Errorf("SQL should contain ef_construction param, got %q", sql)
	}
	if !strings.Contains(sql, "m = '24'") {
		t.Errorf("SQL should contain m param, got %q", sql)
	}
}

func TestVectorCreateIndexSQLNoParams(t *testing.T) {
	ops := &VectorOps{}

	sql := ops.BuildCreateIndexSQL("items", "vec", "ivfflat", nil)

	expected := "CREATE INDEX idx_items_vec_ivfflat ON items USING ivfflat (vec)"
	if sql != expected {
		t.Errorf("SQL mismatch\n  got:  %q\n  want: %q", sql, expected)
	}
}

// --- KV SQL tests ---

func TestKVSetSQL(t *testing.T) {
	ops := &KVOps{}
	sql := ops.BuildSetSQL()
	expected := "SELECT KV_SET($1, $2)"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestKVGetSQL(t *testing.T) {
	ops := &KVOps{}
	sql := ops.BuildGetSQL()
	expected := "SELECT KV_GET($1) AS value"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestKVDeleteSQL(t *testing.T) {
	ops := &KVOps{}
	sql := ops.BuildDeleteSQL()
	expected := "SELECT KV_DEL($1) AS result"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestKVMSetSQL(t *testing.T) {
	ops := &KVOps{}
	mapping := map[string]string{
		"a": "1",
		"b": "2",
	}
	sql, args := ops.BuildMSetSQL(mapping)

	// Keys are sorted, so order is a, b.
	expected := "SELECT KV_MSET($1, $2, $3, $4)"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
	if len(args) != 4 {
		t.Fatalf("expected 4 args, got %d", len(args))
	}
	// Sorted: a, 1, b, 2
	if args[0] != "a" || args[1] != "1" || args[2] != "b" || args[3] != "2" {
		t.Errorf("args mismatch: got %v", args)
	}
}

func TestKVMGetSQL(t *testing.T) {
	ops := &KVOps{}
	keys := []string{"key1", "key2", "key3"}
	sql, args := ops.BuildMGetSQL(keys)

	expected := "SELECT KV_GET($1) AS v0, KV_GET($2) AS v1, KV_GET($3) AS v2"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(args))
	}
	if args[0] != "key1" || args[1] != "key2" || args[2] != "key3" {
		t.Errorf("args mismatch: got %v", args)
	}
}

func TestKVIncrSQL(t *testing.T) {
	ops := &KVOps{}
	sql := ops.BuildIncrSQL()
	expected := "SELECT KV_INCRBY($1, $2) AS value"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestKVExistsSQL(t *testing.T) {
	ops := &KVOps{}
	sql := ops.BuildExistsSQL()
	expected := "SELECT KV_EXISTS($1) AS result"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestKVTTLSQL(t *testing.T) {
	ops := &KVOps{}
	sql := ops.BuildTTLSQL()
	expected := "SELECT KV_TTL($1) AS ttl"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestKVKeysSQL(t *testing.T) {
	ops := &KVOps{}
	sql := ops.BuildKeysSQL()
	expected := "SELECT KV_KEYS($1) AS key"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

// --- RAG SQL tests ---

func TestRAGQuerySQL(t *testing.T) {
	ops := &RAGOps{}
	sql := ops.BuildQuerySQL()
	expected := "SELECT RAG_QUERY($1, $2) AS answer"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestRAGQueryWithOptionsSQL(t *testing.T) {
	ops := &RAGOps{}
	sql := ops.BuildQueryWithOptionsSQL()
	expected := "SELECT RAG_QUERY($1, $2, $3, $4, $5) AS answer"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestRAGCreatePipelineSQL(t *testing.T) {
	ops := &RAGOps{}
	sql := ops.BuildCreatePipelineSQL("my_pipeline", "docs", "embedding", "content", 5, "hybrid")

	expected := "CREATE RAG PIPELINE my_pipeline TABLE docs VECTOR_COLUMN embedding TEXT_COLUMN content K 5 MODE 'hybrid'"
	if sql != expected {
		t.Errorf("SQL mismatch\n  got:  %q\n  want: %q", sql, expected)
	}
}

// --- AI SQL tests ---

func TestAIEmbedSQL(t *testing.T) {
	ops := &AIOps{}

	// With model
	sql := ops.BuildEmbedSQL("text-embedding-ada-002")
	expected := "SELECT EMBED($1, $2) AS embedding"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}

	// Without model
	sql = ops.BuildEmbedSQL("")
	expected = "SELECT EMBED($1) AS embedding"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestAIGenerateSQL(t *testing.T) {
	ops := &AIOps{}

	// With model
	sql := ops.BuildGenerateSQL("gpt-4")
	expected := "SELECT LLM_GENERATE($1, $2) AS result"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}

	// Without model
	sql = ops.BuildGenerateSQL("")
	expected = "SELECT LLM_GENERATE($1) AS result"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestAIClassifySQL(t *testing.T) {
	ops := &AIOps{}
	labels := []string{"positive", "negative", "neutral"}
	sql := ops.BuildClassifySQL(labels)

	expected := "SELECT LLM_CLASSIFY($1, ARRAY['positive','negative','neutral']) AS label"
	if sql != expected {
		t.Errorf("SQL mismatch\n  got:  %q\n  want: %q", sql, expected)
	}
}

func TestAISummarizeSQL(t *testing.T) {
	ops := &AIOps{}
	sql := ops.BuildSummarizeSQL()
	expected := "SELECT LLM_SUMMARIZE($1) AS summary"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

// --- Helper tests ---

func TestItoa(t *testing.T) {
	tests := []struct {
		input    int
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{42, "42"},
		{5432, "5432"},
		{-7, "-7"},
	}
	for _, tc := range tests {
		result := itoa(tc.input)
		if result != tc.expected {
			t.Errorf("itoa(%d) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestFormatFloat32Slice(t *testing.T) {
	tests := []struct {
		input    []float32
		expected string
	}{
		{[]float32{}, "VECTOR('[]')"},
		{[]float32{1.0}, "VECTOR('[1]')"},
		{[]float32{1.5, 2.5, 3.5}, "VECTOR('[1.5,2.5,3.5]')"},
		{[]float32{0.0, -1.0}, "VECTOR('[0,-1]')"},
	}
	for _, tc := range tests {
		result := formatFloat32Slice(tc.input)
		if result != tc.expected {
			t.Errorf("formatFloat32Slice(%v) = %q, want %q", tc.input, result, tc.expected)
		}
	}
}

func TestSortedKeys(t *testing.T) {
	m := map[string]interface{}{
		"c": 3,
		"a": 1,
		"b": 2,
	}
	keys := sortedKeys(m)
	if len(keys) != 3 || keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
		t.Errorf("expected [a b c], got %v", keys)
	}
}

// --- Client accessor tests ---

func TestClientAccessors(t *testing.T) {
	// Create a client without a real DB connection (nil executor).
	c := &Client{}

	if c.Vector() == nil {
		t.Error("Vector() should not return nil")
	}
	if c.KV() == nil {
		t.Error("KV() should not return nil")
	}
	if c.RAG() == nil {
		t.Error("RAG() should not return nil")
	}
	if c.AI() == nil {
		t.Error("AI() should not return nil")
	}
}

func TestBuildDSN(t *testing.T) {
	cfg := DefaultPGConfig()
	dsn := BuildDSN(cfg)
	expected := "host=localhost port=5432 user=particledb dbname=particledb sslmode=disable"
	if dsn != expected {
		t.Errorf("DSN mismatch\n  got:  %q\n  want: %q", dsn, expected)
	}
}

func TestKVExpireSQL(t *testing.T) {
	ops := &KVOps{}
	sql := ops.BuildExpireSQL()
	expected := "SELECT KV_EXPIRE($1, $2)"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

package legacy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Executor is the interface satisfied by both *sql.DB and *sql.Tx, allowing
// the SDK to execute queries against either a connection pool or a transaction
// (PG wire only).
type Executor interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// Client is the main entry point for interacting with ParticleDB.
//
// By default it speaks the PDB native wire (native != nil). With
// Config.Protocol = ProtocolPG (or ConnectPG) it uses database/sql (db/rawDB).
type Client struct {
	config Config
	db     Executor
	rawDB  *sql.DB // kept for Close() and Begin() (PG wire)
	native *PdbWireClient
	ntx    *nativeTx // set on the Client a native Transaction hands to fn
}

// New creates a new Client from the given Config. The connection pool is
// lazy: the first statement opens a connection.
//
// The default (Protocol empty or ProtocolNative) is the PDB native wire on
// Port (5440 when 0). ProtocolPG opens a database/sql pool with the
// "postgres" driver, which the application must import.
func New(config Config) (*Client, error) {
	switch config.Protocol {
	case "", ProtocolNative:
		return newNative(config)
	case ProtocolPG:
		return newPG(config, config.DSN())
	}
	return nil, fmt.Errorf("particledb: unknown protocol %q (use ProtocolNative or ProtocolPG)", config.Protocol)
}

func newNative(config Config) (*Client, error) {
	switch strings.ToLower(config.SSLMode) {
	case "", "disable", "allow", "prefer":
	default:
		return nil, fmt.Errorf("particledb: sslmode=%s is not supported on the native wire (no TLS); "+
			"use Protocol: ProtocolPG for a TLS connection", config.SSLMode)
	}
	config.Protocol = ProtocolNative
	if config.Host == "" {
		config.Host = "localhost"
	}
	if config.Port == 0 {
		config.Port = WireDefaultPort
	}
	opts := defaultConnectionOptions()
	opts.Host = config.Host
	opts.Port = config.Port
	if config.PoolSize > 0 {
		opts.PoolSize = config.PoolSize
	}
	opts.User = config.User
	opts.Password = config.Password
	opts.DisableUnixSocket = config.DisableUnixSocket
	opts.UnixSocketDir = config.UnixSocketDir
	return &Client{config: config, native: NewPdbWireClientWithOptions(opts)}, nil
}

func newPG(config Config, dsn string) (*Client, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("particledb: failed to open PG-wire connection (import a \"postgres\" "+
			"database/sql driver, e.g. _ \"github.com/lib/pq\"): %w", err)
	}
	if config.PoolSize > 0 {
		db.SetMaxOpenConns(config.PoolSize)
	}
	config.Protocol = ProtocolPG
	return &Client{config: config, db: db, rawDB: db}, nil
}

// Connect creates a Client from a connection string.
//
//   - "pdb://host:port" or "particledb://user:pass@host:port": native wire.
//   - key=value form ("host=h port=5440 user=u password=p pool_size=10"):
//     native wire, unless it contains protocol=pg.
//   - "postgres://..." / "postgresql://..." URLs, or key=value with
//     protocol=pg: PG wire via database/sql (the application must import a
//     "postgres" driver).
func Connect(connectionString string) (*Client, error) {
	cs := strings.TrimSpace(connectionString)
	lower := strings.ToLower(cs)
	switch {
	case strings.HasPrefix(lower, "postgres://"), strings.HasPrefix(lower, "postgresql://"):
		return newPG(Config{}, cs)
	case strings.HasPrefix(lower, "pdb://"), strings.HasPrefix(lower, "particledb://"):
		u, err := url.Parse(cs)
		if err != nil {
			return nil, fmt.Errorf("particledb: bad connection URL: %w", err)
		}
		cfg := DefaultConfig()
		if h := u.Hostname(); h != "" {
			cfg.Host = h
		}
		if p := u.Port(); p != "" {
			if cfg.Port, err = strconv.Atoi(p); err != nil {
				return nil, fmt.Errorf("particledb: bad port %q", p)
			}
		}
		if u.User != nil {
			cfg.User = u.User.Username()
			cfg.Password, _ = u.User.Password()
		}
		if ps := u.Query().Get("pool_size"); ps != "" {
			if cfg.PoolSize, err = strconv.Atoi(ps); err != nil {
				return nil, fmt.Errorf("particledb: bad pool_size %q", ps)
			}
		}
		if sm := u.Query().Get("sslmode"); sm != "" {
			cfg.SSLMode = sm
		}
		if err := applySocketParams(&cfg, u.Query().Get("unix_socket"), u.Query().Get("unix_socket_dir")); err != nil {
			return nil, err
		}
		return newNative(cfg)
	}
	kv, rest, isPG, err := parseKeyValueDSN(cs)
	if err != nil {
		return nil, err
	}
	if isPG {
		return newPG(Config{}, rest)
	}
	cfg := DefaultConfig()
	for k, v := range kv {
		switch k {
		case "host":
			cfg.Host = v
		case "port":
			if cfg.Port, err = strconv.Atoi(v); err != nil {
				return nil, fmt.Errorf("particledb: bad port %q", v)
			}
		case "user":
			cfg.User = v
		case "password":
			cfg.Password = v
		case "dbname":
			cfg.Database = v
		case "sslmode":
			cfg.SSLMode = v
		case "pool_size":
			if cfg.PoolSize, err = strconv.Atoi(v); err != nil {
				return nil, fmt.Errorf("particledb: bad pool_size %q", v)
			}
		case "unix_socket":
			if err := applySocketParams(&cfg, v, ""); err != nil {
				return nil, err
			}
		case "unix_socket_dir":
			cfg.UnixSocketDir = v
		default:
			return nil, fmt.Errorf("particledb: unknown native connection parameter %q "+
				"(add protocol=pg for a PG-wire DSN)", k)
		}
	}
	return newNative(cfg)
}

// applySocketParams reads the native URL / DSN socket knobs:
// unix_socket=on|off (default on) and unix_socket_dir=<dir>.
func applySocketParams(cfg *Config, onOff, dir string) error {
	switch strings.ToLower(onOff) {
	case "":
	case "on", "true", "1", "yes":
		cfg.DisableUnixSocket = false
	case "off", "false", "0", "no":
		cfg.DisableUnixSocket = true
	default:
		return fmt.Errorf("particledb: bad unix_socket %q (on|off)", onOff)
	}
	if dir != "" {
		cfg.UnixSocketDir = dir
	}
	return nil
}

// ConnectPG creates a Client over the PostgreSQL wire from a lib/pq-style DSN
// or postgres:// URL. The application must import a "postgres" database/sql
// driver (e.g. _ "github.com/lib/pq").
func ConnectPG(dsn string) (*Client, error) {
	return newPG(Config{}, dsn)
}

// parseKeyValueDSN splits "k=v k2='v 2'" pairs. It reports protocol=pg and
// returns the DSN with that key removed (for database/sql).
func parseKeyValueDSN(s string) (map[string]string, string, bool, error) {
	kv := map[string]string{}
	var kept []string
	isPG := false
	i := 0
	for i < len(s) {
		for i < len(s) && s[i] == ' ' {
			i++
		}
		if i >= len(s) {
			break
		}
		start := i
		eq := strings.IndexByte(s[i:], '=')
		if eq < 0 {
			return nil, "", false, fmt.Errorf("particledb: bad connection string near %q", s[i:])
		}
		key := strings.TrimSpace(s[i : i+eq])
		i += eq + 1
		var val strings.Builder
		if i < len(s) && s[i] == '\'' {
			i++
			for i < len(s) && s[i] != '\'' {
				if s[i] == '\\' && i+1 < len(s) {
					i++
				}
				val.WriteByte(s[i])
				i++
			}
			if i >= len(s) {
				return nil, "", false, fmt.Errorf("particledb: unterminated quote in connection string")
			}
			i++
		} else {
			for i < len(s) && s[i] != ' ' {
				val.WriteByte(s[i])
				i++
			}
		}
		if key == "protocol" {
			switch strings.ToLower(val.String()) {
			case "pg", "postgres", "postgresql":
				isPG = true
			case "native", "pdb":
			default:
				return nil, "", false, fmt.Errorf("particledb: unknown protocol %q", val.String())
			}
			continue
		}
		kv[key] = val.String()
		kept = append(kept, s[start:i])
	}
	return kv, strings.Join(kept, " "), isPG, nil
}

// newFromExecutor creates a Client that wraps an arbitrary Executor. This is
// used internally for transaction-scoped clients and for testing.
func newFromExecutor(exec Executor) *Client {
	return &Client{db: exec}
}

// Protocol reports the transport this client speaks.
func (c *Client) Protocol() Protocol {
	if c.native != nil || c.ntx != nil {
		return ProtocolNative
	}
	return ProtocolPG
}

// Native returns the underlying native wire client (nil on the PG wire).
func (c *Client) Native() *PdbWireClient {
	return c.native
}

// Ping verifies that the server is reachable.
func (c *Client) Ping() error {
	if c.native != nil {
		return c.native.Ping(context.Background())
	}
	if c.rawDB != nil {
		return c.rawDB.Ping()
	}
	return errors.New("particledb: no connection")
}

// Query executes a SQL query and returns all result rows.
//
// Native wire: without args a plain QUERY; with args a cached native prepared
// statement whose $1..$N are bound from a typed Arrow parameter batch.
// Values: integers -> int64, floats -> float64, text -> string, bytea ->
// []byte, bool -> bool, date/timestamp -> time.Time, numeric -> string.
func (c *Client) Query(sqlStr string, args ...interface{}) (*QueryResult, error) {
	if c.native != nil || c.ntx != nil {
		var res *arrowResult
		var err error
		if c.ntx != nil {
			res, err = c.ntx.query(sqlStr, args)
		} else {
			res, err = c.native.queryResult(context.Background(), sqlStr, args, false)
		}
		if err != nil {
			return nil, fmt.Errorf("particledb: query failed: %w", err)
		}
		return &QueryResult{Columns: res.columns, Rows: res.rows, RowCount: len(res.rows)}, nil
	}
	rows, err := c.db.Query(sqlStr, args...)
	if err != nil {
		return nil, fmt.Errorf("particledb: query failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("particledb: failed to get columns: %w", err)
	}

	var resultRows []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("particledb: scan failed: %w", err)
		}
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			row[col] = values[i]
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("particledb: rows iteration failed: %w", err)
	}

	return &QueryResult{
		Columns:  cols,
		Rows:     resultRows,
		RowCount: len(resultRows),
	}, nil
}

// QueryOne executes a SQL query and returns the first row, or nil if no rows
// are returned.
func (c *Client) QueryOne(sqlStr string, args ...interface{}) (map[string]interface{}, error) {
	result, err := c.Query(sqlStr, args...)
	if err != nil {
		return nil, err
	}
	if result.RowCount == 0 {
		return nil, nil
	}
	return result.Rows[0], nil
}

// Execute runs a SQL statement (INSERT, UPDATE, DELETE, DDL) and returns the
// number of rows affected. Args are bound as in Query.
func (c *Client) Execute(sqlStr string, args ...interface{}) (int64, error) {
	if c.native != nil || c.ntx != nil {
		var n int64
		var err error
		if c.ntx != nil {
			n, err = c.ntx.exec(sqlStr, args)
		} else {
			n, err = c.native.executeAffected(context.Background(), sqlStr, args, false)
		}
		if err != nil {
			return 0, fmt.Errorf("particledb: exec failed: %w", err)
		}
		return n, nil
	}
	result, err := c.db.Exec(sqlStr, args...)
	if err != nil {
		return 0, fmt.Errorf("particledb: exec failed: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		// Some drivers/statements don't support RowsAffected (e.g. DDL).
		return 0, nil
	}
	return affected, nil
}

// ExecuteBatch runs sqlStr once per parameter row.
//
// Native wire: one MSG_EXECUTE_BATCH round trip (split above ~12 MiB of
// parameters) carrying the rows as a typed Arrow batch. Outside a
// transaction every row autocommits independently; inside Transaction the
// server stops at the first failed row and the transaction is aborted. If any
// row failed, the per-row result is returned with a *PdbBatchError (wrapped;
// use errors.As).
//
// PG wire: the rows are executed one by one with the same per-row semantics.
func (c *Client) ExecuteBatch(sqlStr string, rows [][]interface{}) (*BatchResult, error) {
	if c.native != nil || c.ntx != nil {
		var res *BatchResult
		var err error
		if c.ntx != nil {
			res, err = c.ntx.execBatch(sqlStr, rows)
		} else {
			res, err = c.native.ExecuteBatch(context.Background(), sqlStr, rows)
		}
		if err != nil {
			return res, fmt.Errorf("particledb: batch failed: %w", err)
		}
		return res, nil
	}
	_, inTx := c.db.(*sql.Tx)
	res := &BatchResult{}
	for i, r := range rows {
		out, err := c.db.Exec(sqlStr, r...)
		res.Attempted++
		if err != nil {
			res.RowsAffected = append(res.RowsAffected, -1)
			res.Errors = append(res.Errors, BatchRowError{Row: i, Message: err.Error()})
			if inTx {
				break
			}
			continue
		}
		n, _ := out.RowsAffected()
		res.RowsAffected = append(res.RowsAffected, n)
	}
	if _, err := finishBatch(res, len(rows)); err != nil {
		return res, fmt.Errorf("particledb: batch failed: %w", err)
	}
	return res, nil
}

// Tx wraps a database transaction with the same Query/Execute interface as Client.
type Tx struct {
	client *Client
}

// Query executes a SQL query within the transaction.
func (tx *Tx) Query(sqlStr string, args ...interface{}) (*QueryResult, error) {
	return tx.client.Query(sqlStr, args...)
}

// QueryOne executes a SQL query within the transaction and returns the first row.
func (tx *Tx) QueryOne(sqlStr string, args ...interface{}) (map[string]interface{}, error) {
	return tx.client.QueryOne(sqlStr, args...)
}

// Execute runs a SQL statement within the transaction.
func (tx *Tx) Execute(sqlStr string, args ...interface{}) (int64, error) {
	return tx.client.Execute(sqlStr, args...)
}

// ExecuteBatch runs sqlStr once per row within the transaction; the server
// stops at the first failed row (the transaction is then aborted).
func (tx *Tx) ExecuteBatch(sqlStr string, rows [][]interface{}) (*BatchResult, error) {
	return tx.client.ExecuteBatch(sqlStr, rows)
}

// Transaction executes the given function within a database transaction. If fn
// returns nil, the transaction is committed; otherwise it is rolled back.
//
// Native wire: one pooled connection is held for the whole transaction
// (BEGIN ... COMMIT/ROLLBACK on it); every Tx call runs on that connection.
func (c *Client) Transaction(fn func(tx *Tx) error) error {
	if c.ntx != nil {
		return errors.New("particledb: nested transactions are not supported")
	}
	if c.native != nil {
		return c.nativeTransaction(fn)
	}
	if c.rawDB == nil {
		return errors.New("particledb: transactions require a *sql.DB connection")
	}
	sqlTx, err := c.rawDB.Begin()
	if err != nil {
		return fmt.Errorf("particledb: failed to begin transaction: %w", err)
	}
	txClient := &Client{db: sqlTx}
	tx := &Tx{client: txClient}

	if err := fn(tx); err != nil {
		_ = sqlTx.Rollback()
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("particledb: failed to commit transaction: %w", err)
	}
	return nil
}

// Close closes the underlying connection pool.
func (c *Client) Close() error {
	if c.native != nil {
		return c.native.Close()
	}
	if c.rawDB != nil {
		return c.rawDB.Close()
	}
	return nil
}

// Vector returns a VectorOps instance for vector similarity search operations.
func (c *Client) Vector() *VectorOps {
	return &VectorOps{client: c}
}

// KV returns a KVOps instance for Redis-compatible key-value operations.
func (c *Client) KV() *KVOps {
	return &KVOps{client: c}
}

// RAG returns a RAGOps instance for retrieval-augmented generation operations.
func (c *Client) RAG() *RAGOps {
	return &RAGOps{client: c}
}

// AI returns an AIOps instance for AI function operations.
func (c *Client) AI() *AIOps {
	return &AIOps{client: c}
}

// --- SQL builder helpers (exported for testing) ---

// BuildDSN generates a DSN string from the given config. This is identical to
// Config.DSN() but provided as a package-level function for convenience.
func BuildDSN(config Config) string {
	return config.DSN()
}

// formatFloat32Slice formats a []float32 as a ParticleDB VECTOR literal,
// e.g. "VECTOR('[1.5,2.3]')".
func formatFloat32Slice(v []float32) string {
	var b strings.Builder
	b.WriteString("VECTOR('[")
	for i, f := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(fmt.Sprintf("%g", f))
	}
	b.WriteString("]')")
	return b.String()
}

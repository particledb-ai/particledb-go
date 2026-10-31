package particledb

// database/sql driver, registered as "particledb":
//
//	db, err := sql.Open("particledb", "particledb://app@localhost:7173/db")
//
// or, with a Config:
//
//	db := sql.OpenDB(particledb.NewConnector(cfg))
//
// Each database/sql connection is one native connection. The native
// connection behind a *sql.Conn is reachable with Raw:
//
//	conn.Raw(func(dc any) error { pc := dc.(particledb.DriverConn).NativeConn(); ... })

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"time"
)

func init() {
	sql.Register("particledb", &Driver{})
}

// Driver is the database/sql driver.
type Driver struct{}

// Open opens a connection from a connection string.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	c, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

// OpenConnector parses dsn once for sql.DB.
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{cfg: cfg, drv: d}, nil
}

// NewConnector returns a driver.Connector for sql.OpenDB.
func NewConnector(cfg *Config) driver.Connector { return &connector{cfg: cfg.Copy(), drv: &Driver{}} }

type connector struct {
	cfg *Config
	drv *Driver
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	nc, err := ConnectConfig(ctx, c.cfg)
	if err != nil {
		return nil, err
	}
	return &driverConn{c: nc}, nil
}

func (c *connector) Driver() driver.Driver { return c.drv }

// DriverConn is implemented by the driver's connections; use it with
// sql.Conn.Raw to reach the native connection.
type DriverConn interface {
	NativeConn() *Conn
}

type driverConn struct {
	c  *Conn
	tx *Tx
}

var (
	_ driver.Conn               = (*driverConn)(nil)
	_ driver.ConnBeginTx        = (*driverConn)(nil)
	_ driver.ConnPrepareContext = (*driverConn)(nil)
	_ driver.ExecerContext      = (*driverConn)(nil)
	_ driver.QueryerContext     = (*driverConn)(nil)
	_ driver.SessionResetter    = (*driverConn)(nil)
	_ driver.Validator          = (*driverConn)(nil)
	_ driver.NamedValueChecker  = (*driverConn)(nil)
	_ driver.Pinger             = (*driverConn)(nil)
)

func (dc *driverConn) NativeConn() *Conn { return dc.c }

func (dc *driverConn) Prepare(query string) (driver.Stmt, error) {
	return dc.PrepareContext(context.Background(), query)
}

func (dc *driverConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	st, err := dc.c.Prepare(ctx, query)
	if err != nil {
		return nil, mapDriverErr(err)
	}
	return &driverStmt{dc: dc, st: st}, nil
}

func (dc *driverConn) Close() error { return dc.c.Close() }

func (dc *driverConn) Begin() (driver.Tx, error) {
	return dc.BeginTx(context.Background(), driver.TxOptions{})
}

func (dc *driverConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var o TxOptions
	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
	case sql.LevelReadUncommitted:
		o.IsoLevel = ReadUncommitted
	case sql.LevelReadCommitted:
		o.IsoLevel = ReadCommitted
	case sql.LevelRepeatableRead, sql.LevelSnapshot:
		o.IsoLevel = RepeatableRead
	case sql.LevelSerializable:
		o.IsoLevel = Serializable
	default:
		return nil, clientErr("0A000", "isolation level %v is not supported", sql.IsolationLevel(opts.Isolation))
	}
	if opts.ReadOnly {
		o.AccessMode = ReadOnly
	}
	tx, err := dc.c.BeginTx(ctx, o)
	if err != nil {
		return nil, mapDriverErr(err)
	}
	dc.tx = tx
	return &driverTx{dc: dc, tx: tx}, nil
}

type driverTx struct {
	dc *driverConn
	tx *Tx
}

func (t *driverTx) Commit() error {
	t.dc.tx = nil
	return t.tx.Commit(context.Background())
}

func (t *driverTx) Rollback() error {
	t.dc.tx = nil
	err := t.tx.Rollback(context.Background())
	if errors.Is(err, ErrTxClosed) {
		return nil
	}
	return err
}

func (dc *driverConn) Ping(ctx context.Context) error {
	if dc.c.IsClosed() {
		return driver.ErrBadConn
	}
	return dc.c.Ping(ctx)
}

func (dc *driverConn) ResetSession(ctx context.Context) error {
	if dc.c.IsClosed() {
		return driver.ErrBadConn
	}
	return nil
}

func (dc *driverConn) IsValid() bool { return !dc.c.IsClosed() }

// CheckNamedValue accepts every value the native encoder binds (slices as
// arrays, Decimal, UUID, json.RawMessage, time types, Valuers, ...).
func (dc *driverConn) CheckNamedValue(nv *driver.NamedValue) error {
	if nv.Name != "" {
		return clientErr("0A000", "named parameters are not supported; use $1..$n")
	}
	if _, err := classify(nv.Value, nv.Ordinal-1); err != nil {
		return err
	}
	return nil
}

func namedArgs(args []driver.NamedValue) []any {
	out := make([]any, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

// mapDriverErr turns a failure that happened before anything was sent into
// driver.ErrBadConn, so database/sql retries it on another connection.
func mapDriverErr(err error) error {
	var ns *errNotSent
	if errors.As(err, &ns) {
		var e *Error
		if errors.As(ns.err, &e) && e.SQLState[:2] == "08" {
			return driver.ErrBadConn
		}
		return ns.err
	}
	return err
}

func (dc *driverConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) == 0 && strings.IndexByte(query, ';') >= 0 {
		if stmts := splitStatements(query); len(stmts) > 1 && !anyTxnControl(stmts) {
			res, err := dc.multi(ctx, stmts)
			if err != nil {
				return nil, err
			}
			var n int64
			for _, r := range res {
				if !r.isArrow {
					n = r.affected
				}
			}
			return driverResult(n), nil
		}
	}
	tag, err := dc.c.Exec(ctx, query, namedArgs(args)...)
	if err != nil {
		return nil, mapDriverErr(err)
	}
	return driverResult(tag.RowsAffected()), nil
}

type driverResult int64

func (r driverResult) LastInsertId() (int64, error) {
	return 0, errors.New("particledb: LastInsertId is not supported; use RETURNING")
}
func (r driverResult) RowsAffected() (int64, error) { return int64(r), nil }

func anyTxnControl(stmts []string) bool {
	for _, s := range stmts {
		if isTxnControl(s) {
			return true
		}
	}
	return false
}

// multi runs a multi-statement string as one BATCH, atomically outside a
// transaction (as the server runs a multi-statement query), and returns
// every statement's result.
func (dc *driverConn) multi(ctx context.Context, stmts []string) ([]batchEntry, error) {
	var b Batch
	for _, s := range stmts {
		b.Queue(s)
	}
	br := dc.c.SendBatch(ctx, &b)
	if br.err != nil {
		return nil, mapDriverErr(br.err)
	}
	return br.entries, nil
}

func (dc *driverConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) == 0 && strings.IndexByte(query, ';') >= 0 {
		if stmts := splitStatements(query); len(stmts) > 1 && !anyTxnControl(stmts) {
			res, err := dc.multi(ctx, stmts)
			if err != nil {
				return nil, err
			}
			dr := &driverRows{dc: dc, entries: res}
			if err := dr.load(0); err != nil {
				return nil, err
			}
			return dr, nil
		}
	}
	r, err := dc.c.Query(ctx, query, namedArgs(args)...)
	if err != nil {
		return nil, mapDriverErr(err)
	}
	return &driverRows{dc: dc, r: r}, nil
}

type driverStmt struct {
	dc *driverConn
	st *Stmt
}

func (s *driverStmt) Close() error  { return s.st.Close(context.Background()) }
func (s *driverStmt) NumInput() int { return s.st.NumParams() }

func (s *driverStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamed(args))
}

func (s *driverStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamed(args))
}

func valuesToNamed(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, a := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return out
}

func (s *driverStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	tag, err := s.st.Exec(ctx, namedArgs(args)...)
	if err != nil {
		return nil, mapDriverErr(err)
	}
	return driverResult(tag.RowsAffected()), nil
}

func (s *driverStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	r, err := s.st.Query(ctx, namedArgs(args)...)
	if err != nil {
		return nil, mapDriverErr(err)
	}
	return &driverRows{dc: s.dc, r: r}, nil
}

func (s *driverStmt) CheckNamedValue(nv *driver.NamedValue) error { return s.dc.CheckNamedValue(nv) }

// driverRows adapts Rows (or the results of a multi-statement string).
type driverRows struct {
	dc      *driverConn
	r       *Rows
	entries []batchEntry
	cur     int
}

var (
	_ driver.RowsColumnTypeScanType         = (*driverRows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*driverRows)(nil)
	_ driver.RowsColumnTypeNullable         = (*driverRows)(nil)
	_ driver.RowsColumnTypeLength           = (*driverRows)(nil)
	_ driver.RowsColumnTypePrecisionScale   = (*driverRows)(nil)
	_ driver.RowsNextResultSet              = (*driverRows)(nil)
)

func (d *driverRows) load(i int) error {
	d.cur = i
	e := d.entries[i]
	if !e.isArrow {
		d.r = &Rows{row: -1, done: true, ended: true, tag: CommandTag{rows: e.affected}}
		return nil
	}
	r, err := rowsFromArrow(d.dc.c, e.arrow)
	if err != nil {
		return err
	}
	d.r = r
	return nil
}

func (d *driverRows) Columns() []string { return d.r.Columns() }

func (d *driverRows) Close() error {
	d.r.Close()
	return nil
}

func (d *driverRows) Next(dest []driver.Value) error {
	if !d.r.Next() {
		if err := d.r.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	loc := d.r.loc
	for j := range dest {
		dest[j] = d.r.cell(j).value(false, loc)
	}
	return nil
}

func (d *driverRows) HasNextResultSet() bool {
	return d.entries != nil && d.cur+1 < len(d.entries)
}

func (d *driverRows) NextResultSet() error {
	if !d.HasNextResultSet() {
		return io.EOF
	}
	d.r.Close()
	return d.load(d.cur + 1)
}

func (d *driverRows) ColumnTypeScanType(i int) reflect.Type {
	return d.r.fields[i].scanType(false)
}

func (d *driverRows) ColumnTypeDatabaseTypeName(i int) string {
	return d.r.fields[i].DatabaseTypeName
}

func (d *driverRows) ColumnTypeNullable(i int) (nullable, ok bool) {
	return d.r.fields[i].Nullable, true
}

func (d *driverRows) ColumnTypeLength(i int) (int64, bool) {
	fd := &d.r.fields[i]
	switch fd.DatabaseTypeName {
	case "VARCHAR", "BPCHAR":
		if fd.Length >= 0 {
			return fd.Length, true
		}
		return 1<<63 - 1, true
	case "TEXT", "BYTEA":
		return 1<<63 - 1, true
	}
	return 0, false
}

func (d *driverRows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	fd := &d.r.fields[i]
	if fd.DatabaseTypeName == "NUMERIC" {
		if fd.Precision >= 0 {
			return fd.Precision, fd.Scale, true
		}
		return 0, 0, false
	}
	return 0, 0, false
}

var _ = time.Second

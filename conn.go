package particledb

import (
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Conn is one connection to the server: one socket and one server session
// (its transaction, settings and prepared statements).
//
// A Conn is safe for concurrent use. Concurrent statements run on separate
// multiplexed streams of the socket, inside the same session: they share its
// transaction, and their order is undefined. Code that needs one statement to
// see another's effect waits for the first to return.
type Conn struct {
	wc  *wireConn
	cfg *Config

	stmtMu  sync.Mutex
	stmts   map[string]*stmtEntry
	lru     *list.List
	nameSeq atomic.Uint64

	schemaMu sync.Mutex
	schemas  map[string]*cachedSchema
	tables   map[string][]FieldDescription // CopyFrom target descriptions

	// transaction state of the session as this client drives it
	txMu     sync.Mutex
	txLazy   bool        // a BEGIN is pending: the next statement carries it
	txLazyA  atomic.Bool // txLazy, readable without txMu
	txActive *Tx

	createdAt time.Time
	lastUsed  atomic.Int64
	inflight  atomic.Int32
}

type cachedSchema struct {
	schema *arrowSchema
	fields []FieldDescription
}

type stmtEntry struct {
	sql     string
	name    string
	nParams int
	ready   chan struct{}
	err     error
	elem    *list.Element
	refs    int
	evicted bool
}

// Connect opens a connection described by a connection string (see
// ParseConfig).
func Connect(ctx context.Context, dsn string) (*Conn, error) {
	cfg, err := ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	return ConnectConfig(ctx, cfg)
}

// ConnectConfig opens a connection described by cfg.
func ConnectConfig(ctx context.Context, cfg *Config) (*Conn, error) {
	cfg = cfg.Copy()
	wc, err := dialWire(ctx, cfg)
	if err != nil {
		return nil, err
	}
	c := &Conn{
		wc:        wc,
		cfg:       cfg,
		stmts:     map[string]*stmtEntry{},
		lru:       list.New(),
		schemas:   map[string]*cachedSchema{},
		createdAt: time.Now(),
	}
	c.lastUsed.Store(time.Now().UnixNano())
	return c, nil
}

// Close closes the connection. An open transaction is rolled back by the
// server.
func (c *Conn) Close() error { return c.wc.Close() }

// IsClosed reports whether the connection is closed or broken.
func (c *Conn) IsClosed() bool { return c.wc.isClosed() }

// Ping checks the connection with a PING frame.
func (c *Conn) Ping(ctx context.Context) error {
	if c.IsClosed() {
		return errClosed
	}
	return c.wc.ping(ctx)
}

// Config returns a copy of the connection's configuration.
func (c *Conn) Config() *Config { return c.cfg.Copy() }

// NegotiatedProtocol is the protocol the handshake selected, named as the
// server's SHOW pdb.wire_protocol names it ("pdb-native-v2").
func (c *Conn) NegotiatedProtocol() string { return c.wc.hello.protocolName() }

// NegotiatedCapabilities are the capabilities the server echoed, sorted and
// named as SHOW pdb.wire_capabilities names them.
func (c *Conn) NegotiatedCapabilities() []string { return c.wc.hello.capabilityNames() }

// Transport is "tcp", "unix" or "tls".
func (c *Conn) Transport() string { return c.wc.transport }

// MaxStreams is the number of concurrent statements the server allows on
// this connection; more wait for a free stream.
func (c *Conn) MaxStreams() int { return cap(c.wc.slots) }

// InTransaction reports whether the session is inside a transaction block
// according to the server's latest answer.
func (c *Conn) InTransaction() bool {
	open, _ := c.wc.txnStatus()
	return open
}

func (c *Conn) schemaFor(m ipcMessage) (*arrowSchema, []FieldDescription) {
	c.schemaMu.Lock()
	cs, ok := c.schemas[string(m.meta)]
	c.schemaMu.Unlock()
	if ok {
		return cs.schema, cs.fields
	}
	s := parseSchema(m.header)
	fds := make([]FieldDescription, len(s.fields))
	for i, f := range s.fields {
		fds[i] = describeField(f)
	}
	c.schemaMu.Lock()
	if len(c.schemas) >= 512 {
		c.schemas = map[string]*cachedSchema{}
	}
	c.schemas[string(m.meta)] = &cachedSchema{schema: s, fields: fds}
	c.schemaMu.Unlock()
	return s, fds
}

// ---------------------------------------------------------------------------
// Statement cache
// ---------------------------------------------------------------------------

var errStmtGone = errors.New("prepared statement does not exist")

func isMissingStmt(err error) bool {
	var e *Error
	return errors.As(err, &e) && strings.Contains(e.Message, "prepared statement") && strings.Contains(e.Message, "does not exist")
}

// prepareNamed sends PREPARE and returns the parameter count.
func (c *Conn) prepareNamed(ctx context.Context, name, sql string) (int, error) {
	p := appendLenStr(make([]byte, 0, 8+len(name)+len(sql)), name)
	p = appendLenStr(p, sql)
	f, _, err := c.wc.roundTrip(ctx, fPrepare, 0, p)
	if err != nil {
		return 0, err
	}
	if f.typ != fPrepareOK || len(f.payload) < 2 {
		return 0, protoErr("unexpected answer 0x%02x to PREPARE", f.typ)
	}
	return int(binary.LittleEndian.Uint16(f.payload)), nil
}

func (c *Conn) closeStmt(name string) {
	p := appendLenStr(nil, name)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _, _ = c.wc.roundTrip(ctx, fCloseStmt, 0, p)
	}()
}

// acquireStmt returns a prepared handle for sql (preparing it on first use).
// release must be called after the EXECUTE answer arrives.
func (c *Conn) acquireStmt(ctx context.Context, sql string) (*stmtEntry, error) {
	capacity := c.cfg.stmtCacheCap()
	if capacity == 0 {
		e := &stmtEntry{sql: sql, name: "u" + strconv.FormatUint(c.nameSeq.Add(1), 10), refs: 1, evicted: true}
		n, err := c.prepareNamed(ctx, e.name, sql)
		if err != nil {
			return nil, err
		}
		e.nParams = n
		return e, nil
	}
	c.stmtMu.Lock()
	if e, ok := c.stmts[sql]; ok {
		e.refs++
		c.lru.MoveToFront(e.elem)
		c.stmtMu.Unlock()
		select {
		case <-e.ready:
		case <-ctx.Done():
			c.releaseStmt(e)
			return nil, canceledErr(ctx.Err(), "")
		}
		if e.err != nil {
			c.releaseStmt(e)
			return nil, e.err
		}
		return e, nil
	}
	e := &stmtEntry{sql: sql, name: "s" + strconv.FormatUint(c.nameSeq.Add(1), 10), ready: make(chan struct{}), refs: 1}
	e.elem = c.lru.PushFront(e)
	c.stmts[sql] = e
	var victims []*stmtEntry
	for c.lru.Len() > capacity {
		back := c.lru.Back()
		v := back.Value.(*stmtEntry)
		if v == e {
			break
		}
		c.lru.Remove(back)
		delete(c.stmts, v.sql)
		v.evicted = true
		if v.refs == 0 {
			victims = append(victims, v)
		}
	}
	c.stmtMu.Unlock()
	for _, v := range victims {
		if v.err == nil {
			c.closeStmt(v.name)
		}
	}
	n, err := c.prepareNamed(ctx, e.name, sql)
	e.nParams, e.err = n, err
	close(e.ready)
	if err != nil {
		c.stmtMu.Lock()
		if c.stmts[sql] == e {
			delete(c.stmts, sql)
			c.lru.Remove(e.elem)
		}
		e.refs--
		c.stmtMu.Unlock()
		return nil, err
	}
	return e, nil
}

func (c *Conn) releaseStmt(e *stmtEntry) {
	c.stmtMu.Lock()
	e.refs--
	closeIt := e.refs == 0 && e.evicted && e.err == nil
	c.stmtMu.Unlock()
	if closeIt {
		c.closeStmt(e.name)
	}
}

// forgetStmt drops a cache entry the server no longer has.
func (c *Conn) forgetStmt(e *stmtEntry) {
	c.stmtMu.Lock()
	if c.stmts[e.sql] == e {
		delete(c.stmts, e.sql)
		c.lru.Remove(e.elem)
	}
	e.evicted = true
	e.err = errStmtGone
	c.stmtMu.Unlock()
}

// ---------------------------------------------------------------------------
// Request execution
// ---------------------------------------------------------------------------

// txnFlags returns the request flags for a statement, and whether the
// statement carries the lazy BEGIN of a transaction (the caller then holds
// txMu until the answer arrives).
// setLazy sets txLazy (txMu held).
func (c *Conn) setLazy(v bool) {
	c.txLazy = v
	c.txLazyA.Store(v)
}

func (c *Conn) beginFlags() (byte, bool) {
	if !c.txLazyA.Load() {
		return 0, false
	}
	c.txMu.Lock()
	if c.txLazy {
		return flagTxnBegin, true
	}
	c.txMu.Unlock()
	return 0, false
}

func (c *Conn) endBegin(carried bool, err error) {
	if !carried {
		return
	}
	// The server ran the BEGIN once the request left the client (even when
	// the statement itself then failed).
	var ns *errNotSent
	if !errors.As(err, &ns) {
		c.setLazy(false)
	}
	c.txMu.Unlock()
}

// send runs one request and returns its first frame and stream. For an
// EXECUTE whose statement vanished server-side it re-prepares once.
func (c *Conn) send(ctx context.Context, sql string, args []any, wantStream bool) (*stream, frame, []Notice, error) {
	c.lastUsed.Store(time.Now().UnixNano())
	c.inflight.Add(1)
	defer c.inflight.Add(-1)
	if c.wc.isClosed() {
		return nil, frame{}, nil, &errNotSent{c.wc.err()}
	}
	if len(args) == 0 {
		buf := getPayload()
		*buf = append((*buf)[:0], sql...)
		s, f, n, err := c.request(ctx, fQuery, *buf, wantStream)
		putPayload(buf)
		return s, f, n, err
	}
	for attempt := 0; ; attempt++ {
		e, err := c.acquireStmt(ctx, sql)
		if err != nil {
			return nil, frame{}, nil, err
		}
		if e.nParams != len(args) {
			c.releaseStmt(e)
			return nil, frame{}, nil, clientErr("08P01", "bind message supplies %d parameters, but prepared statement requires %d", len(args), e.nParams)
		}
		buf := getPayload()
		payload, err := appendExecute((*buf)[:0], e.name, args, c.wc.hello)
		if err != nil {
			*buf = payload
			putPayload(buf)
			c.releaseStmt(e)
			return nil, frame{}, nil, err
		}
		s, f, notices, err := c.request(ctx, fExecute, payload, wantStream)
		*buf = payload
		putPayload(buf) // writeFrame copied it
		c.releaseStmt(e)
		if err != nil && attempt == 0 && isMissingStmt(err) {
			c.forgetStmt(e)
			continue
		}
		return s, f, notices, err
	}
}

// request sends one frame (carrying a pending BEGIN) and waits for the
// first answer frame. With wantStream the stream is returned for a result
// that continues (ARROW_STREAM).
func (c *Conn) request(ctx context.Context, typ byte, payload []byte, wantStream bool) (*stream, frame, []Notice, error) {
	flags, carried := c.beginFlags()
	var notices []Notice
	for attempt := 0; ; attempt++ {
		s, err := c.wc.start(ctx, typ, flags, payload)
		if err != nil {
			c.endBegin(carried, err)
			return nil, frame{}, nil, err
		}
		f, err := s.await(ctx, &notices)
		if err == nil && f.typ == fRstStreamS && len(f.payload) >= 2 && binary.LittleEndian.Uint16(f.payload) == errStreamLimit && attempt < 100 {
			if serr := sleepCtx(ctx, time.Duration(attempt+1)*time.Millisecond); serr != nil {
				c.endBegin(carried, &errNotSent{serr})
				return nil, frame{}, notices, canceledErr(serr, "")
			}
			continue
		}
		c.endBegin(carried, err)
		if len(notices) > 0 && c.cfg.OnNotice != nil {
			for _, n := range notices {
				c.cfg.OnNotice(c, n)
			}
		}
		if err != nil {
			return nil, frame{}, notices, err
		}
		switch f.typ {
		case fError:
			return nil, f, notices, frameError(f)
		case fRstStreamS:
			code := uint16(0)
			if len(f.payload) >= 2 {
				code = binary.LittleEndian.Uint16(f.payload)
			}
			return nil, f, notices, serverError(code, "stream reset by the server (code "+strconv.Itoa(int(code))+")")
		}
		return s, f, notices, nil
	}
}

// Exec runs a statement that returns no rows ($1..$n placeholders bound to
// args) and returns its outcome. A statement that does return rows is run
// to completion and its rows are counted.
func (c *Conn) Exec(ctx context.Context, sql string, args ...any) (CommandTag, error) {
	s, f, _, err := c.send(ctx, sql, args, true)
	if err != nil {
		return CommandTag{}, err
	}
	switch f.typ {
	case fOK:
		return tagFromOK(f), nil
	case fArrowResult, fArrowStream:
		r, err := newRows(ctx, c, s, f, nil)
		if err != nil {
			return CommandTag{}, err
		}
		for r.Next() {
		}
		r.Close()
		return r.CommandTag(), r.Err()
	}
	return CommandTag{}, protoErr("unexpected answer 0x%02x to a statement", f.typ)
}

// Query runs a statement and returns its rows. The rows stream: large results
// are decoded batch by batch as they arrive.
func (c *Conn) Query(ctx context.Context, sql string, args ...any) (*Rows, error) {
	r := &Rows{}
	if err := c.queryInto(ctx, r, sql, args); err != nil {
		return nil, err
	}
	return r, nil
}

func (c *Conn) queryInto(ctx context.Context, r *Rows, sql string, args []any) error {
	s, f, notices, err := c.send(ctx, sql, args, true)
	if err != nil {
		return err
	}
	return r.init(ctx, c, s, f, notices)
}

// QueryRow runs a query expected to return at most one row.
func (c *Conn) QueryRow(ctx context.Context, sql string, args ...any) *Row {
	row := &Row{}
	if row.err = c.queryInto(ctx, &row.own, sql, args); row.err == nil {
		row.rows = &row.own
	}
	return row
}

// ---------------------------------------------------------------------------
// Explicit prepared statements
// ---------------------------------------------------------------------------

// Stmt is a statement prepared on one connection.
type Stmt struct {
	c       *Conn
	sql     string
	name    string
	nParams int
	closed  atomic.Bool
}

// Prepare prepares sql ($1..$n placeholders) on the server.
func (c *Conn) Prepare(ctx context.Context, sql string) (*Stmt, error) {
	name := "p" + strconv.FormatUint(c.nameSeq.Add(1), 10)
	n, err := c.prepareNamed(ctx, name, sql)
	if err != nil {
		return nil, err
	}
	return &Stmt{c: c, sql: sql, name: name, nParams: n}, nil
}

// NumParams is the number of parameters the server found.
func (s *Stmt) NumParams() int { return s.nParams }

// SQL is the statement text.
func (s *Stmt) SQL() string { return s.sql }

// IsServerPrepared reports that the statement is executed by its server-side
// handle (always true for a Stmt).
func (s *Stmt) IsServerPrepared() bool { return !s.closed.Load() }

func (s *Stmt) exec(ctx context.Context, args []any) (*stream, frame, []Notice, error) {
	if s.closed.Load() {
		return nil, frame{}, nil, clientErr("26000", "prepared statement is closed")
	}
	if len(args) != s.nParams {
		return nil, frame{}, nil, clientErr("08P01", "bind message supplies %d parameters, but prepared statement requires %d", len(args), s.nParams)
	}
	buf := getPayload()
	defer putPayload(buf)
	payload, err := appendExecute((*buf)[:0], s.name, args, s.c.wc.hello)
	*buf = payload
	if err != nil {
		return nil, frame{}, nil, err
	}
	s.c.inflight.Add(1)
	defer s.c.inflight.Add(-1)
	return s.c.request(ctx, fExecute, payload, true)
}

// Query executes the statement.
func (s *Stmt) Query(ctx context.Context, args ...any) (*Rows, error) {
	st, f, notices, err := s.exec(ctx, args)
	if err != nil {
		return nil, err
	}
	return newRows(ctx, s.c, st, f, notices)
}

// QueryRow executes the statement expecting at most one row.
func (s *Stmt) QueryRow(ctx context.Context, args ...any) *Row {
	r, err := s.Query(ctx, args...)
	return &Row{rows: r, err: err}
}

// Exec executes the statement.
func (s *Stmt) Exec(ctx context.Context, args ...any) (CommandTag, error) {
	st, f, _, err := s.exec(ctx, args)
	if err != nil {
		return CommandTag{}, err
	}
	if f.typ == fOK {
		return tagFromOK(f), nil
	}
	r, err := newRows(ctx, s.c, st, f, nil)
	if err != nil {
		return CommandTag{}, err
	}
	for r.Next() {
	}
	r.Close()
	return r.CommandTag(), r.Err()
}

// Close deallocates the statement on the server.
func (s *Stmt) Close(ctx context.Context) error {
	if s.closed.Swap(true) {
		return nil
	}
	if s.c.IsClosed() {
		return nil
	}
	_, _, err := s.c.wc.roundTrip(ctx, fCloseStmt, 0, appendLenStr(nil, s.name))
	return err
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

// TxIsoLevel is a transaction isolation level.
type TxIsoLevel string

const (
	Serializable    TxIsoLevel = "SERIALIZABLE"
	RepeatableRead  TxIsoLevel = "REPEATABLE READ"
	ReadCommitted   TxIsoLevel = "READ COMMITTED"
	ReadUncommitted TxIsoLevel = "READ UNCOMMITTED"
)

// TxAccessMode is READ WRITE or READ ONLY.
type TxAccessMode string

const (
	ReadWrite TxAccessMode = "READ WRITE"
	ReadOnly  TxAccessMode = "READ ONLY"
)

// TxOptions are the options of BeginTx. The zero value is the session's
// default isolation (READ COMMITTED unless the server or session says
// otherwise), read-write.
type TxOptions struct {
	IsoLevel   TxIsoLevel
	AccessMode TxAccessMode
}

func (o TxOptions) beginSQL() string {
	if o.IsoLevel == "" && o.AccessMode == "" {
		return ""
	}
	s := "BEGIN"
	if o.IsoLevel != "" {
		s += " ISOLATION LEVEL " + string(o.IsoLevel)
	}
	if o.AccessMode != "" {
		s += " " + string(o.AccessMode)
	}
	return s
}

// ErrTxClosed is returned by a Tx used after Commit or Rollback.
var ErrTxClosed = &Error{SQLState: "25000", Severity: "ERROR", Message: "transaction is already committed or rolled back", BatchIndex: -1}

// ErrTxCommitRollback matches the error Commit returns when the server
// rolled the transaction back instead (it had failed).
var ErrTxCommitRollback error = &stateClass{"commit unexpectedly resulted in rollback", []string{"40000"}}

// Tx is a transaction on one connection. Statements run through it (or on
// its Conn) are part of it.
type Tx struct {
	c       *Conn
	done    atomic.Bool
	release func(error) // pool hook
}

// Begin starts a transaction with the session's defaults. Nothing is sent:
// the BEGIN rides on the transaction's first statement.
func (c *Conn) Begin(ctx context.Context) (*Tx, error) { return c.BeginTx(ctx, TxOptions{}) }

// BeginTx starts a transaction with the given options.
func (c *Conn) BeginTx(ctx context.Context, opts TxOptions) (*Tx, error) {
	if c.IsClosed() {
		return nil, errClosed
	}
	c.txMu.Lock()
	if c.txActive != nil || c.txLazy {
		c.txMu.Unlock()
		return nil, clientErr("25001", "a transaction is already in progress on this connection")
	}
	tx := &Tx{c: c}
	if sql := opts.beginSQL(); sql != "" {
		c.txMu.Unlock()
		if _, _, err := c.wc.roundTrip(ctx, fQuery, 0, []byte(sql)); err != nil {
			return nil, err
		}
		c.txMu.Lock()
	} else {
		c.setLazy(true)
	}
	c.txActive = tx
	c.txMu.Unlock()
	return tx, nil
}

// Conn returns the transaction's connection.
func (tx *Tx) Conn() *Conn { return tx.c }

func (tx *Tx) check() error {
	if tx.done.Load() {
		return ErrTxClosed
	}
	return nil
}

// Exec runs a statement in the transaction.
func (tx *Tx) Exec(ctx context.Context, sql string, args ...any) (CommandTag, error) {
	if err := tx.check(); err != nil {
		return CommandTag{}, err
	}
	return tx.c.Exec(ctx, sql, args...)
}

// Query runs a query in the transaction.
func (tx *Tx) Query(ctx context.Context, sql string, args ...any) (*Rows, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	return tx.c.Query(ctx, sql, args...)
}

// QueryRow runs a single-row query in the transaction.
func (tx *Tx) QueryRow(ctx context.Context, sql string, args ...any) *Row {
	if err := tx.check(); err != nil {
		return &Row{err: err}
	}
	return tx.c.QueryRow(ctx, sql, args...)
}

// SendBatch runs a batch in the transaction.
func (tx *Tx) SendBatch(ctx context.Context, b *Batch) *BatchResults {
	if err := tx.check(); err != nil {
		return &BatchResults{err: err}
	}
	return tx.c.SendBatch(ctx, b)
}

// CopyFrom bulk-loads rows inside the transaction.
func (tx *Tx) CopyFrom(ctx context.Context, table Identifier, columns []string, src CopyFromSource) (int64, error) {
	if err := tx.check(); err != nil {
		return 0, err
	}
	return tx.c.CopyFrom(ctx, table, columns, src)
}

func (tx *Tx) finish(ctx context.Context, sql string) (CommandTag, error) {
	if tx.done.Swap(true) {
		return CommandTag{}, ErrTxClosed
	}
	c := tx.c
	c.txMu.Lock()
	lazy := c.txLazy
	c.setLazy(false)
	c.txActive = nil
	c.txMu.Unlock()
	var tag CommandTag
	var err error
	if !lazy {
		var f frame
		f, _, err = c.wc.roundTrip(ctx, fQuery, 0, []byte(sql))
		if err == nil {
			tag = tagFromOK(f)
		}
	}
	if tx.release != nil {
		tx.release(err)
	}
	return tag, err
}

// Commit commits the transaction. When the transaction had failed the server
// rolls it back instead; Commit then returns an error matching
// ErrTxCommitRollback.
func (tx *Tx) Commit(ctx context.Context) error {
	tag, err := tx.finish(ctx, "COMMIT")
	if err != nil {
		return err
	}
	if tag.rolledBack {
		return &Error{SQLState: "40000", Severity: "ERROR", Message: "commit unexpectedly resulted in rollback: the transaction had failed", BatchIndex: -1}
	}
	return nil
}

// Rollback rolls the transaction back. It is safe to call after Commit (it
// then returns ErrTxClosed), so `defer tx.Rollback(ctx)` is the idiom.
func (tx *Tx) Rollback(ctx context.Context) error {
	_, err := tx.finish(ctx, "ROLLBACK")
	return err
}

// Savepoint creates a savepoint; RollbackTo and Release act on it.
func (tx *Tx) Savepoint(ctx context.Context, name string) error {
	_, err := tx.Exec(ctx, "SAVEPOINT "+quoteIdent(name))
	return err
}

// RollbackTo rolls back to a savepoint.
func (tx *Tx) RollbackTo(ctx context.Context, name string) error {
	_, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+quoteIdent(name))
	return err
}

// Release releases a savepoint.
func (tx *Tx) Release(ctx context.Context, name string) error {
	_, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+quoteIdent(name))
	return err
}

// BeginFunc runs fn in a transaction: committed when fn returns nil, rolled
// back otherwise.
func BeginFunc(ctx context.Context, b interface {
	BeginTx(context.Context, TxOptions) (*Tx, error)
}, opts TxOptions, fn func(*Tx) error) (err error) {
	tx, err := b.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil || recover() != nil {
			_ = tx.Rollback(context.Background())
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ---------------------------------------------------------------------------
// Key-value frames
// ---------------------------------------------------------------------------

// KVSet sets key to value in the server's key-value store (the keyspace the
// RESP wire serves). Binary safe; clears any TTL. Not transactional: it
// commits on its own even inside a transaction.
func (c *Conn) KVSet(ctx context.Context, key, value []byte) error {
	p := make([]byte, 0, 8+len(key)+len(value))
	p = binary.LittleEndian.AppendUint32(p, uint32(len(key)))
	p = append(p, key...)
	p = binary.LittleEndian.AppendUint32(p, uint32(len(value)))
	p = append(p, value...)
	f, _, err := c.wc.roundTrip(ctx, fKVSet, 0, p)
	if err != nil {
		return err
	}
	if f.typ != fOK {
		return protoErr("unexpected answer 0x%02x to KV_SET", f.typ)
	}
	return nil
}

func keyPayload(key []byte) []byte {
	p := make([]byte, 0, 4+len(key))
	p = binary.LittleEndian.AppendUint32(p, uint32(len(key)))
	return append(p, key...)
}

func bulkValue(p []byte) ([]byte, error) {
	if len(p) < 4 {
		return nil, protoErr("truncated KV value")
	}
	n := int(binary.LittleEndian.Uint32(p))
	if n < 0 || 4+n > len(p) {
		return nil, protoErr("KV value length past the frame")
	}
	return append([]byte(nil), p[4:4+n]...), nil
}

// KVGet returns the value of key, or nil when it is absent or expired.
func (c *Conn) KVGet(ctx context.Context, key []byte) ([]byte, error) {
	f, _, err := c.wc.roundTrip(ctx, fKVGet, 0, keyPayload(key))
	if err != nil {
		return nil, err
	}
	switch f.typ {
	case fKVNil:
		return nil, nil
	case fKVBulk:
		return bulkValue(f.payload)
	}
	return nil, protoErr("unexpected answer 0x%02x to KV_GET", f.typ)
}

// KVDel deletes key and reports whether it existed.
func (c *Conn) KVDel(ctx context.Context, key []byte) (bool, error) {
	f, _, err := c.wc.roundTrip(ctx, fKVDel, 0, keyPayload(key))
	if err != nil {
		return false, err
	}
	return tagFromOK(f).rows > 0, nil
}

// KVIncr increments the integer at key by one (an absent key counts from 0)
// and returns the new value.
func (c *Conn) KVIncr(ctx context.Context, key []byte) (int64, error) {
	f, _, err := c.wc.roundTrip(ctx, fKVIncr, 0, keyPayload(key))
	if err != nil {
		return 0, err
	}
	if f.typ != fKVBulk {
		return 0, protoErr("unexpected answer 0x%02x to KV_INCR", f.typ)
	}
	b, err := bulkValue(f.payload)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
}

// location is the zone timestamptz values are returned in.
func (c *Config) location() *time.Location { return time.Local }

package particledb

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"
)

// Pool is a set of connections for concurrent use.
//
// Statements issued on the Pool itself (Exec, Query, QueryRow, SendBatch)
// run in autocommit mode on a shared connection: each connection carries
// many concurrent statements on its multiplexed streams, so a small number
// of sockets serves many goroutines. The pool opens connections up to
// MaxConns while every open one is busy, then spreads statements over them.
//
// Acquire (and Begin, CopyFrom) take a connection for exclusive use: its
// session (transaction, settings) belongs to the caller until Release.
//
// Session settings changed with Pool.Exec ("SET ...") apply to whichever
// shared connection ran the statement; use Acquire for session state. A
// transaction cannot be started with Pool.Exec("BEGIN"); use Pool.Begin.
type Pool struct {
	cfg *Config

	mu      sync.Mutex
	conns   []*pconn
	dialing int
	closed  bool
	changed chan struct{} // closed and replaced whenever a connection frees up
	waiters int           // goroutines waiting on changed

	stop chan struct{}
	wg   sync.WaitGroup
}

type pconn struct {
	c        *Conn
	acquired bool
	writes   int32 // shared writes in flight (guarded by Pool.mu)
}

// NewPool creates a pool from a connection string (see ParseConfig for the
// pool_* parameters). No connection is opened until one is needed (or
// MinConns are opened in the background).
func NewPool(ctx context.Context, dsn string) (*Pool, error) {
	cfg, err := ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	return NewPoolConfig(ctx, cfg)
}

// NewPoolConfig creates a pool from cfg.
func NewPoolConfig(ctx context.Context, cfg *Config) (*Pool, error) {
	cfg = cfg.Copy()
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = runtime.GOMAXPROCS(0)
		if cfg.MaxConns < 4 {
			cfg.MaxConns = 4
		}
	}
	if cfg.MinConns > cfg.MaxConns {
		cfg.MinConns = cfg.MaxConns
	}
	if cfg.MaxConnLifetime <= 0 {
		cfg.MaxConnLifetime = time.Hour
	}
	if cfg.MaxConnIdleTime <= 0 {
		cfg.MaxConnIdleTime = 30 * time.Minute
	}
	if cfg.HealthCheckPeriod <= 0 {
		cfg.HealthCheckPeriod = time.Minute
	}
	p := &Pool{cfg: cfg, changed: make(chan struct{}), stop: make(chan struct{})}
	for i := 0; i < cfg.MinConns; i++ {
		c, err := ConnectConfig(ctx, cfg)
		if err != nil {
			p.Close()
			return nil, err
		}
		p.conns = append(p.conns, &pconn{c: c})
	}
	p.wg.Add(1)
	go p.maintain()
	return p, nil
}

// Config returns a copy of the pool's configuration.
func (p *Pool) Config() *Config { return p.cfg.Copy() }

// Close closes every connection. Acquired connections are closed when
// released.
func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	conns := p.conns
	p.conns = nil
	close(p.changed)
	p.mu.Unlock()
	close(p.stop)
	p.wg.Wait()
	for _, pc := range conns {
		if !pc.acquired {
			pc.c.Close()
		}
	}
}

// PoolStat is a snapshot of the pool.
type PoolStat struct {
	Total, Acquired, Idle, MaxConns int
}

// Stat returns a snapshot of the pool.
func (p *Pool) Stat() PoolStat {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := PoolStat{Total: len(p.conns), MaxConns: p.cfg.MaxConns}
	for _, pc := range p.conns {
		if pc.acquired {
			s.Acquired++
		} else if pc.c.inflight.Load() == 0 {
			s.Idle++
		}
	}
	return s
}

func (p *Pool) signalLocked() {
	if p.closed {
		return
	}
	close(p.changed)
	p.changed = make(chan struct{})
}

// removeLocked drops a dead connection.
func (p *Pool) removeLocked(pc *pconn) {
	for i, x := range p.conns {
		if x == pc {
			p.conns[i] = p.conns[len(p.conns)-1]
			p.conns = p.conns[:len(p.conns)-1]
			break
		}
	}
	p.signalLocked()
}

var errPoolClosed = &Error{SQLState: "08003", Severity: "FATAL", Message: "pool is closed", BatchIndex: -1}

// pick returns a connection: exclusive (marked acquired) or shared.
//
// A shared read goes to the least loaded connection, and a new connection is
// opened (up to MaxConns) once every connection carries SharedStreamsPerConn
// statements: a multiplexed session serves concurrent reads well. A shared
// write goes to a connection with no write in flight, opening one when there
// is none: the server waits for a statement's commit to be durable while it
// holds the session, so writes on one session do not overlap.
func (p *Pool) pick(ctx context.Context, exclusive bool) (*pconn, error) {
	return p.pickFor(ctx, exclusive, false)
}

func (p *Pool) pickFor(ctx context.Context, exclusive, write bool) (*pconn, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, errPoolClosed
		}
		var best *pconn
		bestLoad, bestWrites := int32(1<<30), int32(1<<30)
		for i := 0; i < len(p.conns); i++ {
			pc := p.conns[i]
			if pc.acquired {
				continue
			}
			if pc.c.IsClosed() {
				p.removeLocked(pc)
				i--
				continue
			}
			l, w := pc.c.inflight.Load(), pc.writes
			if write {
				if w < bestWrites || (w == bestWrites && l < bestLoad) {
					best, bestLoad, bestWrites = pc, l, w
				}
			} else if l < bestLoad {
				best, bestLoad, bestWrites = pc, l, w
			}
		}
		canDial := len(p.conns)+p.dialing < p.cfg.MaxConns
		var use bool
		switch {
		case best == nil:
		case exclusive:
			use = bestLoad == 0
		case write:
			// Never two writes on one session (they would wait for each
			// other's durable commit); past MaxConns a write waits for one.
			use = bestWrites == 0
		default:
			use = bestLoad < int32(p.cfg.sharedStreams()) || !canDial
		}
		if use {
			if exclusive {
				best.acquired = true
			} else {
				best.c.inflight.Add(1) // under p.mu: Acquire cannot take it now
				if write {
					best.writes++
				}
			}
			p.mu.Unlock()
			return best, nil
		}
		if canDial {
			p.dialing++
			p.mu.Unlock()
			c, err := ConnectConfig(ctx, p.cfg)
			p.mu.Lock()
			p.dialing--
			if err != nil {
				p.signalLocked()
				p.mu.Unlock()
				return nil, err
			}
			pc := &pconn{c: c, acquired: exclusive}
			if !exclusive {
				c.inflight.Add(1)
				if write {
					pc.writes++
				}
			}
			if p.closed {
				p.mu.Unlock()
				c.Close()
				return nil, errPoolClosed
			}
			p.conns = append(p.conns, pc)
			p.signalLocked() // shared waiters can use it too
			p.mu.Unlock()
			return pc, nil
		}
		wait := p.changed
		p.waiters++
		p.mu.Unlock()
		select {
		case <-wait:
			p.mu.Lock()
			p.waiters--
			p.mu.Unlock()
		case <-ctx.Done():
			p.mu.Lock()
			p.waiters--
			p.mu.Unlock()
			return nil, canceledErr(ctx.Err(), "canceled while waiting for a pool connection")
		}
	}
}

// done ends a shared statement started by pickFor.
func (p *Pool) done(pc *pconn, write bool) {
	if write {
		p.endWrite(pc)
	}
	pc.c.inflight.Add(-1)
}

// endWrite releases a shared write slot and wakes waiting callers.
func (p *Pool) endWrite(pc *pconn) {
	p.mu.Lock()
	pc.writes--
	if p.waiters > 0 {
		p.signalLocked()
	}
	p.mu.Unlock()
}

// isWrite classifies a statement for routing: anything but a plain query.
func isWrite(sql string) bool {
	switch firstKeyword(sql) {
	case "SELECT", "WITH", "SHOW", "VALUES", "TABLE", "EXPLAIN":
		return false
	}
	return true
}

// PoolConn is a connection acquired from a Pool.
type PoolConn struct {
	*Conn
	p    *Pool
	pc   *pconn
	once sync.Once
}

// Acquire takes a connection for exclusive use. Release it when done.
func (p *Pool) Acquire(ctx context.Context) (*PoolConn, error) {
	pc, err := p.pick(ctx, true)
	if err != nil {
		return nil, err
	}
	return &PoolConn{Conn: pc.c, p: p, pc: pc}, nil
}

// Release returns the connection to the pool. An open transaction is rolled
// back first; a broken connection is discarded.
func (c *PoolConn) Release() {
	c.once.Do(func() { c.p.release(c.pc) })
}

func (p *Pool) release(pc *pconn) {
	c := pc.c
	if !c.IsClosed() {
		c.txMu.Lock()
		active := c.txActive != nil && !c.txLazy
		c.txActive = nil
		c.setLazy(false)
		c.txMu.Unlock()
		if active || c.InTransaction() {
			ctx, cancel := context.WithTimeout(context.Background(), c.cfg.cancelGrace())
			_, _, err := c.wc.roundTrip(ctx, fQuery, 0, []byte("ROLLBACK"))
			cancel()
			if err != nil {
				c.Close()
			}
		}
	}
	p.mu.Lock()
	pc.acquired = false
	if c.IsClosed() || p.closed || time.Since(c.createdAt) > p.cfg.MaxConnLifetime {
		if !p.closed {
			p.removeLocked(pc)
		}
		p.mu.Unlock()
		c.Close()
		return
	}
	p.signalLocked()
	p.mu.Unlock()
}

// AcquireFunc runs fn with an acquired connection and releases it.
func (p *Pool) AcquireFunc(ctx context.Context, fn func(*PoolConn) error) error {
	c, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer c.Release()
	return fn(c)
}

var errPoolTxControl = clientErr("25000", "transaction control on a Pool's shared connection is not allowed: use Pool.Begin or Pool.Acquire")

func (p *Pool) sharedConn(ctx context.Context, sql string, write bool) (*pconn, error) {
	if isTxnControl(sql) {
		return nil, errPoolTxControl
	}
	return p.pickFor(ctx, false, write)
}

// guardTxn rolls back a transaction a shared statement left open.
func (p *Pool) guardTxn(pc *pconn, err error) error {
	if pc.c.InTransaction() {
		ctx, cancel := context.WithTimeout(context.Background(), pc.c.cfg.cancelGrace())
		_, _, _ = pc.c.wc.roundTrip(ctx, fQuery, 0, []byte("ROLLBACK"))
		cancel()
		if err == nil {
			return errPoolTxControl
		}
	}
	return err
}

// Exec runs a statement on a shared connection.
func (p *Pool) Exec(ctx context.Context, sql string, args ...any) (CommandTag, error) {
	write := isWrite(sql)
	for attempt := 0; ; attempt++ {
		pc, err := p.sharedConn(ctx, sql, write)
		if err != nil {
			return CommandTag{}, err
		}
		tag, err := pc.c.Exec(ctx, sql, args...)
		p.done(pc, write)
		var ns *errNotSent
		if err != nil && errors.As(err, &ns) && attempt < 3 && ctx.Err() == nil {
			continue // the connection was already broken; nothing was sent
		}
		return tag, p.guardTxn(pc, err)
	}
}

// Query runs a query on a shared connection.
func (p *Pool) Query(ctx context.Context, sql string, args ...any) (*Rows, error) {
	r := &Rows{}
	if err := p.queryInto(ctx, r, sql, args); err != nil {
		return nil, err
	}
	return r, nil
}

func (p *Pool) queryInto(ctx context.Context, r *Rows, sql string, args []any) error {
	write := isWrite(sql)
	for attempt := 0; ; attempt++ {
		pc, err := p.sharedConn(ctx, sql, write)
		if err != nil {
			return err
		}
		err = pc.c.queryInto(ctx, r, sql, args)
		if err != nil {
			p.done(pc, write)
			var ns *errNotSent
			if errors.As(err, &ns) && attempt < 3 && ctx.Err() == nil {
				continue
			}
			return p.guardTxn(pc, err)
		}
		if write {
			// a write's result is complete when queryInto returns for
			// everything but a streamed RETURNING; release the write slot now
			p.endWrite(pc)
		}
		if r.closed {
			pc.c.inflight.Add(-1)
		} else {
			r.pooled = pc.c
		}
		return nil
	}
}

// QueryRow runs a single-row query on a shared connection.
func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) *Row {
	row := &Row{}
	if row.err = p.queryInto(ctx, &row.own, sql, args); row.err == nil {
		row.rows = &row.own
	}
	return row
}

// SendBatch runs a batch (atomically) on a shared connection.
func (p *Pool) SendBatch(ctx context.Context, b *Batch) *BatchResults {
	for _, it := range b.items {
		if isTxnControl(it.sql) {
			return &BatchResults{err: errPoolTxControl, failAt: 0}
		}
	}
	if b.bulkShaped() {
		// one bulk ingest per session: take a connection for it
		c, err := p.Acquire(ctx)
		if err != nil {
			return &BatchResults{err: err, failAt: 0}
		}
		defer c.Release()
		return c.SendBatch(ctx, b)
	}
	pc, err := p.pickFor(ctx, false, true)
	if err != nil {
		return &BatchResults{err: err, failAt: 0}
	}
	br := pc.c.SendBatch(ctx, b)
	p.done(pc, true)
	return br
}

// CopyFrom bulk-loads rows on an acquired connection.
func (p *Pool) CopyFrom(ctx context.Context, table Identifier, columns []string, src CopyFromSource) (int64, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer c.Release()
	return c.CopyFrom(ctx, table, columns, src)
}

// Begin starts a transaction on an acquired connection, released when the
// transaction ends.
func (p *Pool) Begin(ctx context.Context) (*Tx, error) { return p.BeginTx(ctx, TxOptions{}) }

// BeginTx starts a transaction with options on an acquired connection.
func (p *Pool) BeginTx(ctx context.Context, opts TxOptions) (*Tx, error) {
	c, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := c.BeginTx(ctx, opts)
	if err != nil {
		c.Release()
		return nil, err
	}
	tx.release = func(error) { c.Release() }
	return tx, nil
}

// Ping checks a connection.
func (p *Pool) Ping(ctx context.Context) error {
	pc, err := p.pick(ctx, false)
	if err != nil {
		return err
	}
	defer pc.c.inflight.Add(-1)
	return pc.c.Ping(ctx)
}

// maintain closes idle and expired connections and keeps MinConns open.
func (p *Pool) maintain() {
	defer p.wg.Done()
	t := time.NewTicker(p.cfg.HealthCheckPeriod)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
		}
		var drop []*Conn
		now := time.Now()
		p.mu.Lock()
		for i := 0; i < len(p.conns); i++ {
			pc := p.conns[i]
			if pc.acquired || pc.c.inflight.Load() != 0 {
				continue
			}
			idle := now.Sub(time.Unix(0, pc.c.lastUsed.Load()))
			if pc.c.IsClosed() || now.Sub(pc.c.createdAt) > p.cfg.MaxConnLifetime ||
				(idle > p.cfg.MaxConnIdleTime && len(p.conns) > p.cfg.MinConns) {
				drop = append(drop, pc.c)
				p.conns[i] = p.conns[len(p.conns)-1]
				p.conns = p.conns[:len(p.conns)-1]
				i--
			}
		}
		need := p.cfg.MinConns - len(p.conns) - p.dialing
		p.mu.Unlock()
		for _, c := range drop {
			c.Close()
		}
		for i := 0; i < need; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), p.cfg.connectTimeout())
			c, err := ConnectConfig(ctx, p.cfg)
			cancel()
			if err != nil {
				break
			}
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				c.Close()
				return
			}
			p.conns = append(p.conns, &pconn{c: c})
			p.signalLocked()
			p.mu.Unlock()
		}
	}
}

package particledb

import (
	"context"
	"errors"
)

// Batch queues statements to run in one round trip.
//
// The statements travel in one BATCH frame and run in order. Outside a
// transaction the batch is atomic (it is wrapped in BEGIN ... COMMIT): if
// one statement fails, none of them take effect. Inside a transaction the
// statements are part of it, and a failure fails the transaction.
//
// Parameters are bound by value: each $n is replaced with the literal the
// server itself renders for the same typed parameter, so a batched statement
// behaves exactly like the same statement executed with parameters.
type Batch struct {
	items []batchItem
}

type batchItem struct {
	sql  string
	args []any
}

// Queue adds a statement.
func (b *Batch) Queue(sql string, args ...any) { b.items = append(b.items, batchItem{sql, args}) }

// Len is the number of queued statements.
func (b *Batch) Len() int { return len(b.items) }

// BatchResults reads the results of a batch in the order it was queued.
type BatchResults struct {
	c       *Conn
	entries []batchEntry
	idx     int
	err     error // failure of the batch
	failAt  int   // index of the failed statement (-1 none)
	closed  bool
	cleanup func(error)
}

// maxBatchFrame is the payload size at which a batch is split into several
// frames (sent one after another inside its transaction).
const maxBatchFrame = 8 << 20

// maxBatchStatements is the server's limit of statements per BATCH frame.
const maxBatchStatements = 10000

var allCaps = helloOK{caps: capV2Mux | capPrepared, ext: capExtTypedParams | capExtArrayParams}

// SendBatch runs b and returns its results.
func (c *Conn) SendBatch(ctx context.Context, b *Batch) *BatchResults {
	br := &BatchResults{c: c, failAt: -1}
	if len(b.items) == 0 {
		return br
	}
	if bb, ok := c.tryBulkBatch(ctx, b); ok {
		return bb
	}
	sqls := make([]string, 0, len(b.items)+2)
	var ps []pval
	for i, it := range b.items {
		if len(it.args) == 0 {
			sqls = append(sqls, it.sql)
			continue
		}
		var err error
		ps, err = classifyArgs(ps[:0], it.args, allCaps)
		if err != nil {
			br.err = withBatchIndex(err, i)
			br.failAt = i
			return br
		}
		s, err := renderSQL(it.sql, ps)
		if err != nil {
			br.err = withBatchIndex(err, i)
			br.failAt = i
			return br
		}
		sqls = append(sqls, s)
	}
	c.inflight.Add(1)
	defer c.inflight.Add(-1)

	// Decide the transaction wrapping, holding txMu so a lazy BEGIN is
	// consumed exactly once.
	c.txMu.Lock()
	inTx := c.txActive != nil
	lazy := c.txLazy
	open, _ := c.wc.txnStatus()
	wrap := !inTx && !open
	lead := 0
	if wrap || lazy {
		sqls = append([]string{"BEGIN"}, sqls...)
		lead = 1
	}
	if wrap {
		sqls = append(sqls, "COMMIT")
	}
	if lazy {
		c.setLazy(false)
	}
	c.txMu.Unlock()

	entries, failed, err := c.runBatch(ctx, sqls)
	if err != nil {
		br.err = err
		br.failAt = 0
		return br
	}
	n := len(b.items)
	if failed != nil {
		k := len(entries) - lead // index of the failing statement
		if k >= n {
			k = n - 1 // the COMMIT itself failed
		}
		if k < 0 {
			k = 0
		}
		failed.BatchIndex = k
		br.err = failed
		br.failAt = k
		if wrap {
			ctx2, cancel := context.WithTimeout(context.Background(), c.cfg.cancelGrace())
			_, _, _ = c.wc.roundTrip(ctx2, fQuery, 0, []byte("ROLLBACK"))
			cancel()
		}
	}
	if lead <= len(entries) {
		entries = entries[lead:]
	} else {
		entries = nil
	}
	if len(entries) > n {
		entries = entries[:n]
	}
	br.entries = entries
	return br
}

func withBatchIndex(err error, i int) error {
	var e *Error
	if errors.As(err, &e) {
		cp := *e
		cp.BatchIndex = i
		return &cp
	}
	return err
}

// runBatch sends sqls in one or more BATCH frames (sequentially) and returns
// the entries of the statements that ran and the failure, if any.
func (c *Conn) runBatch(ctx context.Context, sqls []string) ([]batchEntry, *Error, error) {
	var all []batchEntry
	for start := 0; start < len(sqls); {
		end, size := start, 4
		for end < len(sqls) && end-start < maxBatchStatements && (end == start || size+4+len(sqls[end]) <= maxBatchFrame) {
			size += 4 + len(sqls[end])
			end++
		}
		f, _, err := c.wc.roundTrip(ctx, fBatch, 0, encodeBatchPayload(sqls[start:end]))
		if err != nil {
			var e *Error
			if errors.As(err, &e) && f.typ == fError {
				return all, e, nil
			}
			return all, nil, err
		}
		if f.typ != fBatchResult {
			return all, nil, protoErr("unexpected answer 0x%02x to BATCH", f.typ)
		}
		entries, failed, derr := decodeBatchResult(f.payload)
		if derr != nil {
			return all, nil, derr
		}
		all = append(all, entries...)
		if failed != nil {
			return all, failed, nil
		}
		if len(entries) != end-start {
			return all, nil, protoErr("batch of %d statements answered with %d results and no error", end-start, len(entries))
		}
		start = end
	}
	return all, nil, nil
}

func (br *BatchResults) next() (*batchEntry, error) {
	if br.closed {
		return nil, errors.New("particledb: batch results already closed")
	}
	i := br.idx
	br.idx++
	if br.failAt >= 0 && i >= br.failAt {
		return nil, br.err
	}
	if br.err != nil && br.failAt < 0 {
		return nil, br.err
	}
	if i >= len(br.entries) {
		if br.err != nil {
			return nil, br.err
		}
		return nil, errors.New("particledb: no more results in batch")
	}
	return &br.entries[i], nil
}

// Exec reads the next statement's outcome.
func (br *BatchResults) Exec() (CommandTag, error) {
	e, err := br.next()
	if err != nil {
		return CommandTag{}, err
	}
	if e.isArrow {
		r, err := rowsFromArrow(br.c, e.arrow)
		if err != nil {
			return CommandTag{}, err
		}
		for r.Next() {
		}
		r.Close()
		return r.CommandTag(), r.Err()
	}
	return CommandTag{rows: e.affected}, nil
}

// Query reads the next statement's rows.
func (br *BatchResults) Query() (*Rows, error) {
	e, err := br.next()
	if err != nil {
		return nil, err
	}
	if !e.isArrow {
		return &Rows{row: -1, done: true, ended: true, tag: CommandTag{rows: e.affected}}, nil
	}
	return rowsFromArrow(br.c, e.arrow)
}

// QueryRow reads the next statement's single row.
func (br *BatchResults) QueryRow() *Row {
	r, err := br.Query()
	return &Row{rows: r, err: err}
}

// Close finishes the batch and returns its error, if any.
func (br *BatchResults) Close() error {
	if br.closed {
		return br.err
	}
	br.closed = true
	if br.cleanup != nil {
		br.cleanup(br.err)
	}
	return br.err
}

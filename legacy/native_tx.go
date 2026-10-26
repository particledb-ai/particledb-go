package legacy

// Client.Transaction over the PDB native wire: one pooled connection is
// pinned for BEGIN .. COMMIT/ROLLBACK and every Tx call runs on it.

import (
	"context"
	"errors"
	"fmt"
)

// errTxnRolledBack is returned when the server answered COMMIT by rolling
// back a failed transaction block. It is a server answer (the stream is in
// sync), so it is a *PdbQueryError.
var errTxnRolledBack = &PdbQueryError{Message: "COMMIT did not commit: the server rolled back the transaction because a statement in it had failed"}

// nativeTx is the pinned connection of one native Client.Transaction.
type nativeTx struct {
	c  *PdbWireClient
	pc *pooledConn
	// broken is set when a transport or protocol failure left the stream in
	// an unknown state; the connection is then discarded, never pooled.
	broken bool
	// aborted is set when a statement failed: the server has aborted the
	// transaction, so COMMIT is replaced by ROLLBACK.
	aborted bool
}

func (t *nativeTx) note(err error) error {
	if err == nil {
		return nil
	}
	var pe *PdbParamError
	var be *PdbBatchError
	switch {
	case errors.As(err, &pe):
		// Nothing was sent.
	case errors.As(err, &be), isServerError(err):
		t.aborted = true
	default:
		t.broken = true
	}
	return err
}

func (t *nativeTx) usable() error {
	if t.broken {
		return &PdbConnectionError{Cause: errors.New("transaction connection failed earlier; transaction outcome unknown")}
	}
	return nil
}

func (t *nativeTx) query(sql string, args []interface{}) (*arrowResult, error) {
	if err := t.usable(); err != nil {
		return nil, err
	}
	frames, err := t.pc.run(sql, args, false)
	if err != nil {
		return nil, t.note(err)
	}
	res, err := resultFromFrames(frames)
	return res, t.note(err)
}

func (t *nativeTx) exec(sql string, args []interface{}) (int64, error) {
	if err := t.usable(); err != nil {
		return 0, err
	}
	frames, err := t.pc.run(sql, args, false)
	if err != nil {
		return 0, t.note(err)
	}
	return affectedFromFrames(frames), nil
}

func (t *nativeTx) execBatch(sql string, rows [][]interface{}) (*BatchResult, error) {
	if err := t.usable(); err != nil {
		return nil, err
	}
	res, err := t.pc.execPreparedBatch(sql, rows, true)
	return res, t.note(err)
}

// endTxn sends BEGIN, COMMIT or ROLLBACK as a plain QUERY. A COMMIT the
// server answered by rolling back a failed block (OK carrying
// flagTxnRolledBack) is an error: nothing was committed.
func (t *nativeTx) endTxn(stmt string) error {
	frames, err := t.pc.wc.sendQuery(stmt)
	if err != nil {
		return err
	}
	if stmt == "COMMIT" && len(frames) > 0 && frames[0].Flags&flagTxnRolledBack != 0 {
		return errTxnRolledBack
	}
	return nil
}

func (c *Client) nativeTransaction(fn func(tx *Tx) error) (retErr error) {
	ctx := context.Background()
	pc, err := c.native.acquireWithRetry(ctx)
	if err != nil {
		return fmt.Errorf("particledb: failed to begin transaction: %w", err)
	}
	t := &nativeTx{c: c.native, pc: pc}
	discard := false
	// finished is set on every normal return. If fn panics, neither COMMIT
	// nor ROLLBACK was sent and the connection is still inside the open
	// transaction: it is discarded (closing it makes the server roll back),
	// never pooled where the next caller would join that transaction.
	finished := false
	defer func() {
		c.native.pool.release(pc, discard || t.broken || !finished)
	}()

	if err := t.endTxn("BEGIN"); err != nil {
		discard = true
		return fmt.Errorf("particledb: failed to begin transaction: %w", err)
	}

	txClient := &Client{config: c.config, ntx: t}
	ferr := fn(&Tx{client: txClient})
	finished = true
	if ferr != nil || t.aborted || t.broken {
		if !t.broken {
			if rerr := t.endTxn("ROLLBACK"); rerr != nil {
				discard = true
			}
		}
		if ferr != nil {
			return ferr
		}
		if t.broken {
			return &PdbConnectionError{Cause: errors.New("transaction connection failed; outcome unknown")}
		}
		return errors.New("particledb: transaction rolled back: a statement failed inside it (its error was returned to fn)")
	}
	if err := t.endTxn("COMMIT"); err != nil {
		if !isServerError(err) {
			// The COMMIT may or may not have been applied.
			discard = true
			return fmt.Errorf("particledb: commit outcome unknown: %w", err)
		}
		// Refused COMMIT (e.g. serialization failure): make sure the block
		// is closed before the connection is reused.
		if rerr := t.endTxn("ROLLBACK"); rerr != nil {
			discard = true
		}
		return fmt.Errorf("particledb: failed to commit transaction: %w", err)
	}
	return nil
}

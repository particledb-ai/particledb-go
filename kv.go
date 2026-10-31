package particledb

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// Querier is what Conn, Pool, PoolConn and Tx have in common.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (*Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) *Row
}

var (
	_ Querier = (*Conn)(nil)
	_ Querier = (*Pool)(nil)
	_ Querier = (*Tx)(nil)
	_ Querier = (*PoolConn)(nil)
)

// KVSet, KVGet, KVDel and KVIncr on a Pool use a shared connection (key-value
// frames are not transactional, so any connection serves them).

// KVSet sets key to value.
func (p *Pool) KVSet(ctx context.Context, key, value []byte) error {
	pc, err := p.pick(ctx, false)
	if err != nil {
		return err
	}
	defer pc.c.inflight.Add(-1)
	return pc.c.KVSet(ctx, key, value)
}

// KVGet returns the value of key (nil when absent).
func (p *Pool) KVGet(ctx context.Context, key []byte) ([]byte, error) {
	pc, err := p.pick(ctx, false)
	if err != nil {
		return nil, err
	}
	defer pc.c.inflight.Add(-1)
	return pc.c.KVGet(ctx, key)
}

// KVDel deletes key.
func (p *Pool) KVDel(ctx context.Context, key []byte) (bool, error) {
	pc, err := p.pick(ctx, false)
	if err != nil {
		return false, err
	}
	defer pc.c.inflight.Add(-1)
	return pc.c.KVDel(ctx, key)
}

// KVIncr increments the integer at key.
func (p *Pool) KVIncr(ctx context.Context, key []byte) (int64, error) {
	pc, err := p.pick(ctx, false)
	if err != nil {
		return 0, err
	}
	defer pc.c.inflight.Add(-1)
	return pc.c.KVIncr(ctx, key)
}

// KVExpire sets a key's time to live in seconds (the server's KV_EXPIRE)
// and reports whether the key exists.
func KVExpire(ctx context.Context, q Querier, key string, seconds int64) (bool, error) {
	var v any
	if err := q.QueryRow(ctx, "SELECT KV_EXPIRE($1, $2)", key, seconds).Scan(&v); err != nil {
		return false, err
	}
	return truthy(v), nil
}

// KVTTL returns a key's remaining time to live in seconds: -1 for a key
// without one, -2 for an absent key (as RESP TTL).
func KVTTL(ctx context.Context, q Querier, key string) (int64, error) {
	var n int64
	err := q.QueryRow(ctx, "SELECT KV_TTL($1)", key).Scan(&n)
	return n, err
}

// KVMSet sets several keys at once.
func KVMSet(ctx context.Context, q Querier, kv map[string]string) error {
	if len(kv) == 0 {
		return nil
	}
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	args := make([]any, 0, 2*len(kv))
	ph := make([]string, 0, 2*len(kv))
	for i, k := range keys {
		args = append(args, k, kv[k])
		ph = append(ph, fmt.Sprintf("$%d, $%d", 2*i+1, 2*i+2))
	}
	_, err := q.Exec(ctx, "SELECT KV_MSET("+strings.Join(ph, ", ")+")", args...)
	return err
}

// KVMGet returns the values of keys in order (nil for an absent key).
func KVMGet(ctx context.Context, q Querier, keys ...string) ([]*string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	args := make([]any, len(keys))
	ph := make([]string, len(keys))
	for i, k := range keys {
		args[i] = k
		ph[i] = fmt.Sprintf("$%d", i+1)
	}
	var out []*string
	err := q.QueryRow(ctx, "SELECT KV_MGET("+strings.Join(ph, ", ")+")", args...).Scan(&out)
	return out, err
}

func truthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int64:
		return x != 0
	case string:
		return x == "1" || strings.EqualFold(x, "true") || x == "OK"
	}
	return false
}

// NearestOptions configures NearestNeighbors.
type NearestOptions struct {
	// Columns to return besides the distance (default: every column).
	Columns []string
	// Metric: "l2" (<->, default), "cosine" (<=>) or "ip" (<#>).
	Metric string
	// Where is an optional filter with its own $n placeholders, numbered
	// after the query vector ($1): for example "tenant = $2".
	Where     string
	WhereArgs []any
}

// NearestNeighbors returns the k rows of table nearest to query in column,
// ordered by distance, with the distance as the last column ("distance").
// Identifiers are quoted; the vector is a bound parameter.
func NearestNeighbors(ctx context.Context, q Querier, table Identifier, column string, query Vector, k int, opts *NearestOptions) (*Rows, error) {
	if k <= 0 {
		k = 10
	}
	var o NearestOptions
	if opts != nil {
		o = *opts
	}
	op := "<->"
	switch strings.ToLower(o.Metric) {
	case "", "l2":
	case "cosine":
		op = "<=>"
	case "ip", "inner_product":
		op = "<#>"
	default:
		return nil, clientErr("22023", "unknown vector metric %q (l2 | cosine | ip)", o.Metric)
	}
	cols := "*"
	if len(o.Columns) > 0 {
		qc := make([]string, len(o.Columns))
		for i, c := range o.Columns {
			qc[i] = quoteIdent(c)
		}
		cols = strings.Join(qc, ", ")
	}
	sql := fmt.Sprintf("SELECT %s, %s %s $1::vector AS distance FROM %s", cols, quoteIdent(column), op, table.Sanitize())
	if o.Where != "" {
		sql += " WHERE " + o.Where
	}
	sql += fmt.Sprintf(" ORDER BY distance LIMIT %d", k)
	args := append([]any{query.String()}, o.WhereArgs...)
	return q.Query(ctx, sql, args...)
}

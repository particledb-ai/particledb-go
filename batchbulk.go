package particledb

// A batch whose statements are all the same plain
// INSERT INTO t [(cols)] VALUES ($1, ..., $n) is loaded through native bulk
// ingest in one round trip of DATA frames instead of n parsed statements,
// as the JDBC driver's executeBatch does. The outcome is the same: the
// batch is atomic, every statement reports one row, and a failure fails
// the whole batch (its error carries no statement index, because the load
// is checked as a whole).

import (
	"context"
	"strconv"
	"strings"
	"unicode"
)

type insertShape struct {
	table   Identifier
	columns []string // nil: the table's leading columns
	n       int
}

// parseInsertShape recognises a plain single-tuple INSERT whose every value
// is the next $k placeholder.
func parseInsertShape(sql string) *insertShape {
	lx := &shapeLexer{s: sql}
	if !lx.keyword("insert") || !lx.keyword("into") {
		return nil
	}
	var table Identifier
	for {
		lx.ws()
		id, ok := lx.ident()
		if !ok {
			return nil
		}
		table = append(table, id)
		if lx.peek() != '.' {
			break
		}
		lx.pos++
	}
	if len(table) > 3 {
		return nil
	}
	var cols []string
	lx.ws()
	if lx.peek() == '(' {
		lx.pos++
		for {
			lx.ws()
			c, ok := lx.ident()
			if !ok {
				return nil
			}
			cols = append(cols, c)
			lx.ws()
			ch := lx.peek()
			lx.pos++
			if ch == ')' {
				break
			}
			if ch != ',' {
				return nil
			}
		}
	}
	if !lx.keyword("values") {
		return nil
	}
	lx.ws()
	if lx.peek() != '(' {
		return nil
	}
	lx.pos++
	n := 0
	for {
		lx.ws()
		if lx.peek() != '$' {
			return nil
		}
		lx.pos++
		start := lx.pos
		for lx.pos < len(lx.s) && lx.s[lx.pos] >= '0' && lx.s[lx.pos] <= '9' {
			lx.pos++
		}
		k, err := strconv.Atoi(lx.s[start:lx.pos])
		if err != nil || k != n+1 {
			return nil
		}
		n++
		lx.ws()
		ch := lx.peek()
		lx.pos++
		if ch == ')' {
			break
		}
		if ch != ',' {
			return nil
		}
	}
	lx.ws()
	for lx.peek() == ';' {
		lx.pos++
		lx.ws()
	}
	if lx.pos < len(lx.s) {
		return nil
	}
	if cols != nil {
		if len(cols) != n {
			return nil
		}
		seen := map[string]bool{}
		for _, c := range cols {
			if seen[c] {
				return nil
			}
			seen[c] = true
		}
	}
	return &insertShape{table: table, columns: cols, n: n}
}

type shapeLexer struct {
	s   string
	pos int
}

func (l *shapeLexer) peek() byte {
	if l.pos < len(l.s) {
		return l.s[l.pos]
	}
	return 0
}

func (l *shapeLexer) ws() {
	for l.pos < len(l.s) && (l.s[l.pos] == ' ' || l.s[l.pos] == '\t' || l.s[l.pos] == '\n' || l.s[l.pos] == '\r') {
		l.pos++
	}
}

func (l *shapeLexer) keyword(kw string) bool {
	l.ws()
	end := l.pos + len(kw)
	if end > len(l.s) || !strings.EqualFold(l.s[l.pos:end], kw) {
		return false
	}
	if end < len(l.s) && isIdentChar(l.s[end]) {
		return false
	}
	l.pos = end
	return true
}

func (l *shapeLexer) ident() (string, bool) {
	if l.pos >= len(l.s) {
		return "", false
	}
	if l.s[l.pos] == '"' {
		var b strings.Builder
		for i := l.pos + 1; i < len(l.s); i++ {
			if l.s[i] == '"' {
				if i+1 < len(l.s) && l.s[i+1] == '"' {
					b.WriteByte('"')
					i++
					continue
				}
				if b.Len() == 0 {
					return "", false
				}
				l.pos = i + 1
				return b.String(), true
			}
			b.WriteByte(l.s[i])
		}
		return "", false
	}
	c := rune(l.s[l.pos])
	if !(unicode.IsLetter(c) || c == '_') {
		return "", false
	}
	start := l.pos
	for l.pos < len(l.s) && (isIdentChar(l.s[l.pos]) || l.s[l.pos] == '$') {
		l.pos++
	}
	id := strings.ToLower(l.s[start:l.pos])
	switch id {
	case "values", "select", "default", "on", "returning", "overriding", "as", "with":
		return "", false
	}
	return id, true
}

// bulkBatchMin is the batch size from which an INSERT-shaped batch is bulk
// loaded.
const bulkBatchMin = 64

// bulkShaped reports whether b would take the bulk path (cheap check).
func (b *Batch) bulkShaped() bool {
	if len(b.items) < bulkBatchMin {
		return false
	}
	sql := b.items[0].sql
	for _, it := range b.items[1:] {
		if it.sql != sql {
			return false
		}
	}
	return parseInsertShape(sql) != nil
}

// tryBulkBatch loads b through bulk ingest when its shape allows. ok is
// false when the batch must take the statement path (nothing was sent).
func (c *Conn) tryBulkBatch(ctx context.Context, b *Batch) (br *BatchResults, ok bool) {
	if len(b.items) < bulkBatchMin || !c.wc.hello.bulkIngest() {
		return nil, false
	}
	sql := b.items[0].sql
	for _, it := range b.items[1:] {
		if it.sql != sql {
			return nil, false
		}
	}
	shape := parseInsertShape(sql)
	if shape == nil {
		return nil, false
	}
	for _, it := range b.items {
		if len(it.args) != shape.n {
			return nil, false
		}
		for _, a := range it.args {
			if _, isArr := a.(arrayArg); isArr {
				return nil, false
			}
		}
	}
	cols := shape.columns
	if cols == nil {
		all, err := c.tableColumns(ctx, shape.table)
		if err != nil || len(all) < shape.n {
			return nil, false
		}
		cols = all[:shape.n]
	}
	i := 0
	src := CopyFromFunc(func() ([]any, error) {
		if i >= len(b.items) {
			return nil, nil
		}
		i++
		return b.items[i-1].args, nil
	})
	n, err := c.CopyFrom(ctx, shape.table, cols, src)
	if err != nil {
		if SQLState(err) == "0A000" {
			return nil, false // a target bulk ingest refuses (triggers, RLS): statement path
		}
		return &BatchResults{c: c, err: err, failAt: 0}, true
	}
	if n != int64(len(b.items)) {
		return &BatchResults{c: c, err: protoErr("bulk ingest loaded %d rows of %d", n, len(b.items)), failAt: 0}, true
	}
	entries := make([]batchEntry, len(b.items))
	for k := range entries {
		entries[k].affected = 1
	}
	return &BatchResults{c: c, entries: entries, failAt: -1}, true
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	particledb "github.com/particledb-ai/particledb-go"
)

var bg = context.Background()

// session is one SDK connection the script interpreter drives.
type session struct {
	c   *particledb.Conn
	tx  *particledb.Tx
	iso particledb.TxIsoLevel
}

func openSession() *session {
	c, err := particledb.Connect(bg, dsn())
	must(err, "connect")
	return &session{c: c}
}

type param struct {
	typ   string
	value any // string, []any (arrays), or nil
}

func toGo(typ string, v any) any {
	if v == nil {
		return nil
	}
	if strings.HasSuffix(typ, "[]") {
		et := strings.TrimSuffix(typ, "[]")
		vs := v.([]any)
		switch et {
		case "int2", "int4", "int8":
			out := make([]*int64, len(vs))
			for i, e := range vs {
				if e != nil {
					n, err := strconv.ParseInt(e.(string), 10, 64)
					must(err, "array param")
					out[i] = &n
				}
			}
			if et == "int4" {
				o := make([]*int32, len(out))
				for i, p := range out {
					if p != nil {
						x := int32(*p)
						o[i] = &x
					}
				}
				return o
			}
			return out
		default:
			out := make([]*string, len(vs))
			for i, e := range vs {
				if e != nil {
					s := e.(string)
					out[i] = &s
				}
			}
			return out
		}
	}
	s := v.(string)
	var err error
	var out any
	switch typ {
	case "int2":
		var n int64
		n, err = strconv.ParseInt(s, 10, 16)
		out = int16(n)
	case "int4":
		var n int64
		n, err = strconv.ParseInt(s, 10, 32)
		out = int32(n)
	case "int8":
		out, err = strconv.ParseInt(s, 10, 64)
	case "float4":
		var f float64
		f, err = strconv.ParseFloat(s, 32)
		out = float32(f)
	case "float8":
		out, err = strconv.ParseFloat(s, 64)
	case "numeric":
		out, err = particledb.ParseDecimal(s)
	case "bool":
		out, err = strconv.ParseBool(s)
	case "bytea":
		out = unhex(s)
	case "date":
		var t time.Time
		t, err = time.Parse("2006-01-02", s)
		out = particledb.Date{Time: t}
	case "timestamp":
		var t time.Time
		t, err = time.Parse("2006-01-02 15:04:05.999999", s)
		out = particledb.Timestamp{Time: t}
	case "timestamptz":
		out, err = parseTz(s)
	case "uuid":
		out, err = particledb.ParseUUID(s)
	case "json", "jsonb":
		out = json.RawMessage(s)
	default:
		out = s
	}
	must(err, "param "+typ+" "+s)
	return out
}

func parseTz(s string) (time.Time, error) {
	for _, l := range []string{"2006-01-02 15:04:05.999999Z07", "2006-01-02 15:04:05.999999Z07:00"} {
		if t, err := time.Parse(l, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("bad timestamptz %q", s)
}

func goArgs(ps []param) []any {
	out := make([]any, len(ps))
	for i, p := range ps {
		out[i] = toGo(p.typ, p.value)
	}
	return out
}

func (s *session) exec(sql string, ps []param) error {
	if s.tx != nil {
		_, err := s.tx.Exec(bg, sql, goArgs(ps)...)
		return err
	}
	_, err := s.c.Exec(bg, sql, goArgs(ps)...)
	return err
}

func (s *session) query(sql string, ps []param, types []string) ([][]any, error) {
	var rows *particledb.Rows
	var err error
	if s.tx != nil {
		rows, err = s.tx.Query(bg, sql, goArgs(ps)...)
	} else {
		rows, err = s.c.Query(bg, sql, goArgs(ps)...)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]any
	for rows.Next() {
		row, err := canonRow(rows, types)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// canonRow reads each cell with the typed destination an application would
// use and renders it canonically.
func canonRow(rows *particledb.Rows, types []string) ([]any, error) {
	dest := make([]any, len(types))
	for i, t := range types {
		switch {
		case strings.HasSuffix(t, "[]"):
			dest[i] = new([]*string)
		case t == "int2" || t == "int4" || t == "int8":
			dest[i] = new(*int64)
		case t == "float4":
			dest[i] = new(*float32)
		case t == "float8":
			dest[i] = new(*float64)
		case t == "numeric":
			dest[i] = new(*particledb.Decimal)
		case t == "bool":
			dest[i] = new(*bool)
		case t == "bytea":
			dest[i] = new([]byte)
		case t == "date" || t == "timestamp" || t == "timestamptz":
			dest[i] = new(*time.Time)
		case t == "uuid":
			dest[i] = new(*particledb.UUID)
		case t == "vector":
			dest[i] = new(particledb.Vector)
		default:
			dest[i] = new(*string)
		}
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, err
	}
	out := make([]any, len(types))
	for i, t := range types {
		switch d := dest[i].(type) {
		case *[]*string:
			if *d == nil {
				out[i] = nil
				continue
			}
			l := make([]any, len(*d))
			for j, e := range *d {
				if e != nil {
					l[j] = *e
				}
			}
			out[i] = l
		case **int64:
			if *d != nil {
				out[i] = strconv.FormatInt(**d, 10)
			}
		case **float32:
			if *d != nil {
				out[i] = canonFloat(float64(**d), 32)
			}
		case **float64:
			if *d != nil {
				out[i] = canonFloat(**d, 64)
			}
		case **particledb.Decimal:
			if *d != nil {
				out[i] = (*d).String()
			}
		case **bool:
			if *d != nil {
				out[i] = strconv.FormatBool(**d)
			}
		case *[]byte:
			if *d != nil {
				out[i] = hexBytes(*d)
			}
		case **time.Time:
			if *d != nil {
				tm := **d
				switch t {
				case "date":
					out[i] = tm.Format("2006-01-02")
				case "timestamp":
					out[i] = canonTimestamp(tm)
				default:
					out[i] = canonTimestamp(tm.UTC()) + "+00"
				}
			}
		case **particledb.UUID:
			if *d != nil {
				out[i] = (*d).String()
			}
		case *particledb.Vector:
			if *d != nil {
				l := make([]any, len(*d))
				for j, f := range *d {
					l[j] = canonFloat(float64(f), 32)
				}
				out[i] = l
			}
		case **string:
			if *d != nil {
				out[i] = **d
			}
		}
	}
	return out, nil
}

func (s *session) begin() error {
	tx, err := s.c.BeginTx(bg, particledb.TxOptions{IsoLevel: s.iso})
	s.tx = tx
	return err
}

func (s *session) commit() error {
	if s.tx == nil {
		return fmt.Errorf("commit outside a transaction")
	}
	tx := s.tx
	s.tx = nil
	return tx.Commit(bg)
}

func (s *session) rollback() error {
	if s.tx == nil {
		return nil
	}
	tx := s.tx
	s.tx = nil
	return tx.Rollback(bg)
}

func (s *session) isolation(level string) error {
	switch level {
	case "read committed":
		s.iso = particledb.ReadCommitted
	case "repeatable read":
		s.iso = particledb.RepeatableRead
	case "serializable":
		s.iso = particledb.Serializable
	default:
		return fmt.Errorf("unknown level %q", level)
	}
	return nil
}

func (s *session) savepoint(name string) error {
	if s.tx == nil {
		return fmt.Errorf("savepoint outside a transaction")
	}
	return s.tx.Savepoint(bg, name)
}

func (s *session) rollbackTo(name string) error {
	if s.tx == nil {
		return fmt.Errorf("rollback to savepoint outside a transaction")
	}
	return s.tx.RollbackTo(bg, name)
}

func (s *session) close() {
	if s.tx != nil {
		s.tx.Rollback(bg)
	}
	s.c.Close()
}

func params(raw any) []param {
	l, _ := raw.([]any)
	var out []param
	for _, o := range l {
		m := o.(map[string]any)
		var v any = m["value"]
		if g, ok := m["gen"]; ok {
			v = genValue(g.(map[string]any))
		}
		out = append(out, param{typ: m["type"].(string), value: v})
	}
	return out
}

func abbreviate(s string) string {
	if len(s) > 60 {
		return s[:57] + "..."
	}
	return s
}

func runScript(c caseDef) {
	sessions := map[string]*session{}
	skipNext := map[string]bool{}
	defer func() {
		for _, s := range sessions {
			s.close()
		}
	}()
	steps := c["steps"].([]any)
	for n, o := range steps {
		st := o.(map[string]any)
		conn, _ := st["conn"].(string)
		if conn == "" {
			conn = "a"
		}
		s := sessions[conn]
		if s == nil {
			s = openSession()
			sessions[conn] = s
		}
		op := st["op"].(string)
		sql, _ := st["sql"].(string)
		ps := params(st["params"])
		where := fmt.Sprintf("step %d (%s", n+1, op)
		if sql != "" {
			where += " " + abbreviate(sql)
		}
		where += ")"
		var err error
		switch op {
		case "exec":
			err = s.exec(sql, ps)
		case "query":
			var types []string
			for _, t := range st["types"].([]any) {
				types = append(types, t.(string))
			}
			var got [][]any
			got, err = s.query(sql, ps, types)
			if err == nil {
				want, _ := st["expect_rows"].([]any)
				compareRows(types, want, got, where)
			}
		case "error":
			want := st["sqlstate"].(string)
			e := s.exec(sql, ps)
			if e == nil {
				fail("%s: expected SQLSTATE %s, statement succeeded", where, want)
			}
			eq(want, particledb.SQLState(e), where+" SQLSTATE ("+e.Error()+")")
		case "begin":
			err = s.begin()
		case "commit":
			err = s.commit()
		case "rollback":
			err = s.rollback()
		case "isolation":
			err = s.isolation(st["level"].(string))
		case "savepoint":
			err = s.savepoint(st["name"].(string))
		case "rollback_to":
			err = s.rollbackTo(st["name"].(string))
		case "commit_error":
			want := st["sqlstate"].(string)
			if skipNext[conn] {
				delete(skipNext, conn)
				break
			}
			e := s.commit()
			if e == nil {
				fail("%s: expected the commit to fail %s, it succeeded", where, want)
			}
			eq(want, particledb.SQLState(e), where+" SQLSTATE ("+e.Error()+")")
		default:
			fail("unknown script op %s", op)
		}
		if err != nil {
			stt := particledb.SQLState(err)
			if next := nextFor(steps, n+1, conn); next != nil && next["op"] == "commit_error" && stt != "" && stt == next["sqlstate"] {
				s.rollback()
				skipNext[conn] = true
				continue
			}
			suffix := ""
			if stt != "" {
				suffix = " [SQLSTATE " + stt + "]"
			}
			fail("%s: %v%s", where, err, suffix)
		}
	}
}

func nextFor(steps []any, from int, conn string) map[string]any {
	for i := from; i < len(steps); i++ {
		m := steps[i].(map[string]any)
		c, _ := m["conn"].(string)
		if c == "" {
			c = "a"
		}
		if c == conn {
			return m
		}
	}
	return nil
}

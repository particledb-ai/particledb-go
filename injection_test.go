package particledb

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// Injection payloads: every one must stay inside the single literal its
// placeholder becomes, on every path that renders parameters as SQL text
// (the BATCH frame carries statements as text, so a batched statement's
// arguments are rendered by renderSQL/appendLiteral).
var injectionStrings = []string{
	`'); DROP TABLE t; --`,
	`x' OR 1=1 --`,
	`\'); DROP TABLE t; --`,
	`\\'; SELECT 1; --`,
	`it''s'''`,
	`$1 $2 $$ ; -- /* */`,
	"héllo ☃ 𝄞 '; DROP TABLE t; --",
}

// literalEnd lexes a standard-conforming SQL literal starting with a quote
// (a doubled quote is the only escape) and returns the index just past its closing quote, or
// -1 when the literal never closes.
func literalEnd(s string) int {
	if s == "" || s[0] != '\'' {
		return -1
	}
	for i := 1; i < len(s); i++ {
		if s[i] == '\'' {
			if i+1 < len(s) && s[i+1] == '\'' {
				i++
				continue
			}
			return i + 1
		}
	}
	return -1
}

func TestRenderSQLInjectionPayloadsStayInOneLiteral(t *testing.T) {
	const pre, post = "SELECT ", " AS v, 1 AS canary"
	var vals []any
	for _, s := range injectionStrings {
		vals = append(vals, s, json.RawMessage(`{"k":"`+strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`)+`"}`))
	}
	vals = append(vals,
		[]string{`a'b`, `c"d`, `e\f`, `'); DROP TABLE t; --`, "☃'"},
		[][]string{{`'); DROP TABLE t; --`, `"`}, {`\`, `}{,'`}},
		[]byte(`'); DROP TABLE t; --`),
	)
	for _, v := range vals {
		p, err := classify(v, 0)
		if err != nil {
			t.Fatalf("%#v: %v", v, err)
		}
		fixSpecials(&p)
		got, err := renderSQL(pre+"$1"+post, []pval{p})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(got, pre) || !strings.HasSuffix(got, post) {
			t.Fatalf("%#v: %s", v, got)
		}
		mid := got[len(pre) : len(got)-len(post)]
		end := literalEnd(mid)
		if end < 0 {
			t.Fatalf("%#v: literal never closes: %s", v, got)
		}
		rest := mid[end:]
		if rest != "" && !(strings.HasPrefix(rest, "::") && strings.HasSuffix(rest, "[]") && !strings.ContainsAny(rest, "' ;-")) {
			t.Fatalf("%#v: SQL after the literal: %q in %s", v, rest, got)
		}
		var kinds []int
		if err := scanSQL(got, func(k int, _ string) { kinds = append(kinds, k) }); err != nil {
			t.Fatal(err)
		}
		for _, k := range kinds {
			if k != tokText {
				t.Fatalf("%#v: rendered SQL has a placeholder or statement break: %s", v, got)
			}
		}
	}
}

// TestLiveBatchInjectionStoredAsData sends hostile values through SendBatch
// (the text-rendered path) and checks they come back byte-identical, the
// canary table survives and no extra statement ran.
func TestLiveBatchInjectionStoredAsData(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	tbl := tableName(t)
	canary := tbl + "_canary"
	mustExec(t, c, "DROP TABLE IF EXISTS "+tbl)
	mustExec(t, c, "DROP TABLE IF EXISTS "+canary)
	mustExec(t, c, "CREATE TABLE "+canary+" (id int8 PRIMARY KEY)")
	mustExec(t, c, "INSERT INTO "+canary+" VALUES (1)")
	mustExec(t, c, "CREATE TABLE "+tbl+" (id int8 PRIMARY KEY, s text, j jsonb, a text[], b bytea)")
	b := &Batch{}
	type row struct {
		s string
		j string
		a []string
	}
	var want []row
	payloads := append(append([]string{}, injectionStrings...), `'); DROP TABLE `+canary+`; --`, `\'); DROP TABLE `+canary+`; --`)
	for i, s := range payloads {
		js, _ := json.Marshal(map[string]string{"k": s})
		r := row{s, string(js), []string{s, `"` + s + `"`, `\` + s}}
		want = append(want, r)
		b.Queue("INSERT INTO "+tbl+" VALUES ($1, $2, $3, $4, $5)", i, s, json.RawMessage(js), r.a, []byte(s))
	}
	br := c.SendBatch(ctx, b)
	if err := br.Close(); err != nil {
		t.Fatal(err)
	}
	for i, w := range want {
		var s string
		var js json.RawMessage
		var a []string
		var bs []byte
		if err := c.QueryRow(ctx, "SELECT s, j, a, b FROM "+tbl+" WHERE id = $1", i).Scan(&s, &js, &a, &bs); err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		var gotJ, wantJ any
		_ = json.Unmarshal(js, &gotJ)
		_ = json.Unmarshal([]byte(w.j), &wantJ)
		if s != w.s || string(bs) != w.s || !reflect.DeepEqual(a, w.a) || !reflect.DeepEqual(gotJ, wantJ) {
			t.Fatalf("row %d: got %q %s %q %q, want %q %s %q", i, s, js, a, bs, w.s, w.j, w.a)
		}
	}
	var n int64
	if err := c.QueryRow(ctx, "SELECT count(*) FROM "+tbl).Scan(&n); err != nil || n != int64(len(want)) {
		t.Fatalf("rows %d %v, want %d", n, err, len(want))
	}
	if err := c.QueryRow(ctx, "SELECT count(*) FROM "+canary).Scan(&n); err != nil || n != 1 {
		t.Fatalf("canary %d %v", n, err)
	}
}

// Command adapter runs the ParticleDB SDK conformance suite (../SPEC.md)
// against the Go SDK built from this module. It is started by
// ../run_conformance.sh, which sets the PDB_CONF_* environment.
package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	osexec "os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	particledb "github.com/particledb-ai/particledb-go"
)

// ── outcome signals ────────────────────────────────────────────────────────

type skipErr struct{ reason string }

func (s skipErr) Error() string { return s.reason }

type failErr struct{ detail string }

func (f failErr) Error() string { return f.detail }

func skip(reason string) { panic(skipErr{reason}) }
func fail(format string, args ...any) {
	panic(failErr{fmt.Sprintf(format, args...)})
}
func check(ok bool, format string, args ...any) {
	if !ok {
		fail(format, args...)
	}
}
func eq(want, got any, what string) {
	if fmt.Sprint(want) != fmt.Sprint(got) {
		fail("%s: expected %s, got %s", what, show(want), show(got))
	}
}

// must turns an unexpected SDK error into a case failure.
func must(err error, what string) {
	if err != nil {
		st := particledb.SQLState(err)
		if st != "" {
			fail("%s: %v [SQLSTATE %s]", what, err, st)
		}
		fail("%s: %v", what, err)
	}
}

func show(v any) string {
	if v == nil {
		return "null"
	}
	s := fmt.Sprint(v)
	if str, ok := v.(string); ok {
		s = "'" + str + "'"
	}
	if len(s) > 200 {
		return s[:200] + fmt.Sprintf("...(%d chars)", len(s))
	}
	return s
}

// ── env ────────────────────────────────────────────────────────────────────

func env(k string) string {
	v := os.Getenv(k)
	if v == "" {
		fmt.Fprintf(os.Stderr, "%s is not set (run via run_conformance.sh)\n", k)
		os.Exit(2)
	}
	return v
}

func host() string {
	if h := os.Getenv("PDB_CONF_HOST"); h != "" {
		return h
	}
	return "127.0.0.1"
}

func portConfigured() bool { return os.Getenv("PDB_CONF_PORT") != "" }
func defaultPort() int     { n, _ := strconv.Atoi(env("PDB_CONF_DEFAULT_PORT")); return n }
func port() int {
	if portConfigured() {
		n, _ := strconv.Atoi(os.Getenv("PDB_CONF_PORT"))
		return n
	}
	return defaultPort()
}
func respPort() int { n, _ := strconv.Atoi(env("PDB_CONF_RESP_PORT")); return n }

// dsn is the documented zero-option connection string.
func dsn() string {
	if portConfigured() {
		return fmt.Sprintf("particledb://%s:%d", host(), port())
	}
	return "particledb://" + host()
}

func control(action string) {
	hook := os.Getenv("PDB_CONF_CONTROL")
	if hook == "" {
		skip("no server control hook (PDB_CONF_CONTROL): the runner did not start this server, so it cannot kill and restart it; set PDB_BIN to run this case")
	}
	out, err := osexec.Command(hook, action).CombinedOutput()
	if err != nil {
		panic(fmt.Errorf("control %s failed: %v: %s", action, err, out))
	}
}

// ── cases ──────────────────────────────────────────────────────────────────

type caseDef map[string]any

func loadCases(dir string) map[string]caseDef {
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	sort.Strings(files)
	all := map[string]caseDef{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			panic(err)
		}
		var doc struct {
			Cases []caseDef `json:"cases"`
		}
		if err := json.Unmarshal(b, &doc); err != nil {
			panic(fmt.Errorf("%s: %v", f, err))
		}
		for _, c := range doc.Cases {
			all[c["id"].(string)] = c
		}
	}
	return all
}

type result struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
	Detail string `json:"detail,omitempty"`
	Ms     int64  `json:"ms"`
}

func runOne(fn func()) (res result) {
	done := make(chan result, 1)
	go func() {
		r := result{Status: "pass"}
		defer func() {
			if p := recover(); p != nil {
				switch e := p.(type) {
				case skipErr:
					r = result{Status: "skip", Reason: e.reason}
				case failErr:
					r = result{Status: "fail", Detail: e.detail}
				case error:
					d := fmt.Sprintf("%T: %v", e, e)
					if st := particledb.SQLState(e); st != "" {
						d += " [SQLSTATE " + st + "]"
					}
					r = result{Status: "fail", Detail: d}
				default:
					r = result{Status: "fail", Detail: fmt.Sprint(p)}
				}
			}
			done <- r
		}()
		fn()
	}()
	select {
	case r := <-done:
		return r
	case <-time.After(180 * time.Second):
		return result{Status: "fail", Detail: "case did not finish within 180 s"}
	}
}

func main() {
	cases := loadCases(env("PDB_CONF_CASES_DIR"))
	idsRaw, err := os.ReadFile(env("PDB_CONF_IDS"))
	if err != nil {
		panic(err)
	}
	only := map[string]bool{}
	if o := os.Getenv("PDB_CONF_ONLY"); o != "" {
		for _, id := range strings.Split(o, ",") {
			only[id] = true
		}
	}
	customs := customCases(cases)
	results := map[string]result{}
	pass, nfail, nskip := 0, 0, 0
	for _, line := range strings.Split(string(idsRaw), "\n") {
		id := strings.TrimSpace(line)
		if id == "" {
			continue
		}
		c, ok := cases[id]
		var r result
		switch {
		case !ok:
			r = result{Status: "fail", Detail: "the runner listed a case id the case files do not define"}
		case len(only) > 0 && !only[id]:
			r = result{Status: "skip", Reason: "PDB_CONF_ONLY excluded it (debug run)"}
		default:
			t0 := time.Now()
			if c["kind"] == "script" {
				r = runOne(func() { runScript(c) })
			} else if fn, ok := customs[id]; ok {
				r = runOne(fn)
			} else {
				r = result{Status: "fail", Detail: "adapter has no implementation of this custom case"}
			}
			r.Ms = time.Since(t0).Milliseconds()
		}
		switch r.Status {
		case "pass":
			pass++
		case "fail":
			nfail++
		default:
			nskip++
		}
		fmt.Printf("%-16s %-4s %s%s\n", id, r.Status, r.Detail, r.Reason)
		results[id] = r
	}
	doc := map[string]any{"sdk": "particledb-go", "sdk_version": "source", "mode": env("PDB_CONF_MODE"), "results": results}
	b, _ := json.MarshalIndent(doc, "", " ")
	if err := os.WriteFile(env("PDB_CONF_RESULTS"), append(b, '\n'), 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("particledb-go: %d pass, %d fail, %d skip\n", pass, nfail, nskip)
	if nfail > 0 {
		os.Exit(1)
	}
}

// ── canonical forms (SPEC.md section 4) ────────────────────────────────────

func canonFloat(d float64, bits int) string {
	switch {
	case math.IsNaN(d):
		return "NaN"
	case math.IsInf(d, 1):
		return "Infinity"
	case math.IsInf(d, -1):
		return "-Infinity"
	}
	return strconv.FormatFloat(d, 'f', -1, bits)
}

func hexBytes(b []byte) string {
	const h = "0123456789abcdef"
	out := make([]byte, 2, 2+2*len(b))
	out[0], out[1] = '\\', 'x'
	for _, c := range b {
		out = append(out, h[c>>4], h[c&15])
	}
	return string(out)
}

func unhex(s string) []byte {
	if !strings.HasPrefix(s, `\x`) {
		panic(fmt.Errorf("bytea canonical form is \\x<hex>: %s", s))
	}
	b := make([]byte, (len(s)-2)/2)
	for i := range b {
		v, err := strconv.ParseUint(s[2+2*i:4+2*i], 16, 8)
		if err != nil {
			panic(err)
		}
		b[i] = byte(v)
	}
	return b
}

func canonTimestamp(t time.Time) string {
	return t.Format("2006-01-02 15:04:05.999999")
}

func genValue(g map[string]any) string {
	count := int(g["count"].(float64))
	if r, ok := g["repeat"]; ok {
		return strings.Repeat(r.(string), count)
	}
	var b strings.Builder
	b.WriteString(`\x`)
	for k := 0; k < count; k++ {
		for i := 0; i < 256; i++ {
			fmt.Fprintf(&b, "%02x", i)
		}
	}
	return b.String()
}

func cellEquals(typ string, want, got any) bool {
	if want == nil || got == nil {
		return want == nil && got == nil
	}
	if strings.HasSuffix(typ, "[]") || typ == "vector" {
		w, ok1 := want.([]any)
		g, ok2 := got.([]any)
		if !ok1 || !ok2 || len(w) != len(g) {
			return false
		}
		et := "float4"
		if typ != "vector" {
			et = strings.TrimSuffix(typ, "[]")
		}
		for i := range w {
			if !cellEquals(et, w[i], g[i]) {
				return false
			}
		}
		return true
	}
	ws, gs := fmt.Sprint(want), fmt.Sprint(got)
	switch typ {
	case "float4":
		a, e1 := strconv.ParseFloat(ws, 32)
		b, e2 := strconv.ParseFloat(gs, 32)
		return e1 == nil && e2 == nil && (float32(a) == float32(b) || (math.IsNaN(a) && math.IsNaN(b)))
	case "float8":
		a, e1 := strconv.ParseFloat(ws, 64)
		b, e2 := strconv.ParseFloat(gs, 64)
		return e1 == nil && e2 == nil && (a == b || (math.IsNaN(a) && math.IsNaN(b)))
	}
	return ws == gs
}

func compareRows(types []string, want []any, got [][]any, where string) {
	if len(want) != len(got) {
		fail("%s: expected %d rows, got %d %s", where, len(want), len(got), show(got))
	}
	for r := range want {
		wr := want[r].([]any)
		gr := got[r]
		if len(wr) != len(gr) {
			fail("%s: row %d width %d != %d", where, r, len(gr), len(wr))
		}
		for c := range wr {
			w := wr[c]
			if m, ok := w.(map[string]any); ok {
				w = genValue(m["gen"].(map[string]any))
			}
			if !cellEquals(types[c], w, gr[c]) {
				fail("%s: row %d col %d (%s): expected %s, got %s", where, r, c+1, types[c], show(w), show(gr[c]))
			}
		}
	}
}

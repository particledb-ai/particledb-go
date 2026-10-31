package particledb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// Golden spellings shared with the server's native_literal tests: the
// literal a batched statement carries must be byte-identical to what the
// server renders for the same typed parameter.
func TestJavaDoubleSpelling(t *testing.T) {
	for _, c := range []struct {
		d    float64
		want string
	}{
		{1.5, "1.5"}, {1.0, "1.0"}, {100.0, "100.0"}, {1e7, "1.0E7"}, {9999999.0, "9999999.0"},
		{0.001, "0.001"}, {1e-4, "1.0E-4"}, {1e10, "1.0E10"}, {0.1, "0.1"}, {1.0 / 3.0, "0.3333333333333333"},
		{9.223372036854776e18, "9.223372036854776E18"}, {2.2250738585072014e-308, "2.2250738585072014E-308"},
		{4.9e-324, "4.9E-324"}, {math.MaxFloat64, "1.7976931348623157E308"}, {123456.789, "123456.789"},
		{-2.5, "-2.5"}, {0.0, "0.0"}, {math.Copysign(0, -1), "-0.0"},
	} {
		if got := javaDoubleText(c.d); got != c.want {
			t.Errorf("%g: %q, want %q", c.d, got, c.want)
		}
	}
	for _, c := range []struct {
		f    float32
		want string
	}{{1.1, "1.1"}, {3.4028235e38, "3.4028235E38"}, {1e-5, "1.0E-5"}, {0.25, "0.25"}} {
		if got := javaFloatText(c.f); got != c.want {
			t.Errorf("%g: %q, want %q", c.f, got, c.want)
		}
	}
}

func lit(t *testing.T, v any) string {
	t.Helper()
	p, err := classify(v, 0)
	if err != nil {
		t.Fatal(err)
	}
	fixSpecials(&p)
	return string(appendLiteral(nil, &p))
}

func TestLiteralSpelling(t *testing.T) {
	cases := []struct {
		v    any
		want string
	}{
		{-1.5, "(-1.5)"}, {math.Copysign(0, -1), "(-0.0)"}, {math.NaN(), "'NaN'::float8"},
		{math.Inf(-1), "'-Infinity'::float8"}, {float32(math.Inf(1)), "'Infinity'::float8"},
		{[]byte{1, 0xab}, `'\x01ab'`}, {[]byte{}, `'\x'`}, {json.RawMessage(`{"a":"it's"}`), `'{"a":"it''s"}'`},
		{NewDecimal(12345, 2), "123.45"}, {NewDecimal(-123, 4), "(-0.0123)"}, {NewDecimal(0, 3), "0.000"},
		{MustParseDecimal("1e3"), "1000"}, {int64(-5), "(-5)"}, {"it's", "'it''s'"}, {nil, "NULL"},
		{true, "TRUE"}, {UUID{0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38, 0x0a, 0x11}, "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"},
		{Date{time.Date(-43, 3, 15, 0, 0, 0, 0, time.UTC)}, "'0044-03-15 BC'"},
		{Date{time.Date(12024, 3, 15, 0, 0, 0, 0, time.UTC)}, "'12024-03-15'"},
		{Timestamp{time.Unix(0, 0).UTC()}, "'1970-01-01 00:00:00.0'"},
		{Timestamp{time.UnixMicro(-1).UTC()}, "'1969-12-31 23:59:59.999999'"},
		{Timestamp{time.UnixMicro(1704112496789000).UTC()}, "'2024-01-01 12:34:56.789'"},
		{time.UnixMicro(1704112496789000).UTC(), "'2024-01-01T12:34:56.789Z'"},
		{time.UnixMicro(1704112440000000).In(time.FixedZone("", 7200)), "'2024-01-01T14:34+02:00'"},
		{time.UnixMicro(1704112440000001).In(time.FixedZone("", -18000)), "'2024-01-01T07:34:00.000001-05:00'"},
		{time.UnixMicro(1704112440000000).In(time.FixedZone("", 19815)), "'2024-01-01T18:04:15+05:30:15'"},
		{[]int64{1, -2}, "'{1,-2}'::int8[]"}, {[]int32{1}, "'{1}'::int4[]"}, {[]int16{1}, "'{1}'::int2[]"},
		{[]*string{ptr("a"), nil, ptr(`q"\`)}, `'{"a",NULL,"q\"\\"}'::text[]`},
		{[][]int64{{1, 2}, {3, 4}}, "'{{1,2},{3,4}}'::int8[]"}, {[]string{}, "'{}'::text[]"},
		{[]float64{math.NaN(), 1.5}, "'{NaN,1.5}'::float8[]"}, {[]bool{true, false}, "'{true,false}'::bool[]"},
		{[]string{"it's"}, `'{"it''s"}'::text[]`},
		{DecimalNaN(), "'NaN'"},
	}
	for _, c := range cases {
		if got := lit(t, c.v); got != c.want {
			t.Errorf("%#v: %s, want %s", c.v, got, c.want)
		}
	}
}

func TestRenderSQLSkipsQuotedText(t *testing.T) {
	ps := []pval{{tag: tagInt8, i: 7}, {tag: tagText, s: "x"}}
	got, err := renderSQL(`SELECT $1, '$1', "$2", $$ $1 $$, $tag$ $2 $tag$, -- $1
	/* $2 /* nested $1 */ */ E'\' $1', $2, a$1`, ps)
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT 7, '$1', "$2", $$ $1 $$, $tag$ $2 $tag$, -- $1
	/* $2 /* nested $1 */ */ E'\' $1', 'x', a$1`
	if got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
}

func TestSplitStatements(t *testing.T) {
	got := splitStatements(`INSERT INTO t VALUES (';'); -- c;
	SELECT $$a;b$$ ; /* x; */ ; SELECT "a;b" FROM t;;`)
	want := []string{"INSERT INTO t VALUES (';')", "-- c;\n\tSELECT $$a;b$$", `SELECT "a;b" FROM t`}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%q", got)
	}
	if !isTxnControl("  begin isolation level serializable") || isTxnControl("SELECT 1") || !isTxnControl("SET TRANSACTION READ ONLY") {
		t.Fatal("isTxnControl")
	}
}

func TestInsertShape(t *testing.T) {
	ok := map[string]*insertShape{
		"INSERT INTO t VALUES ($1, $2)":               {table: Identifier{"t"}, n: 2},
		`insert into "My T" (a, "B") values ($1,$2);`: {table: Identifier{"My T"}, columns: []string{"a", "B"}, n: 2},
		"INSERT INTO s.t (x) VALUES ( $1 )":           {table: Identifier{"s", "t"}, columns: []string{"x"}, n: 1},
	}
	for sql, want := range ok {
		got := parseInsertShape(sql)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: %+v", sql, got)
		}
	}
	for _, sql := range []string{
		"INSERT INTO t VALUES ($1, $1)", "INSERT INTO t VALUES ($2, $1)", "INSERT INTO t VALUES ($1, 5)",
		"INSERT INTO t VALUES ($1) RETURNING id", "INSERT INTO t VALUES ($1) ON CONFLICT DO NOTHING",
		"INSERT INTO t (a, a) VALUES ($1, $2)", "INSERT INTO t (a) VALUES ($1, $2)", "INSERT INTO t SELECT $1",
		"INSERT INTO t VALUES ($1), ($2)", "UPDATE t SET a = $1",
	} {
		if parseInsertShape(sql) != nil {
			t.Errorf("%s: accepted", sql)
		}
	}
}

func TestDecimal(t *testing.T) {
	for _, c := range []struct{ in, out string }{
		{"1.50", "1.50"}, {"-0.0001", "-0.0001"}, {"1e3", "1000"}, {"1.5e-3", "0.0015"},
		{"123456789012345678901234567890.123", "123456789012345678901234567890.123"}, {"NaN", "NaN"},
		{"-Infinity", "-Infinity"}, {"0", "0"}, {".5", "0.5"},
	} {
		d, err := ParseDecimal(c.in)
		if err != nil || d.String() != c.out {
			t.Errorf("%s: %s %v", c.in, d, err)
		}
	}
	for _, bad := range []string{"", "abc", "1.2.3", "1e", "--1"} {
		if _, err := ParseDecimal(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	// wire bytes: big-endian two's complement, minimal
	for _, c := range []struct {
		d    Decimal
		want []byte
	}{
		{NewDecimal(12345, 2), []byte{0x30, 0x39}}, {NewDecimal(-123, 4), []byte{0x85}},
		{NewDecimal(0, 0), []byte{0}}, {NewDecimal(128, 0), []byte{0, 0x80}}, {NewDecimal(-128, 0), []byte{0x80}},
		{NewDecimal(-1, 0), []byte{0xff}}, {NewDecimal(math.MinInt64, 0), []byte{0x80, 0, 0, 0, 0, 0, 0, 0}},
	} {
		if got := c.d.wireBytes(nil); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: % x, want % x", c.d, got, c.want)
		}
	}
	big1, _ := new(big.Int).SetString("-340282366920938463463374607431768211456", 10) // -2^128
	if got := twosComplementBE(big1); len(got) != 17 || got[0] != 0xff || got[1] != 0 {
		t.Errorf("-2^128: % x", got)
	}
	// Arrow decimal decode round trip
	le := make([]byte, 16)
	v := uint64(0xFFFFFFFFFFFFFF85) // -123
	binary.LittleEndian.PutUint64(le, v)
	for i := 8; i < 16; i++ {
		le[i] = 0xff
	}
	if d := decimalFromArrow(le, 4); d.String() != "-0.0123" {
		t.Errorf("arrow decimal %s", d)
	}
}

func TestArrayText(t *testing.T) {
	got, err := parseArrayAny(`{1,NULL,3}`, ckInt)
	if err != nil || !reflect.DeepEqual(got, []any{int64(1), nil, int64(3)}) {
		t.Fatalf("%v %v", got, err)
	}
	var ss []*string
	if err := scanArrayText(`{"a,b",NULL,"q\"x",plain}`, &ss); err != nil || len(ss) != 4 || *ss[0] != "a,b" || ss[1] != nil || *ss[2] != `q"x` || *ss[3] != "plain" {
		t.Fatalf("%v %v", ss, err)
	}
	var m [][]int32
	if err := scanArrayText(`{{1,2},{3,4}}`, &m); err != nil || !reflect.DeepEqual(m, [][]int32{{1, 2}, {3, 4}}) {
		t.Fatalf("%v %v", m, err)
	}
	var e []int64
	if err := scanArrayText(`{}`, &e); err != nil || e == nil || len(e) != 0 {
		t.Fatalf("%v %v", e, err)
	}
	var bad []int64
	if err := scanArrayText(`{1,NULL}`, &bad); err == nil {
		t.Fatal("NULL into []int64 accepted")
	}
}

func TestParseConfig(t *testing.T) {
	c, err := ParseConfig("particledb://u:p%40ss@db.example:7200/app?sslmode=disable&connect_timeout=3&timezone=UTC&pool_max_conns=7&statement_cache_capacity=-1")
	if err != nil {
		t.Fatal(err)
	}
	if c.Host != "db.example" || c.Port != 7200 || c.User != "u" || c.Password != "p@ss" || c.Database != "app" ||
		c.ConnectTimeout != 3*time.Second || c.RuntimeParams["TimeZone"] != "UTC" || c.MaxConns != 7 || c.stmtCacheCap() != 0 || c.TLS != nil {
		t.Fatalf("%+v", c)
	}
	c, err = ParseConfig("host=h port=7300 user=x password='a b\\'c' sslmode=require")
	if err != nil || c.Host != "h" || c.Port != 7300 || c.Password != "a b'c" || c.TLS == nil || !c.TLS.InsecureSkipVerify {
		t.Fatalf("%+v %v", c, err)
	}
	c, _ = ParseConfig("")
	if c.Host != "localhost" || c.Port != DefaultPort || !c.PreferUnixSocket {
		t.Fatalf("%+v", c)
	}
	if p, _ := c.unixSocket(); p != filepath.Join(DefaultUnixSocketDir, ".s.PDB.7173") {
		t.Fatalf("socket %s", p)
	}
	for _, bad := range []string{"particledb://h:0", "sslmode=prefer", "sslmode=verify-ca", "mysql://x", "sslmode=bogus"} {
		if _, err := ParseConfig(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestSQLStateMapping(t *testing.T) {
	for code, want := range map[uint16]string{
		23505: "23505", 8006: "08006", 60008: "22P02", 60003: "25P02", 60036: "08P01", 40001: "40001",
		1064: "XX000", 0x13: "28000", 0x14: "28P01", 7: "57014", 61234: "XX000",
	} {
		if got := sqlStateForCode(code); got != want {
			t.Errorf("%d: %s, want %s", code, got, want)
		}
	}
	e := serverError(1064, "SQL execution error: duplicate key value violates unique constraint \"t_pkey\" [23505]")
	if e.SQLState != "23505" || e.Message != `duplicate key value violates unique constraint "t_pkey"` {
		t.Fatalf("%+v", e)
	}
	if !errors.Is(e, ErrUniqueViolation) || !errors.Is(e, ErrIntegrityViolation) || errors.Is(e, ErrSerializationFailure) {
		t.Fatal("sentinels")
	}
	s := serverError(40001, "could not serialize access")
	if !IsRetryable(s) || !s.Retryable() || !errors.Is(s, ErrSerializationFailure) {
		t.Fatal("retryable")
	}
}

func TestHelloEncoding(t *testing.T) {
	b := encodeHello(0xD3, 0x17, [][2]string{{"TimeZone", "UTC"}})
	if n := binary.LittleEndian.Uint32(b); int(n) != len(b)-8 || b[6] != fHello {
		t.Fatalf("% x", b)
	}
	p := b[8:]
	if p[0] != 2 || p[1] != 1 || p[2] != 0xD3 || p[3] != 1 || p[4] != fmtArrowIPC || p[5] != 1 || p[6] != 0x17 {
		t.Fatalf("% x", p)
	}
	h := parseHelloOK([]byte{2, 0xD2, 1, 36, 0, 0, 0, 0, 0, 1, 0, 1, 0x17})
	if !h.v2() || !h.prepared() || !h.typedParams() || !h.arrayParams() || !h.bulkIngest() || h.maxStreams != 36 {
		t.Fatalf("%+v", h)
	}
	want := []string{"array_params", "batch_sqlstate", "bulk_ingest", "numeric_text", "prepared", "statement_notice", "typed_params", "v2_mux"}
	if !reflect.DeepEqual(h.capabilityNames(), want) {
		t.Fatalf("%v", h.capabilityNames())
	}
}

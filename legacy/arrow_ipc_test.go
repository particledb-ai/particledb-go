package legacy

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math"
	"strings"
	"testing"
	"time"
)

// pyarrowGolden is a stream written by the reference implementation
// (pyarrow 23, ipc.new_stream): columns i int64, f double, s string, b bool,
// y binary; row 0 = (7, 1.5, "ab", true, 0x01), row 1 all NULL.
const pyarrowGolden = "ffffffff280100001000000000000a000c000600050008000a000000000104000c0000000800080000000400080000000400000005000000c000000080000000540000002c0000000400000064ffffff00000104100000001400000004000000000000000100000079000000b8ffffff88ffffff00000106100000001400000004000000000000000100000062000000dcffffffacffffff000001051000000018000000040000000000000001000000730000000400040004000000d4ffffff00000103100000001800000004000000000000000100000066000600080006000600000000000200100014000800060007000c00000010001000000000000102100000001c0000000400000000000000010000006900000008000c000800070008000000000000014000000000000000ffffffff6801000014000000000000000c0016000600050008000c000c0000000003040018000000800000000000000000000a0018000c00040008000a000000dc000000100000000200000000000000000000000c000000000000000000000001000000000000000800000000000000100000000000000018000000000000000100000000000000200000000000000010000000000000003000000000000000010000000000000038000000000000000c000000000000004800000000000000020000000000000050000000000000000100000000000000580000000000000001000000000000006000000000000000010000000000000068000000000000000c0000000000000078000000000000000100000000000000000000000500000002000000000000000100000000000000020000000000000001000000000000000200000000000000010000000000000002000000000000000100000000000000020000000000000001000000000000000100000000000000070000000000000000000000000000000100000000000000000000000000f83f00000000000000000100000000000000000000000200000002000000000000006162000000000000010000000000000001000000000000000100000000000000000000000100000001000000000000000100000000000000ffffffff00000000"

func TestArrowDecode_ReferenceStream(t *testing.T) {
	data, err := hex.DecodeString(pyarrowGolden)
	if err != nil {
		t.Fatal(err)
	}
	res, err := decodeArrowStream(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := res.columns; len(got) != 5 || got[0] != "i" || got[4] != "y" {
		t.Fatalf("columns = %v", got)
	}
	if len(res.rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(res.rows))
	}
	r0 := res.rows[0]
	if r0["i"] != int64(7) || r0["f"] != 1.5 || r0["s"] != "ab" || r0["b"] != true || !bytes.Equal(r0["y"].([]byte), []byte{1}) {
		t.Errorf("row 0 = %#v", r0)
	}
	for k, v := range res.rows[1] {
		if v != nil {
			t.Errorf("row 1 column %s = %#v, want NULL", k, v)
		}
	}
}

// TestArrowEncode_Framing byte-checks the writer's IPC framing: continuation
// markers, 8-byte padded metadata, a V5 Schema then a RecordBatch message,
// an 8-byte aligned body, and the end-of-stream marker.
func TestArrowEncode_Framing(t *testing.T) {
	out, err := encodeArrowParams([][]interface{}{{int64(1), "x"}, {nil, "yz"}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(out, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0}) {
		t.Fatalf("missing end-of-stream marker: % x", out[len(out)-8:])
	}
	off := 0
	var headers []uint8
	for {
		if binary.LittleEndian.Uint32(out[off:]) != arrowContinuation {
			t.Fatalf("message at %d lacks the continuation marker", off)
		}
		metaLen := int(binary.LittleEndian.Uint32(out[off+4:]))
		off += 8
		if metaLen == 0 {
			break
		}
		if metaLen%8 != 0 {
			t.Fatalf("metadata length %d not a multiple of 8", metaLen)
		}
		msg := fbRoot(out[off : off+metaLen])
		if v := msg.i16(0, 0); v != arrowMetadataV5 {
			t.Fatalf("metadata version %d, want V5", v)
		}
		headers = append(headers, msg.u8(1, 0))
		body := int(msg.i64(3, 0))
		if body%8 != 0 {
			t.Fatalf("body length %d not 8-aligned", body)
		}
		off += metaLen + body
	}
	if off != len(out) {
		t.Fatalf("walked %d of %d bytes", off, len(out))
	}
	if len(headers) != 2 || headers[0] != arrowHeaderSchema || headers[1] != arrowHeaderRecordBatch {
		t.Fatalf("message headers = %v, want [Schema RecordBatch]", headers)
	}
}

func TestArrowEncode_RoundTrip(t *testing.T) {
	ts := time.Date(2026, 10, 12, 1, 2, 3, 456789000, time.UTC)
	rows := [][]interface{}{
		{int64(math.MinInt64), 1.25, "héllo", true, []byte{0, 255}, ts, nil, 3},
		{nil, nil, nil, nil, nil, nil, nil, 2.5},
		{int64(math.MaxInt64), -0.0, "", false, []byte{}, nil, nil, nil},
	}
	out, err := encodeArrowParams(rows, 8)
	if err != nil {
		t.Fatal(err)
	}
	res, err := decodeArrowStream(out)
	if err != nil {
		t.Fatalf("decode own stream: %v", err)
	}
	if len(res.rows) != 3 {
		t.Fatalf("rows = %d", len(res.rows))
	}
	r := res.rows[0]
	if r["p1"] != int64(math.MinInt64) || r["p2"] != 1.25 || r["p3"] != "héllo" || r["p4"] != true ||
		!bytes.Equal(r["p5"].([]byte), []byte{0, 255}) || !r["p6"].(time.Time).Equal(ts) || r["p7"] != nil {
		t.Errorf("row 0 = %#v", r)
	}
	// Mixed int and float in one column is sent as Float64.
	if r["p8"] != float64(3) || res.rows[1]["p8"] != 2.5 || res.rows[2]["p8"] != nil {
		t.Errorf("p8 column = %#v %#v %#v", r["p8"], res.rows[1]["p8"], res.rows[2]["p8"])
	}
	for k, v := range res.rows[1] {
		if k != "p8" && v != nil {
			t.Errorf("row 1 %s = %#v, want NULL", k, v)
		}
	}
	r = res.rows[2]
	if r["p3"] != "" || r["p5"] == nil || len(r["p5"].([]byte)) != 0 || r["p4"] != false {
		t.Errorf("row 2 empty values must stay non-NULL: %#v", r)
	}
}

func TestArrowEncode_Rejects(t *testing.T) {
	if _, err := encodeArrowParams([][]interface{}{{"a"}, {int64(1)}}, 1); err == nil {
		t.Error("string and int64 in one column must be refused")
	}
	if _, err := encodeArrowParams([][]interface{}{{struct{}{}}}, 1); err == nil {
		t.Error("an unsupported Go type must be refused")
	}
	if _, err := encodeArrowParams([][]interface{}{{1, 2}}, 1); err == nil {
		t.Error("a row with the wrong value count must be refused")
	}
	if _, err := encodeArrowParams([][]interface{}{{uint64(math.MaxUint64)}}, 1); err == nil {
		t.Error("a uint64 above MaxInt64 must be refused")
	}
}

func TestArrowDecode_RejectsMalformed(t *testing.T) {
	good, _ := encodeArrowParams([][]interface{}{{int64(1)}}, 1)
	// A truncated stream (anything short of the full record batch) must never
	// decode into rows.
	for n := 1; n < len(good)-8; n++ {
		if res, err := decodeArrowStream(good[:n]); err == nil && len(res.rows) > 0 {
			t.Fatalf("truncated stream (%d of %d bytes) decoded to %v", n, len(good), res.rows)
		}
	}
	bad := append([]byte(nil), good...)
	binary.LittleEndian.PutUint32(bad[4:], 0x7FFFFFF0) // metadata length past the end
	if _, err := decodeArrowStream(bad); err == nil {
		t.Error("oversized metadata length must be an error")
	}
}

func TestFormatDecimal(t *testing.T) {
	le := func(v int64) []byte {
		b := make([]byte, 16)
		binary.LittleEndian.PutUint64(b, uint64(v))
		if v < 0 {
			for i := 8; i < 16; i++ {
				b[i] = 0xFF
			}
		}
		return b
	}
	cases := []struct {
		v     int64
		scale int
		want  string
	}{{1234, 2, "12.34"}, {-5, 2, "-0.05"}, {7, 0, "7"}, {0, 3, "0.000"}}
	for _, c := range cases {
		if got := formatDecimal(le(c.v), c.scale); got != c.want {
			t.Errorf("formatDecimal(%d, %d) = %q, want %q", c.v, c.scale, got, c.want)
		}
	}
}

// An integer above 2^53 mixed with floats in one parameter column cannot be
// sent as Float64 exactly: refused, never rounded.
func TestEncodeParamsRefusesLossyIntToFloatPromotion(t *testing.T) {
	_, err := encodeArrowParams([][]interface{}{{int64(9007199254740993)}, {float64(1)}}, 1)
	if err == nil || !strings.Contains(err.Error(), "2^53") {
		t.Fatalf("want a 2^53 refusal, got %v", err)
	}
	// Exact integers still promote.
	if _, err := encodeArrowParams([][]interface{}{{int64(1) << 53}, {float64(0.5)}}, 1); err != nil {
		t.Fatalf("2^53 is exact in float64: %v", err)
	}
}

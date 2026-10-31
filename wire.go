package particledb

// Native wire protocol 2 framing: constants, frame layout, HELLO / HELLO_OK.
//
// See PROTOCOL.md in this repository; the reference client is the JDBC
// driver's WireV2Codec.
// HELLO and HELLO_OK travel in the 8-byte protocol 1 framing; every later
// frame uses the 11-byte protocol 2 header:
//
//	[u8 version=2][u8 type][u8 flags][u32 LE stream id][u32 LE payload length]

import (
	"encoding/binary"
	"sort"
)

const (
	v1HeaderSize = 8
	v2HeaderSize = 11
	maxPayload   = 16 << 20

	protoV2 = 2

	// client -> server
	fHello      = 0x00
	fQuery      = 0x01
	fBatch      = 0x02
	fPing       = 0x03
	fClose      = 0x04
	fRstStreamC = 0x06
	fAuth       = 0x09
	fKVSet      = 0x10
	fKVGet      = 0x11
	fKVDel      = 0x12
	fKVIncr     = 0x13
	fPrepare    = 0x20
	fExecute    = 0x21
	fCloseStmt  = 0x22
	fBulk       = 0x40

	// server -> client
	fHelloOK     = 0x80
	fArrowResult = 0x81
	fOK          = 0x82
	fError       = 0x83
	fPong        = 0x84
	fBatchResult = 0x85
	fArrowStream = 0x86
	fKVBulk      = 0x87
	fKVNil       = 0x88
	fRstStreamS  = 0x89
	fGoaway      = 0x8a
	fSettingsAck = 0x8b
	fNotice      = 0x8c
	fPrepareOK   = 0x8d

	// bulk ingest subops
	bulkBegin = 0x01
	bulkData  = 0x02
	bulkEnd   = 0x03
	bulkAbort = 0x04

	// caps byte
	capV1Legacy        = 0x01
	capV2Mux           = 0x02
	capCompression     = 0x04
	capTLSNextProto    = 0x08
	capNumericText     = 0x10
	capAuthRequired    = 0x20 // server -> client only
	capStatementNotice = 0x40
	capPrepared        = 0x80

	// extension byte 0
	capExtTypedParams   = 0x01
	capExtBulkIngest    = 0x02
	capExtBatchSQLState = 0x04
	capExtSessionParams = 0x08
	capExtArrayParams   = 0x10

	fmtArrowIPC = 0x01

	// frame flags
	flagEndStream     = 0x01
	flagMoreChunks    = 0x02
	flagTxnBegin      = 0x08
	flagTxnOpen       = 0x10
	flagTxnFailed     = 0x20
	flagTxnRolledBack = 0x40

	// batch result entry tag carrying a SQLSTATE
	batchEntryErrorSQLState = 0x90

	// RST_STREAM / GOAWAY codes
	errNoError     = 0x0000
	errStreamLimit = 0x0004
	errCancel      = 0x0007

	maxAuthFieldLen = 4096
)

// frame is one decoded protocol 2 frame.
type frame struct {
	typ     byte
	flags   byte
	sid     uint32
	payload []byte
}

// terminal reports whether f ends its stream: everything except a NOTICE and
// an ARROW_STREAM chunk that has more chunks after it.
func (f *frame) terminal() bool {
	switch f.typ {
	case fNotice:
		return false
	case fArrowStream:
		return f.flags&flagMoreChunks == 0
	}
	return true
}

func appendV2Header(b []byte, typ, flags byte, sid uint32, n int) []byte {
	return append(b, protoV2, typ, flags,
		byte(sid), byte(sid>>8), byte(sid>>16), byte(sid>>24),
		byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
}

// encodeHello builds the protocol 1 framed HELLO with the caps byte, the
// extension block and, when params is non-empty, the session parameter block.
func encodeHello(caps, ext byte, params [][2]string) []byte {
	p := []byte{protoV2, 1, caps, 1, fmtArrowIPC, 1, ext}
	if len(params) > 0 {
		p = binary.LittleEndian.AppendUint16(p, uint16(len(params)))
		for _, kv := range params {
			for _, s := range kv {
				p = binary.LittleEndian.AppendUint16(p, uint16(len(s)))
				p = append(p, s...)
			}
		}
	}
	out := make([]byte, 0, v1HeaderSize+len(p))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(p)))
	out = append(out, 0, 0, fHello, 0)
	return append(out, p...)
}

// helloOK is the server's answer to HELLO.
type helloOK struct {
	version    byte
	caps       byte
	format     byte
	maxStreams uint32
	window     uint32
	ext        byte
}

func parseHelloOK(p []byte) helloOK {
	if len(p) < 3 {
		h := helloOK{version: 1, format: fmtArrowIPC, maxStreams: 1}
		if len(p) > 0 {
			h.version = p[0]
		}
		return h
	}
	h := helloOK{version: p[0], caps: p[1], format: p[2], maxStreams: 1}
	if len(p) >= 11 {
		h.maxStreams = binary.LittleEndian.Uint32(p[3:])
		h.window = binary.LittleEndian.Uint32(p[7:])
	}
	if len(p) >= 13 && p[11] >= 1 {
		h.ext = p[12]
	}
	return h
}

func (h helloOK) v2() bool            { return h.caps&capV2Mux != 0 }
func (h helloOK) prepared() bool      { return h.v2() && h.caps&capPrepared != 0 }
func (h helloOK) typedParams() bool   { return h.prepared() && h.ext&capExtTypedParams != 0 }
func (h helloOK) arrayParams() bool   { return h.prepared() && h.ext&capExtArrayParams != 0 }
func (h helloOK) bulkIngest() bool    { return h.v2() && h.ext&capExtBulkIngest != 0 }
func (h helloOK) authRequired() bool  { return h.caps&capAuthRequired != 0 }
func (h helloOK) sessionParams() bool { return h.ext&capExtSessionParams != 0 }
func (h helloOK) tlsUpgradeOK() bool  { return h.caps&capTLSNextProto != 0 }
func (h helloOK) protocolName() string {
	return map[bool]string{true: "pdb-native-v2", false: "pdb-native-v1"}[h.v2()]
}

// capabilityNames are the negotiated capabilities, named and sorted as the
// server's SHOW pdb.wire_capabilities names them.
func (h helloOK) capabilityNames() []string {
	var n []string
	add := func(on bool, s string) {
		if on {
			n = append(n, s)
		}
	}
	add(h.caps&capV2Mux != 0, "v2_mux")
	add(h.caps&capCompression != 0, "compression")
	add(h.caps&capTLSNextProto != 0, "tls")
	add(h.caps&capNumericText != 0, "numeric_text")
	add(h.caps&capStatementNotice != 0, "statement_notice")
	add(h.caps&capPrepared != 0, "prepared")
	add(h.ext&capExtTypedParams != 0, "typed_params")
	add(h.ext&capExtBulkIngest != 0, "bulk_ingest")
	add(h.ext&capExtBatchSQLState != 0, "batch_sqlstate")
	add(h.ext&capExtSessionParams != 0, "session_params")
	add(h.ext&capExtArrayParams != 0, "array_params")
	sort.Strings(n)
	return n
}

func encodeAuth(user, password string) []byte {
	out := make([]byte, 0, 8+len(user)+len(password))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(user)))
	out = append(out, user...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(password)))
	return append(out, password...)
}

func appendLenStr(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

func encodeBatchPayload(sqls []string) []byte {
	n := 4
	for _, s := range sqls {
		n += 4 + len(s)
	}
	out := make([]byte, 0, n)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(sqls)))
	for _, s := range sqls {
		out = appendLenStr(out, s)
	}
	return out
}

// notice is one statement advisory (PostgreSQL's NoticeResponse).
type Notice struct {
	Severity string
	SQLState string
	Message  string
}

func decodeNotices(p []byte) []Notice {
	if len(p) < 2 {
		return nil
	}
	count := int(binary.LittleEndian.Uint16(p))
	off := 2
	out := make([]Notice, 0, count)
	for i := 0; i < count; i++ {
		if off >= len(p) {
			return out
		}
		sl := int(p[off])
		off++
		if off+sl > len(p) {
			return out
		}
		sev := string(p[off : off+sl])
		off += sl
		if off >= len(p) {
			return out
		}
		tl := int(p[off])
		off++
		if off+tl+4 > len(p) {
			return out
		}
		st := string(p[off : off+tl])
		off += tl
		ml := int(binary.LittleEndian.Uint32(p[off:]))
		off += 4
		if ml < 0 || off+ml > len(p) {
			return out
		}
		out = append(out, Notice{Severity: sev, SQLState: st, Message: string(p[off : off+ml])})
		off += ml
	}
	return out
}

// batchEntry is one decoded BATCH_RESULT entry.
type batchEntry struct {
	affected int64  // OK entry
	arrow    []byte // ARROW_RESULT entry
	isArrow  bool
}

// decodeBatchResult decodes a BATCH_RESULT payload; a failed entry comes
// back as *Error (with the completed entries).
func decodeBatchResult(p []byte) ([]batchEntry, *Error, error) {
	if len(p) < 4 {
		return nil, nil, protoErr("batch result shorter than its count")
	}
	count := int(binary.LittleEndian.Uint32(p))
	if count > (len(p)-4)/5 {
		return nil, nil, protoErr("batch result claims %d entries in %d bytes", count, len(p))
	}
	off := 4
	entries := make([]batchEntry, 0, count)
	var failed *Error
	for i := 0; i < count; i++ {
		if failed != nil {
			return nil, nil, protoErr("batch result has entries after an error entry")
		}
		if off >= len(p) {
			return nil, nil, protoErr("batch result truncated")
		}
		t := p[off]
		off++
		switch t {
		case fOK:
			if off+8 > len(p) {
				return nil, nil, protoErr("batch result truncated")
			}
			entries = append(entries, batchEntry{affected: int64(binary.LittleEndian.Uint64(p[off:]))})
			off += 8
		case batchEntryErrorSQLState:
			if off+11 > len(p) {
				return nil, nil, protoErr("batch result truncated")
			}
			code := binary.LittleEndian.Uint16(p[off:])
			state := string(p[off+2 : off+7])
			off += 7
			n := int(binary.LittleEndian.Uint32(p[off:]))
			off += 4
			if n < 0 || off+n > len(p) {
				return nil, nil, protoErr("batch result truncated")
			}
			failed = serverError(code, string(p[off:off+n]))
			failed.SQLState = state
			off += n
		case fArrowResult, fError:
			if off+4 > len(p) {
				return nil, nil, protoErr("batch result truncated")
			}
			n := int(binary.LittleEndian.Uint32(p[off:]))
			off += 4
			if n < 0 || off+n > len(p) {
				return nil, nil, protoErr("batch result truncated")
			}
			if t == fArrowResult {
				entries = append(entries, batchEntry{arrow: p[off : off+n], isArrow: true})
			} else {
				failed = serverError(1064, string(p[off:off+n]))
			}
			off += n
		default:
			return nil, nil, protoErr("unexpected batch entry type 0x%02x", t)
		}
	}
	return entries, failed, nil
}

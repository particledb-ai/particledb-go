package legacy

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

// --- transport selection (no server) ---

func TestNew_DefaultsToNativeWire(t *testing.T) {
	c, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.Protocol() != ProtocolNative || c.Native() == nil || c.rawDB != nil {
		t.Fatalf("default client: protocol %q native=%v rawDB=%v", c.Protocol(), c.Native() != nil, c.rawDB != nil)
	}
	if c.native.opts.Port != WireDefaultPort {
		t.Errorf("native port = %d, want %d", c.native.opts.Port, WireDefaultPort)
	}
	// A zero Port means the native default.
	c2, _ := New(Config{Host: "h"})
	if c2.native == nil || c2.native.opts.Port != WireDefaultPort {
		t.Errorf("zero-port config did not default to the native port")
	}
}

func TestNew_PGIsExplicitOptIn(t *testing.T) {
	c, err := New(DefaultPGConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.Protocol() != ProtocolPG || c.Native() != nil || c.rawDB == nil {
		t.Fatalf("PG client: protocol %q native=%v", c.Protocol(), c.Native() != nil)
	}
	if !strings.Contains(c.config.DSN(), "port=5432") {
		t.Errorf("PG DSN = %q", c.config.DSN())
	}
}

func TestNew_RefusesTLSOnNative(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SSLMode = "require"
	if _, err := New(cfg); err == nil {
		t.Fatal("sslmode=require on the native wire must be refused, not sent in plaintext")
	}
	cfg.SSLMode = "disable"
	cfg.Protocol = "carrier-pigeon"
	if _, err := New(cfg); err == nil {
		t.Fatal("an unknown protocol must be refused")
	}
}

func TestConnect_Forms(t *testing.T) {
	cases := []struct {
		dsn      string
		protocol Protocol
		host     string
		port     int
		user     string
		pass     string
	}{
		{"host=db port=7000 user=u password='p w'", ProtocolNative, "db", 7000, "u", "p w"},
		{"pdb://alice:s3cret@db2:7001?pool_size=3", ProtocolNative, "db2", 7001, "alice", "s3cret"},
		{"particledb://db3", ProtocolNative, "db3", WireDefaultPort, "particledb", ""},
		{"host=db port=5432 user=u protocol=pg", ProtocolPG, "", 0, "", ""},
		{"postgres://u@db:5432/x?sslmode=disable", ProtocolPG, "", 0, "", ""},
	}
	for _, tc := range cases {
		c, err := Connect(tc.dsn)
		if err != nil {
			t.Fatalf("Connect(%q): %v", tc.dsn, err)
		}
		if c.Protocol() != tc.protocol {
			t.Errorf("Connect(%q) protocol = %q, want %q", tc.dsn, c.Protocol(), tc.protocol)
		}
		if tc.protocol == ProtocolNative {
			o := c.native.opts
			if o.Host != tc.host || o.Port != tc.port || o.User != tc.user || o.Password != tc.pass {
				t.Errorf("Connect(%q) opts = %s:%d user=%q pass=%q", tc.dsn, o.Host, o.Port, o.User, o.Password)
			}
		}
		c.Close()
	}
	if _, err := Connect("host=db sslmode=require"); err == nil {
		t.Error("native DSN with sslmode=require must be refused")
	}
	if _, err := Connect("host=db bogus=1"); err == nil {
		t.Error("an unknown native DSN key must be refused")
	}
	if _, _, isPG, _ := parseKeyValueDSN("host=a protocol=pg port=1"); !isPG {
		t.Error("protocol=pg not detected")
	}
	if _, rest, _, _ := parseKeyValueDSN("host=a protocol=pg port=1"); rest != "host=a port=1" {
		t.Errorf("protocol key not stripped: %q", rest)
	}
}

// --- scripted server: EXECUTE_BATCH ---

func batchResultFrame(entries ...[]byte) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(len(entries)))
	for _, e := range entries {
		b = append(b, e...)
	}
	return b
}

func okEntry(n uint64) []byte {
	b := []byte{batchEntryOK, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.LittleEndian.PutUint64(b[1:], n)
	return b
}

func errEntry(msg string) []byte {
	b := []byte{batchEntryError, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(b[1:], uint32(len(msg)))
	return append(b, msg...)
}

// TestExecuteBatch_PerRowErrors: one PREPARE, one EXECUTE_BATCH carrying all
// rows as one Arrow batch, and a failed row surfacing as *PdbBatchError (with
// the row index) that also unwraps to *PdbQueryError - never as success.
func TestExecuteBatch_PerRowErrors(t *testing.T) {
	client, serverConn := mockClientServer(t)
	defer client.Close()

	type frame struct {
		typ  byte
		data []byte
	}
	var got []frame
	done := make(chan struct{})
	go func() {
		defer close(done)
		typ, data, _ := readClientFrame(serverConn) // PREPARE
		got = append(got, frame{typ, data})
		ok := make([]byte, 4)
		binary.LittleEndian.PutUint16(ok, 2)
		_ = writeServerFrame(serverConn, msgWirePrepareOK, 0, ok)
		typ, data, _ = readClientFrame(serverConn) // EXECUTE_BATCH
		got = append(got, frame{typ, data})
		_ = writeServerFrame(serverConn, msgWireBatchResult, 0,
			batchResultFrame(okEntry(1), errEntry("duplicate key value"), okEntry(1)))
		typ, data, _ = readClientFrame(serverConn) // ROLLBACK (BATCH)
		got = append(got, frame{typ, data})
		_ = writeServerFrame(serverConn, msgWireBatchResult, 0, batchResultFrame(okEntry(0)))
	}()

	rows := [][]interface{}{{1, "a"}, {1, "dup"}, {3, nil}}
	res, err := client.ExecuteBatch(context.Background(), "INSERT INTO t VALUES ($1, $2)", rows)
	<-done
	go drainConn(serverConn)

	var be *PdbBatchError
	if !errors.As(err, &be) {
		t.Fatalf("err = %T %v, want *PdbBatchError", err, err)
	}
	var qe *PdbQueryError
	if !errors.As(err, &qe) || !strings.Contains(qe.Message, "duplicate key") {
		t.Errorf("batch error does not unwrap to the row's *PdbQueryError: %v", err)
	}
	if res == nil || res.Attempted != 3 || len(res.Errors) != 1 || res.Errors[0].Row != 1 || res.TotalAffected() != 2 {
		t.Fatalf("result = %+v", res)
	}
	if got[0].typ != msgWirePrepare || got[1].typ != msgWireExecuteBatch || got[2].typ != msgWireBatch {
		t.Fatalf("frame types = %x %x %x", got[0].typ, got[1].typ, got[2].typ)
	}
	nameLen := binary.LittleEndian.Uint32(got[1].data)
	params, err := decodeArrowStream(got[1].data[4+nameLen:])
	if err != nil {
		t.Fatalf("EXECUTE_BATCH payload: %v", err)
	}
	if len(params.rows) != 3 || params.rows[2]["p1"] != int64(3) || params.rows[2]["p2"] != nil {
		t.Errorf("params = %v", params.rows)
	}
	if client.pool.idleCount() != 1 {
		t.Errorf("connection not returned to the pool after a clean ROLLBACK")
	}
}

// TestPreparedCache_ReusesHandle: a second Query of the same SQL on the same
// connection sends EXECUTE only (no second PREPARE).
func TestPreparedCache_ReusesHandle(t *testing.T) {
	client, serverConn := mockClientServer(t)
	defer client.Close()

	var types []byte
	done := make(chan struct{})
	go func() {
		defer close(done)
		ok := make([]byte, 4)
		binary.LittleEndian.PutUint16(ok, 1)
		for i := 0; i < 3; i++ {
			typ, _, _ := readClientFrame(serverConn)
			types = append(types, typ)
			if typ == msgWirePrepare {
				_ = writeServerFrame(serverConn, msgWirePrepareOK, 0, ok)
			} else {
				_ = writeOKFrame(serverConn, 1)
			}
		}
	}()
	for i := 0; i < 2; i++ {
		if _, err := client.Execute(context.Background(), "DELETE FROM t WHERE id = $1", i); err != nil {
			t.Fatal(err)
		}
	}
	<-done
	go drainConn(serverConn)
	want := []byte{msgWirePrepare, msgWireExecute, msgWireExecute}
	if string(types) != string(want) {
		t.Fatalf("frames = % x, want % x", types, want)
	}
}

// TestParamCountMismatch is refused client-side with *PdbParamError and the
// connection stays pooled.
func TestParamCountMismatch(t *testing.T) {
	client, serverConn := mockClientServer(t)
	defer client.Close()
	go func() {
		_, _, _ = readClientFrame(serverConn)
		ok := make([]byte, 4)
		binary.LittleEndian.PutUint16(ok, 2)
		_ = writeServerFrame(serverConn, msgWirePrepareOK, 0, ok)
		drainConn(serverConn)
	}()
	_, err := client.Execute(context.Background(), "UPDATE t SET a = $1 WHERE id = $2", 1)
	var pe *PdbParamError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %T %v, want *PdbParamError", err, err)
	}
	if client.pool.idleCount() != 1 {
		t.Error("connection must stay pooled after a client-side parameter error")
	}
}

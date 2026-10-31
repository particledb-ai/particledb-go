package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"time"
)

// resp is a minimal RESP2 client for the KV parity cases.
type resp struct {
	nc net.Conn
	br *bufio.Reader
}

func dialResp() *resp {
	nc, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host(), respPort()), 5*time.Second)
	must(err, "RESP connect")
	return &resp{nc: nc, br: bufio.NewReader(nc)}
}

func (r *resp) close() { r.nc.Close() }

type respError string

// cmd sends a command; arguments are strings or []byte. The reply is a
// string (simple), int64, []byte (bulk, nil for absent), []any or a
// respError.
func (r *resp) cmd(args ...any) any {
	b := []byte("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		var v []byte
		switch x := a.(type) {
		case string:
			v = []byte(x)
		case []byte:
			v = x
		default:
			v = []byte(fmt.Sprint(x))
		}
		b = append(b, "$"+strconv.Itoa(len(v))+"\r\n"...)
		b = append(b, v...)
		b = append(b, "\r\n"...)
	}
	_ = r.nc.SetDeadline(time.Now().Add(30 * time.Second))
	_, err := r.nc.Write(b)
	must(err, "RESP write")
	return r.read()
}

func (r *resp) line() string {
	l, err := r.br.ReadString('\n')
	must(err, "RESP read")
	return l[:len(l)-2]
}

func (r *resp) read() any {
	l := r.line()
	switch l[0] {
	case '+':
		return l[1:]
	case '-':
		return respError(l[1:])
	case ':':
		n, _ := strconv.ParseInt(l[1:], 10, 64)
		return n
	case '$':
		n, _ := strconv.Atoi(l[1:])
		if n < 0 {
			return []byte(nil)
		}
		buf := make([]byte, n+2)
		_, err := ioReadFull(r.br, buf)
		must(err, "RESP bulk")
		return buf[:n]
	case '*':
		n, _ := strconv.Atoi(l[1:])
		if n < 0 {
			return nil
		}
		out := make([]any, n)
		for i := range out {
			out[i] = r.read()
		}
		return out
	}
	fail("RESP: unexpected reply %q", l)
	return nil
}

func (r *resp) str(args ...any) any {
	v := r.cmd(args...)
	switch x := v.(type) {
	case []byte:
		if x == nil {
			return nil
		}
		return string(x)
	case respError:
		fail("RESP %v: %s", args, string(x))
	}
	return v
}

func (r *resp) num(args ...any) int64 {
	v := r.cmd(args...)
	switch x := v.(type) {
	case int64:
		return x
	case respError:
		fail("RESP %v: %s", args, string(x))
	}
	fail("RESP %v: non-integer reply %v", args, v)
	return 0
}

func ioReadFull(br *bufio.Reader, b []byte) (int, error) {
	n := 0
	for n < len(b) {
		m, err := br.Read(b[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

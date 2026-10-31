package particledb

// Arrow IPC stream writer for native bulk ingest (F_BULK DATA frames): a
// hand-written forward flatbuffer layout, no external dependency. Adapted
// from the legacy SDK's parameter writer.

import (
	"encoding/binary"
	"fmt"
)

const arrowMetadataV5 = 4

type fbKind int

const (
	fbNone fbKind = iota
	fbScalar
	fbRef
)

type fbField struct {
	kind fbKind
	size int // scalar byte width: 1, 2, 4 or 8
	val  uint64
	ref  any // *fbTable | fbString | fbTableVec | fbStructVec
}

type fbTable struct{ fields []fbField }

type fbString string

type fbTableVec []*fbTable

// fbStructVec is a vector of inline structs whose elements need 8-byte alignment.
type fbStructVec struct {
	n    int
	data []byte
}

func fbU8(v uint8) fbField  { return fbField{kind: fbScalar, size: 1, val: uint64(v)} }
func fbI16(v int16) fbField { return fbField{kind: fbScalar, size: 2, val: uint64(uint16(v))} }
func fbI32(v int32) fbField { return fbField{kind: fbScalar, size: 4, val: uint64(uint32(v))} }
func fbI64(v int64) fbField { return fbField{kind: fbScalar, size: 8, val: uint64(v)} }
func fbObj(o any) fbField   { return fbField{kind: fbRef, size: 4, ref: o} }
func fbAbsent() fbField     { return fbField{} }
func fbBool(v bool) fbField {
	if v {
		return fbU8(1)
	}
	return fbU8(0)
}

type fbWriter struct{ buf []byte }

func (w *fbWriter) pad(align int) {
	for len(w.buf)%align != 0 {
		w.buf = append(w.buf, 0)
	}
}

func (w *fbWriter) put16(pos int, v uint16) { binary.LittleEndian.PutUint16(w.buf[pos:], v) }
func (w *fbWriter) put32(pos int, v uint32) { binary.LittleEndian.PutUint32(w.buf[pos:], v) }

func (w *fbWriter) grow(n int) int {
	pos := len(w.buf)
	w.buf = append(w.buf, make([]byte, n)...)
	return pos
}

// finish serializes root as a complete flatbuffer.
func (w *fbWriter) finish(root *fbTable) []byte {
	w.buf = w.buf[:0]
	w.grow(4)
	t := w.writeObj(root)
	w.put32(0, uint32(t))
	return w.buf
}

func (w *fbWriter) writeObj(o any) int {
	switch v := o.(type) {
	case *fbTable:
		return w.writeTable(v)
	case fbString:
		w.pad(4)
		pos := w.grow(4 + len(v) + 1)
		w.put32(pos, uint32(len(v)))
		copy(w.buf[pos+4:], v)
		return pos
	case fbTableVec:
		w.pad(4)
		pos := w.grow(4 + 4*len(v))
		w.put32(pos, uint32(len(v)))
		for i, t := range v {
			slot := pos + 4 + 4*i
			child := w.writeObj(t)
			w.put32(slot, uint32(child-slot))
		}
		return pos
	case fbStructVec:
		// Length prefix at P with (P+4) % 8 == 0 so the elements are 8-aligned.
		w.pad(4)
		if (len(w.buf)+4)%8 != 0 {
			w.grow(4)
		}
		pos := w.grow(4)
		w.put32(pos, uint32(v.n))
		w.buf = append(w.buf, v.data...)
		return pos
	}
	panic(fmt.Sprintf("fbWriter: unsupported object %T", o))
}

func (w *fbWriter) writeTable(t *fbTable) int {
	nslots := len(t.fields)
	for nslots > 0 && t.fields[nslots-1].kind == fbNone {
		nslots--
	}
	// vtable: [u16 vtable bytes][u16 inline table bytes][u16 field offset]*
	w.pad(2)
	vt := w.grow(4 + 2*nslots)
	w.put16(vt, uint16(4+2*nslots))

	has8 := false
	for _, f := range t.fields[:nslots] {
		if f.kind != fbNone && f.size == 8 {
			has8 = true
		}
	}
	// Table start T is 4-aligned; with an 8-byte field T % 8 == 4 so the
	// fields (which start right after the 4-byte soffset) are 8-aligned.
	w.pad(4)
	if has8 && len(w.buf)%8 != 4 {
		w.grow(4)
	}
	tpos := w.grow(4)
	w.put32(tpos, uint32(int32(tpos-vt)))

	fieldPos := make([]int, nslots)
	for _, size := range []int{8, 4, 2, 1} {
		for i, f := range t.fields[:nslots] {
			if f.kind == fbNone || f.size != size {
				continue
			}
			p := w.grow(size)
			fieldPos[i] = p
			switch size {
			case 1:
				w.buf[p] = byte(f.val)
			case 2:
				w.put16(p, uint16(f.val))
			case 4:
				w.put32(p, uint32(f.val))
			case 8:
				binary.LittleEndian.PutUint64(w.buf[p:], f.val)
			}
		}
	}
	w.put16(vt+2, uint16(len(w.buf)-tpos))
	for i, f := range t.fields[:nslots] {
		if f.kind != fbNone {
			w.put16(vt+4+2*i, uint16(fieldPos[i]-tpos))
		}
	}
	for i, f := range t.fields[:nslots] {
		if f.kind == fbRef {
			child := w.writeObj(f.ref)
			w.put32(fieldPos[i], uint32(child-fieldPos[i]))
		}
	}
	return tpos
}

// encodeMessage frames one IPC message: continuation marker, metadata length,
// metadata padded to 8 bytes, body.
func encodeMessage(out []byte, meta []byte, body []byte) []byte {
	padded := (len(meta) + 7) &^ 7
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], arrowContinuation)
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(padded))
	out = append(out, hdr[:]...)
	out = append(out, meta...)
	out = append(out, make([]byte, padded-len(meta))...)
	return append(out, body...)
}

func messageTable(headerType uint8, header *fbTable, bodyLen int64) *fbTable {
	return &fbTable{fields: []fbField{
		fbI16(arrowMetadataV5),
		fbU8(headerType),
		fbObj(header),
		fbI64(bodyLen),
	}}
}

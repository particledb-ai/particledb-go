package legacy

// grpc_service.go contains hand-written gRPC service client implementations
// matching the RPCs defined in proto/particledb.proto and proto/query.proto.
//
// These are equivalent to what protoc-gen-go-grpc would generate but are kept
// here so the SDK can be used without a protoc build step.

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"google.golang.org/grpc"
)

// ---------------------------------------------------------------------------
// Manual protobuf serialization helpers
//
// The server speaks standard protobuf wire format. Rather than pulling in the
// full protobuf runtime reflection machinery, we implement minimal Marshal /
// Unmarshal for the handful of message types the client needs.
// ---------------------------------------------------------------------------

// --- wire helpers ---

func appendVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func appendTag(b []byte, field uint32, wireType uint32) []byte {
	return appendVarint(b, uint64(field<<3|wireType))
}

func appendString(b []byte, field uint32, s string) []byte {
	if s == "" {
		return b
	}
	b = appendTag(b, field, 2) // length-delimited
	b = appendVarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendBytes(b []byte, field uint32, data []byte) []byte {
	if len(data) == 0 {
		return b
	}
	b = appendTag(b, field, 2)
	b = appendVarint(b, uint64(len(data)))
	return append(b, data...)
}

func appendVarintField(b []byte, field uint32, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = appendTag(b, field, 0)
	return appendVarint(b, v)
}

func appendFixed64Field(b []byte, field uint32, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = appendTag(b, field, 1)
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	return append(b, buf[:]...)
}

func appendMessage(b []byte, field uint32, msg []byte) []byte {
	if len(msg) == 0 {
		return b
	}
	b = appendTag(b, field, 2)
	b = appendVarint(b, uint64(len(msg)))
	return append(b, msg...)
}

// --- Value marshal ---

func marshalPBValue(v *PBValue) []byte {
	if v == nil || v.Kind == nil {
		return nil
	}
	var b []byte
	switch k := v.Kind.(type) {
	case *PBValue_IntValue:
		b = appendTag(b, 1, 0)
		b = appendVarint(b, uint64(k.IntValue))
	case *PBValue_FloatValue:
		b = appendTag(b, 2, 1)
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(k.FloatValue))
		b = append(b, buf[:]...)
	case *PBValue_StringValue:
		b = appendString(b, 3, k.StringValue)
	case *PBValue_BoolValue:
		if k.BoolValue {
			b = appendVarintField(b, 4, 1)
		}
	case *PBValue_NullValue:
		if k.NullValue {
			b = appendVarintField(b, 5, 1)
		}
	case *PBValue_BytesValue:
		b = appendBytes(b, 6, k.BytesValue)
	}
	return b
}

// --- Request marshalers ---

func marshalExecuteSqlRequest(r *PBExecuteSqlRequest) ([]byte, error) {
	var b []byte
	b = appendString(b, 1, r.Sql)
	for _, p := range r.Params {
		b = appendMessage(b, 2, marshalPBValue(p))
	}
	return b, nil
}

func marshalBatchExecuteRequest(r *PBBatchExecuteRequest) ([]byte, error) {
	var b []byte
	for _, s := range r.Statements {
		b = appendString(b, 1, s)
	}
	return b, nil
}

func marshalHealthRequest(_ *PBHealthRequest) ([]byte, error) {
	return nil, nil // empty message
}

func marshalQueryRequest(r *PBQueryRequest) ([]byte, error) {
	var b []byte
	b = appendString(b, 1, r.Sql)
	if r.TxnId != nil {
		b = appendMessage(b, 2, marshalTxnId(r.TxnId))
	}
	b = appendVarintField(b, 3, uint64(r.Mode))
	b = appendVarintField(b, 4, r.MaxRows)
	b = appendBytes(b, 5, r.PageToken)
	for _, p := range r.Params {
		b = appendBytes(b, 6, p)
	}
	return b, nil
}

func marshalExecuteRequest(r *PBExecuteRequest) ([]byte, error) {
	var b []byte
	b = appendString(b, 1, r.Sql)
	if r.TxnId != nil {
		b = appendMessage(b, 2, marshalTxnId(r.TxnId))
	}
	for _, p := range r.Params {
		b = appendBytes(b, 3, p)
	}
	return b, nil
}

func marshalTxnId(t *PBTxnId) []byte {
	if t == nil {
		return nil
	}
	var b []byte
	b = appendBytes(b, 1, t.Id)
	return b
}

// --- wire reader ---

type wireReader struct {
	data []byte
	pos  int
}

func (r *wireReader) remaining() int { return len(r.data) - r.pos }

func (r *wireReader) readVarint() (uint64, error) {
	var val uint64
	var shift uint
	for {
		if r.pos >= len(r.data) {
			return 0, io.ErrUnexpectedEOF
		}
		b := r.data[r.pos]
		r.pos++
		val |= uint64(b&0x7F) << shift
		if b < 0x80 {
			return val, nil
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varint overflow")
		}
	}
}

func (r *wireReader) readFixed64() (uint64, error) {
	if r.pos+8 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	val := binary.LittleEndian.Uint64(r.data[r.pos : r.pos+8])
	r.pos += 8
	return val, nil
}

func (r *wireReader) readFixed32() (uint32, error) {
	if r.pos+4 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	val := binary.LittleEndian.Uint32(r.data[r.pos : r.pos+4])
	r.pos += 4
	return val, nil
}

func (r *wireReader) readBytes() ([]byte, error) {
	length, err := r.readVarint()
	if err != nil {
		return nil, err
	}
	if r.pos+int(length) > len(r.data) {
		return nil, io.ErrUnexpectedEOF
	}
	b := r.data[r.pos : r.pos+int(length)]
	r.pos += int(length)
	return b, nil
}

func (r *wireReader) readString() (string, error) {
	b, err := r.readBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (r *wireReader) skipField(wireType uint32) error {
	switch wireType {
	case 0: // varint
		_, err := r.readVarint()
		return err
	case 1: // fixed64
		_, err := r.readFixed64()
		return err
	case 2: // length-delimited
		_, err := r.readBytes()
		return err
	case 5: // fixed32
		_, err := r.readFixed32()
		return err
	default:
		return fmt.Errorf("unknown wire type %d", wireType)
	}
}

// --- Response unmarshalers ---

func unmarshalExecuteSqlResponse(data []byte) (*PBExecuteSqlResponse, error) {
	r := &wireReader{data: data}
	resp := &PBExecuteSqlResponse{}
	for r.remaining() > 0 {
		tag, err := r.readVarint()
		if err != nil {
			return nil, err
		}
		field := uint32(tag >> 3)
		wireType := uint32(tag & 0x7)
		switch field {
		case 1: // columns
			b, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			col, err := unmarshalColumn(b)
			if err != nil {
				return nil, err
			}
			resp.Columns = append(resp.Columns, col)
		case 2: // rows
			b, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			row, err := unmarshalRow(b)
			if err != nil {
				return nil, err
			}
			resp.Rows = append(resp.Rows, row)
		case 3: // rows_affected
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			resp.RowsAffected = int64(v)
		case 4: // error
			s, err := r.readString()
			if err != nil {
				return nil, err
			}
			resp.Error = s
		default:
			if err := r.skipField(wireType); err != nil {
				return nil, err
			}
		}
	}
	return resp, nil
}

func unmarshalColumn(data []byte) (*PBColumn, error) {
	r := &wireReader{data: data}
	col := &PBColumn{}
	for r.remaining() > 0 {
		tag, err := r.readVarint()
		if err != nil {
			return nil, err
		}
		field := uint32(tag >> 3)
		wireType := uint32(tag & 0x7)
		switch field {
		case 1:
			col.Name, err = r.readString()
		case 2:
			col.Type, err = r.readString()
		default:
			err = r.skipField(wireType)
		}
		if err != nil {
			return nil, err
		}
	}
	return col, nil
}

func unmarshalRow(data []byte) (*PBRow, error) {
	r := &wireReader{data: data}
	row := &PBRow{}
	for r.remaining() > 0 {
		tag, err := r.readVarint()
		if err != nil {
			return nil, err
		}
		field := uint32(tag >> 3)
		wireType := uint32(tag & 0x7)
		switch field {
		case 1:
			b, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			val, err := unmarshalValue(b)
			if err != nil {
				return nil, err
			}
			row.Values = append(row.Values, val)
		default:
			if err := r.skipField(wireType); err != nil {
				return nil, err
			}
		}
	}
	return row, nil
}

func unmarshalValue(data []byte) (*PBValue, error) {
	r := &wireReader{data: data}
	val := &PBValue{}
	for r.remaining() > 0 {
		tag, err := r.readVarint()
		if err != nil {
			return nil, err
		}
		field := uint32(tag >> 3)
		wireType := uint32(tag & 0x7)
		switch field {
		case 1: // int_value
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			val.Kind = &PBValue_IntValue{IntValue: int64(v)}
		case 2: // float_value
			v, err := r.readFixed64()
			if err != nil {
				return nil, err
			}
			val.Kind = &PBValue_FloatValue{FloatValue: math.Float64frombits(v)}
		case 3: // string_value
			s, err := r.readString()
			if err != nil {
				return nil, err
			}
			val.Kind = &PBValue_StringValue{StringValue: s}
		case 4: // bool_value
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			val.Kind = &PBValue_BoolValue{BoolValue: v != 0}
		case 5: // null_value
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			val.Kind = &PBValue_NullValue{NullValue: v != 0}
		case 6: // bytes_value
			b, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			val.Kind = &PBValue_BytesValue{BytesValue: b}
		default:
			if err := r.skipField(wireType); err != nil {
				return nil, err
			}
		}
	}
	return val, nil
}

func unmarshalBatchExecuteResponse(data []byte) (*PBBatchExecuteResponse, error) {
	r := &wireReader{data: data}
	resp := &PBBatchExecuteResponse{}
	for r.remaining() > 0 {
		tag, err := r.readVarint()
		if err != nil {
			return nil, err
		}
		field := uint32(tag >> 3)
		wireType := uint32(tag & 0x7)
		switch field {
		case 1:
			b, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			sub, err := unmarshalExecuteSqlResponse(b)
			if err != nil {
				return nil, err
			}
			resp.Results = append(resp.Results, sub)
		default:
			if err := r.skipField(wireType); err != nil {
				return nil, err
			}
		}
	}
	return resp, nil
}

func unmarshalHealthResponse(data []byte) (*PBHealthResponse, error) {
	r := &wireReader{data: data}
	resp := &PBHealthResponse{}
	for r.remaining() > 0 {
		tag, err := r.readVarint()
		if err != nil {
			return nil, err
		}
		field := uint32(tag >> 3)
		wireType := uint32(tag & 0x7)
		switch field {
		case 1:
			resp.Status, err = r.readString()
		case 2:
			resp.Version, err = r.readString()
		default:
			err = r.skipField(wireType)
		}
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func unmarshalQueryResponse(data []byte) (*PBQueryResponse, error) {
	r := &wireReader{data: data}
	resp := &PBQueryResponse{}
	for r.remaining() > 0 {
		tag, err := r.readVarint()
		if err != nil {
			return nil, err
		}
		field := uint32(tag >> 3)
		wireType := uint32(tag & 0x7)
		switch field {
		case 1: // status
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			resp.Status = PBStatus(v)
		case 2: // result_set
			b, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			rs, err := unmarshalResultSet(b)
			if err != nil {
				return nil, err
			}
			resp.ResultSet = rs
		case 3: // stats
			b, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			st, err := unmarshalQueryStats(b)
			if err != nil {
				return nil, err
			}
			resp.Stats = st
		case 4: // next_page_token
			resp.NextPageToken, err = r.readBytes()
		default:
			err = r.skipField(wireType)
		}
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func unmarshalResultSet(data []byte) (*PBResultSet, error) {
	r := &wireReader{data: data}
	rs := &PBResultSet{}
	for r.remaining() > 0 {
		tag, err := r.readVarint()
		if err != nil {
			return nil, err
		}
		field := uint32(tag >> 3)
		wireType := uint32(tag & 0x7)
		switch field {
		case 1:
			b, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			cm, err := unmarshalColumnMeta(b)
			if err != nil {
				return nil, err
			}
			rs.Columns = append(rs.Columns, cm)
		case 2:
			b, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			qr, err := unmarshalQueryRow(b)
			if err != nil {
				return nil, err
			}
			rs.Rows = append(rs.Rows, qr)
		default:
			if err := r.skipField(wireType); err != nil {
				return nil, err
			}
		}
	}
	return rs, nil
}

func unmarshalColumnMeta(data []byte) (*PBColumnMeta, error) {
	r := &wireReader{data: data}
	cm := &PBColumnMeta{}
	for r.remaining() > 0 {
		tag, err := r.readVarint()
		if err != nil {
			return nil, err
		}
		field := uint32(tag >> 3)
		wireType := uint32(tag & 0x7)
		switch field {
		case 1:
			cm.Name, err = r.readString()
		case 2:
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			cm.DataType = PBDataType(v)
		case 3:
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			cm.Nullable = v != 0
		default:
			err = r.skipField(wireType)
		}
		if err != nil {
			return nil, err
		}
	}
	return cm, nil
}

func unmarshalQueryRow(data []byte) (*PBQueryRow, error) {
	r := &wireReader{data: data}
	qr := &PBQueryRow{}
	for r.remaining() > 0 {
		tag, err := r.readVarint()
		if err != nil {
			return nil, err
		}
		field := uint32(tag >> 3)
		wireType := uint32(tag & 0x7)
		switch field {
		case 1:
			b, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			qr.Values = append(qr.Values, b)
		default:
			if err := r.skipField(wireType); err != nil {
				return nil, err
			}
		}
	}
	return qr, nil
}

func unmarshalQueryStats(data []byte) (*PBQueryStats, error) {
	r := &wireReader{data: data}
	st := &PBQueryStats{}
	for r.remaining() > 0 {
		tag, err := r.readVarint()
		if err != nil {
			return nil, err
		}
		field := uint32(tag >> 3)
		wireType := uint32(tag & 0x7)
		switch field {
		case 1:
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			st.RowsScanned = v
		case 2:
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			st.RowsReturned = v
		case 3:
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			st.ExecutionTimeUs = v
		case 4:
			st.QueryPlan, err = r.readString()
		default:
			err = r.skipField(wireType)
		}
		if err != nil {
			return nil, err
		}
	}
	return st, nil
}

func unmarshalExecuteResponse(data []byte) (*PBExecuteResponse, error) {
	r := &wireReader{data: data}
	resp := &PBExecuteResponse{}
	for r.remaining() > 0 {
		tag, err := r.readVarint()
		if err != nil {
			return nil, err
		}
		field := uint32(tag >> 3)
		wireType := uint32(tag & 0x7)
		switch field {
		case 1:
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			resp.Status = PBStatus(v)
		case 2:
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			resp.RowsAffected = v
		case 3:
			b, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			ts, err := unmarshalTimestamp(b)
			if err != nil {
				return nil, err
			}
			resp.Timestamp = ts
		default:
			if err := r.skipField(wireType); err != nil {
				return nil, err
			}
		}
	}
	return resp, nil
}

func unmarshalTimestamp(data []byte) (*PBTimestamp, error) {
	r := &wireReader{data: data}
	ts := &PBTimestamp{}
	for r.remaining() > 0 {
		tag, err := r.readVarint()
		if err != nil {
			return nil, err
		}
		field := uint32(tag >> 3)
		wireType := uint32(tag & 0x7)
		switch field {
		case 1:
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			ts.Physical = v
		case 2:
			v, err := r.readVarint()
			if err != nil {
				return nil, err
			}
			ts.Logical = uint32(v)
		default:
			if err := r.skipField(wireType); err != nil {
				return nil, err
			}
		}
	}
	return ts, nil
}

// ---------------------------------------------------------------------------
// Custom codec that uses our hand-written marshal/unmarshal
// ---------------------------------------------------------------------------

// rawCodec implements grpc encoding.Codec, passing pre-serialized bytes
// directly to/from the wire. This lets us use our hand-written marshalers
// without requiring proto.Message implementations for every type.
type rawCodec struct{}

func (rawCodec) Marshal(v interface{}) ([]byte, error) {
	if b, ok := v.(*rawBytes); ok {
		return b.data, nil
	}
	return nil, fmt.Errorf("rawCodec: cannot marshal %T", v)
}

func (rawCodec) Unmarshal(data []byte, v interface{}) error {
	if b, ok := v.(*rawBytes); ok {
		b.data = append(b.data[:0], data...)
		return nil
	}
	return fmt.Errorf("rawCodec: cannot unmarshal into %T", v)
}

func (rawCodec) Name() string { return "proto" }

// rawBytes is a container for pre-serialized protobuf bytes.
type rawBytes struct {
	data []byte
}

// ---------------------------------------------------------------------------
// ParticleDB service client
// ---------------------------------------------------------------------------

type particleDBServiceClient struct {
	cc grpc.ClientConnInterface
}

func newParticleDBServiceClient(cc grpc.ClientConnInterface) particleDBServiceClient {
	return particleDBServiceClient{cc: cc}
}

func (c particleDBServiceClient) ExecuteSQL(ctx context.Context, req *PBExecuteSqlRequest) (*PBExecuteSqlResponse, error) {
	data, err := marshalExecuteSqlRequest(req)
	if err != nil {
		return nil, err
	}
	out := &rawBytes{}
	err = c.cc.Invoke(ctx, "/particledb.v1.ParticleDB/ExecuteSQL", &rawBytes{data: data}, out, grpc.ForceCodec(rawCodec{}))
	if err != nil {
		return nil, err
	}
	return unmarshalExecuteSqlResponse(out.data)
}

func (c particleDBServiceClient) BatchExecute(ctx context.Context, req *PBBatchExecuteRequest) (*PBBatchExecuteResponse, error) {
	data, err := marshalBatchExecuteRequest(req)
	if err != nil {
		return nil, err
	}
	out := &rawBytes{}
	err = c.cc.Invoke(ctx, "/particledb.v1.ParticleDB/BatchExecute", &rawBytes{data: data}, out, grpc.ForceCodec(rawCodec{}))
	if err != nil {
		return nil, err
	}
	return unmarshalBatchExecuteResponse(out.data)
}

func (c particleDBServiceClient) Health(ctx context.Context, req *PBHealthRequest) (*PBHealthResponse, error) {
	data, err := marshalHealthRequest(req)
	if err != nil {
		return nil, err
	}
	out := &rawBytes{}
	err = c.cc.Invoke(ctx, "/particledb.v1.ParticleDB/Health", &rawBytes{data: data}, out, grpc.ForceCodec(rawCodec{}))
	if err != nil {
		return nil, err
	}
	return unmarshalHealthResponse(out.data)
}

// ---------------------------------------------------------------------------
// QueryService client
// ---------------------------------------------------------------------------

type queryServiceClient struct {
	cc grpc.ClientConnInterface
}

func newQueryServiceClient(cc grpc.ClientConnInterface) queryServiceClient {
	return queryServiceClient{cc: cc}
}

func (c queryServiceClient) Query(ctx context.Context, req *PBQueryRequest) (*PBQueryResponse, error) {
	data, err := marshalQueryRequest(req)
	if err != nil {
		return nil, err
	}
	out := &rawBytes{}
	err = c.cc.Invoke(ctx, "/spanner.query.QueryService/Query", &rawBytes{data: data}, out, grpc.ForceCodec(rawCodec{}))
	if err != nil {
		return nil, err
	}
	return unmarshalQueryResponse(out.data)
}

func (c queryServiceClient) QueryStream(ctx context.Context, req *PBQueryRequest) (queryStreamClient, error) {
	data, err := marshalQueryRequest(req)
	if err != nil {
		return queryStreamClient{}, err
	}
	desc := &grpc.StreamDesc{
		StreamName:    "QueryStream",
		ServerStreams: true,
	}
	stream, err := c.cc.NewStream(ctx, desc, "/spanner.query.QueryService/QueryStream", grpc.ForceCodec(rawCodec{}))
	if err != nil {
		return queryStreamClient{}, err
	}
	if err := stream.SendMsg(&rawBytes{data: data}); err != nil {
		return queryStreamClient{}, err
	}
	if err := stream.CloseSend(); err != nil {
		return queryStreamClient{}, err
	}
	return queryStreamClient{stream: stream}, nil
}

func (c queryServiceClient) Execute(ctx context.Context, req *PBExecuteRequest) (*PBExecuteResponse, error) {
	data, err := marshalExecuteRequest(req)
	if err != nil {
		return nil, err
	}
	out := &rawBytes{}
	err = c.cc.Invoke(ctx, "/spanner.query.QueryService/Execute", &rawBytes{data: data}, out, grpc.ForceCodec(rawCodec{}))
	if err != nil {
		return nil, err
	}
	return unmarshalExecuteResponse(out.data)
}

// queryStreamClient wraps a grpc.ClientStream for the QueryStream RPC.
type queryStreamClient struct {
	stream grpc.ClientStream
}

// Recv reads the next QueryResponse from the stream.
func (s queryStreamClient) Recv() (*PBQueryResponse, error) {
	out := &rawBytes{}
	if err := s.stream.RecvMsg(out); err != nil {
		return nil, err
	}
	return unmarshalQueryResponse(out.data)
}

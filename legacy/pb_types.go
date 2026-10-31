package legacy

// pb_types.go contains hand-written Go types that correspond to the protobuf
// message definitions in proto/particledb.proto, proto/query.proto, and
// proto/common.proto. These are used by the gRPC client until code generation
// via protoc is set up.
//
// To regenerate from proto files instead, install protoc-gen-go and
// protoc-gen-go-grpc, then run:
//
//   protoc --go_out=. --go-grpc_out=. \
//       -I proto proto/common.proto proto/particledb.proto proto/query.proto

// Note: these types are hand-written equivalents of what protoc-gen-go would
// generate. They do not implement proto.Message but are serialized using the
// custom rawCodec in grpc_service.go.

// ---------------------------------------------------------------------------
// common.proto types
// ---------------------------------------------------------------------------

// PBTimestamp corresponds to spanner.common.Timestamp.
type PBTimestamp struct {
	Physical uint64 `protobuf:"varint,1,opt,name=physical,proto3" json:"physical,omitempty"`
	Logical  uint32 `protobuf:"varint,2,opt,name=logical,proto3" json:"logical,omitempty"`
}

// PBTxnId corresponds to spanner.common.TxnId.
type PBTxnId struct {
	Id []byte `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
}

// PBStatus mirrors spanner.common.Status.
type PBStatus int32

const (
	PBStatusOK               PBStatus = 0
	PBStatusError            PBStatus = 1
	PBStatusNotFound         PBStatus = 2
	PBStatusConflict         PBStatus = 3
	PBStatusTimeout          PBStatus = 4
	PBStatusAlreadyExists    PBStatus = 5
	PBStatusInvalidArgument  PBStatus = 6
	PBStatusUnavailable      PBStatus = 7
	PBStatusAborted          PBStatus = 8
	PBStatusOutOfRange       PBStatus = 9
	PBStatusInternal         PBStatus = 10
	PBStatusPermissionDenied PBStatus = 11
	PBStatusUnauthenticated  PBStatus = 12
)

// ---------------------------------------------------------------------------
// particledb.proto types
// ---------------------------------------------------------------------------

// PBValue corresponds to particledb.v1.Value.
type PBValue struct {
	Kind isPBValue_Kind
}

type isPBValue_Kind interface {
	isPBValue_Kind()
}

// PBValue_IntValue wraps an int64 value.
type PBValue_IntValue struct {
	IntValue int64
}

// PBValue_FloatValue wraps a float64 value.
type PBValue_FloatValue struct {
	FloatValue float64
}

// PBValue_StringValue wraps a string value.
type PBValue_StringValue struct {
	StringValue string
}

// PBValue_BoolValue wraps a bool value.
type PBValue_BoolValue struct {
	BoolValue bool
}

// PBValue_NullValue represents SQL NULL.
type PBValue_NullValue struct {
	NullValue bool
}

// PBValue_BytesValue wraps a byte slice.
type PBValue_BytesValue struct {
	BytesValue []byte
}

func (*PBValue_IntValue) isPBValue_Kind()    {}
func (*PBValue_FloatValue) isPBValue_Kind()  {}
func (*PBValue_StringValue) isPBValue_Kind() {}
func (*PBValue_BoolValue) isPBValue_Kind()   {}
func (*PBValue_NullValue) isPBValue_Kind()   {}
func (*PBValue_BytesValue) isPBValue_Kind()  {}

// PBColumn corresponds to particledb.v1.Column.
type PBColumn struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Type string `protobuf:"bytes,2,opt,name=type,proto3" json:"type,omitempty"`
}

// PBRow corresponds to particledb.v1.Row.
type PBRow struct {
	Values []*PBValue `protobuf:"bytes,1,rep,name=values,proto3" json:"values,omitempty"`
}

// PBExecuteSqlRequest corresponds to particledb.v1.ExecuteSqlRequest.
type PBExecuteSqlRequest struct {
	Sql    string     `protobuf:"bytes,1,opt,name=sql,proto3" json:"sql,omitempty"`
	Params []*PBValue `protobuf:"bytes,2,rep,name=params,proto3" json:"params,omitempty"`
}

// PBExecuteSqlResponse corresponds to particledb.v1.ExecuteSqlResponse.
type PBExecuteSqlResponse struct {
	Columns      []*PBColumn `protobuf:"bytes,1,rep,name=columns,proto3" json:"columns,omitempty"`
	Rows         []*PBRow    `protobuf:"bytes,2,rep,name=rows,proto3" json:"rows,omitempty"`
	RowsAffected int64       `protobuf:"varint,3,opt,name=rows_affected,proto3" json:"rows_affected,omitempty"`
	Error        string      `protobuf:"bytes,4,opt,name=error,proto3" json:"error,omitempty"`
}

// PBBatchExecuteRequest corresponds to particledb.v1.BatchExecuteRequest.
type PBBatchExecuteRequest struct {
	Statements []string `protobuf:"bytes,1,rep,name=statements,proto3" json:"statements,omitempty"`
}

// PBBatchExecuteResponse corresponds to particledb.v1.BatchExecuteResponse.
type PBBatchExecuteResponse struct {
	Results []*PBExecuteSqlResponse `protobuf:"bytes,1,rep,name=results,proto3" json:"results,omitempty"`
}

// PBInsertRowRequest corresponds to particledb.v1.InsertRowRequest.
type PBInsertRowRequest struct {
	Table   string     `protobuf:"bytes,1,opt,name=table,proto3" json:"table,omitempty"`
	Columns []string   `protobuf:"bytes,2,rep,name=columns,proto3" json:"columns,omitempty"`
	Values  []*PBValue `protobuf:"bytes,3,rep,name=values,proto3" json:"values,omitempty"`
}

// PBInsertResult corresponds to particledb.v1.InsertResult.
type PBInsertResult struct {
	RowsInserted int64  `protobuf:"varint,1,opt,name=rows_inserted,proto3" json:"rows_inserted,omitempty"`
	Error        string `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`
}

// PBSubscribeRequest corresponds to particledb.v1.SubscribeRequest.
type PBSubscribeRequest struct {
	Table  string   `protobuf:"bytes,1,opt,name=table,proto3" json:"table,omitempty"`
	Events []string `protobuf:"bytes,2,rep,name=events,proto3" json:"events,omitempty"`
}

// PBChangeEvent corresponds to particledb.v1.ChangeEvent.
type PBChangeEvent struct {
	Event   string `protobuf:"bytes,1,opt,name=event,proto3" json:"event,omitempty"`
	Table   string `protobuf:"bytes,2,opt,name=table,proto3" json:"table,omitempty"`
	Rows    int64  `protobuf:"varint,3,opt,name=rows,proto3" json:"rows,omitempty"`
	Payload string `protobuf:"bytes,4,opt,name=payload,proto3" json:"payload,omitempty"`
}

// PBHealthRequest corresponds to particledb.v1.HealthRequest.
type PBHealthRequest struct{}

// PBHealthResponse corresponds to particledb.v1.HealthResponse.
type PBHealthResponse struct {
	Status  string `protobuf:"bytes,1,opt,name=status,proto3" json:"status,omitempty"`
	Version string `protobuf:"bytes,2,opt,name=version,proto3" json:"version,omitempty"`
}

// ---------------------------------------------------------------------------
// query.proto types
// ---------------------------------------------------------------------------

// PBQueryMode mirrors spanner.query.QueryMode.
type PBQueryMode int32

const (
	PBQueryModeNormal  PBQueryMode = 0
	PBQueryModePlan    PBQueryMode = 1
	PBQueryModeProfile PBQueryMode = 2
)

// PBDataType mirrors spanner.query.DataType.
type PBDataType int32

const (
	PBDataTypeUnknown   PBDataType = 0
	PBDataTypeBool      PBDataType = 1
	PBDataTypeInt64     PBDataType = 2
	PBDataTypeFloat64   PBDataType = 3
	PBDataTypeString    PBDataType = 4
	PBDataTypeBytes     PBDataType = 5
	PBDataTypeTimestamp PBDataType = 6
	PBDataTypeNull      PBDataType = 7
)

// PBColumnMeta corresponds to spanner.query.ColumnMeta.
type PBColumnMeta struct {
	Name     string     `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	DataType PBDataType `protobuf:"varint,2,opt,name=data_type,proto3,enum=PBDataType" json:"data_type,omitempty"`
	Nullable bool       `protobuf:"varint,3,opt,name=nullable,proto3" json:"nullable,omitempty"`
}

// PBQueryRow corresponds to spanner.query.Row (values as raw bytes).
type PBQueryRow struct {
	Values [][]byte `protobuf:"bytes,1,rep,name=values,proto3" json:"values,omitempty"`
}

// PBResultSet corresponds to spanner.query.ResultSet.
type PBResultSet struct {
	Columns []*PBColumnMeta `protobuf:"bytes,1,rep,name=columns,proto3" json:"columns,omitempty"`
	Rows    []*PBQueryRow   `protobuf:"bytes,2,rep,name=rows,proto3" json:"rows,omitempty"`
}

// PBQueryStats corresponds to spanner.query.QueryStats.
type PBQueryStats struct {
	RowsScanned     uint64 `protobuf:"varint,1,opt,name=rows_scanned,proto3" json:"rows_scanned,omitempty"`
	RowsReturned    uint64 `protobuf:"varint,2,opt,name=rows_returned,proto3" json:"rows_returned,omitempty"`
	ExecutionTimeUs uint64 `protobuf:"varint,3,opt,name=execution_time_us,proto3" json:"execution_time_us,omitempty"`
	QueryPlan       string `protobuf:"bytes,4,opt,name=query_plan,proto3" json:"query_plan,omitempty"`
}

// PBQueryRequest corresponds to spanner.query.QueryRequest.
type PBQueryRequest struct {
	Sql       string      `protobuf:"bytes,1,opt,name=sql,proto3" json:"sql,omitempty"`
	TxnId     *PBTxnId    `protobuf:"bytes,2,opt,name=txn_id,proto3" json:"txn_id,omitempty"`
	Mode      PBQueryMode `protobuf:"varint,3,opt,name=mode,proto3,enum=PBQueryMode" json:"mode,omitempty"`
	MaxRows   uint64      `protobuf:"varint,4,opt,name=max_rows,proto3" json:"max_rows,omitempty"`
	PageToken []byte      `protobuf:"bytes,5,opt,name=page_token,proto3" json:"page_token,omitempty"`
	Params    [][]byte    `protobuf:"bytes,6,rep,name=params,proto3" json:"params,omitempty"`
}

// PBQueryResponse corresponds to spanner.query.QueryResponse.
type PBQueryResponse struct {
	Status        PBStatus     `protobuf:"varint,1,opt,name=status,proto3,enum=PBStatus" json:"status,omitempty"`
	ResultSet     *PBResultSet `protobuf:"bytes,2,opt,name=result_set,proto3" json:"result_set,omitempty"`
	Stats         *PBQueryStats `protobuf:"bytes,3,opt,name=stats,proto3" json:"stats,omitempty"`
	NextPageToken []byte       `protobuf:"bytes,4,opt,name=next_page_token,proto3" json:"next_page_token,omitempty"`
}

// PBExecuteRequest corresponds to spanner.query.ExecuteRequest.
type PBExecuteRequest struct {
	Sql    string   `protobuf:"bytes,1,opt,name=sql,proto3" json:"sql,omitempty"`
	TxnId  *PBTxnId `protobuf:"bytes,2,opt,name=txn_id,proto3" json:"txn_id,omitempty"`
	Params [][]byte `protobuf:"bytes,3,rep,name=params,proto3" json:"params,omitempty"`
}

// PBExecuteResponse corresponds to spanner.query.ExecuteResponse.
type PBExecuteResponse struct {
	Status       PBStatus     `protobuf:"varint,1,opt,name=status,proto3,enum=PBStatus" json:"status,omitempty"`
	RowsAffected uint64       `protobuf:"varint,2,opt,name=rows_affected,proto3" json:"rows_affected,omitempty"`
	Timestamp    *PBTimestamp `protobuf:"bytes,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
}

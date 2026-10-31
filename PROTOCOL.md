# ParticleDB native wire: notes for this client

A short description of the native wire protocol as this package implements it
(`wire.go`, `mux.go`, `params.go`). The JDBC driver
(https://github.com/particledb-ai/particledb-jdbc, `WireV2Codec`) is the
reference client. User documentation: https://particledb.ai/docs/.

## Framing

The connection opens with HELLO / HELLO_OK in the 8-byte protocol 1 header:

    [u32 LE payload length][u8 0][u8 0][u8 type][u8 flags]

Every later frame, in both directions, uses the 11-byte protocol 2 header:

    [u8 version=2][u8 type][u8 flags][u32 LE stream id][u32 LE payload length]

A payload is at most 16 MiB. Requests carry a client-chosen stream id; the
server answers on the same id, so many requests share one TCP connection.

## Handshake

HELLO carries the requested protocol version (2), a capability byte, the result
format (Arrow IPC, `0x01`), an extension capability byte and, optionally,
session parameters. HELLO_OK returns the negotiated version, capabilities,
format, the maximum number of concurrent streams and the flow-control window.
A server that answers with protocol 1, or without Arrow IPC, is refused.

## Frame types

| Client -> server | Code | Server -> client | Code |
|---|---|---|---|
| HELLO | 0x00 | HELLO_OK | 0x80 |
| QUERY | 0x01 | ARROW_RESULT | 0x81 |
| BATCH | 0x02 | OK | 0x82 |
| PING | 0x03 | ERROR | 0x83 |
| CLOSE | 0x04 | PONG | 0x84 |
| RST_STREAM | 0x06 | BATCH_RESULT | 0x85 |
| AUTH | 0x09 | ARROW_STREAM (chunked result) | 0x86 |
| KV_SET / KV_GET / KV_DEL / KV_INCR | 0x10-0x13 | KV_BULK / KV_NIL | 0x87 / 0x88 |
| PREPARE / EXECUTE / CLOSE_STMT | 0x20-0x22 | RST_STREAM / GOAWAY | 0x89 / 0x8a |
| BULK (begin / data / end / abort) | 0x40 | SETTINGS_ACK / NOTICE / PREPARE_OK | 0x8b-0x8d |

Frame flags: END_STREAM `0x01`, MORE_CHUNKS `0x02`, TXN_BEGIN `0x08`,
TXN_OPEN `0x10`, TXN_FAILED `0x20`, TXN_ROLLED_BACK `0x40` (a COMMIT that ended
a failed transaction by rolling it back: nothing was committed).

## Typed parameters

EXECUTE parameters are tagged values: 0 NULL, 1 int8, 2 text, 3 bool,
4 float8, 5 float4, 6 numeric, 7 date, 8 timestamp, 9 timestamptz, 10 time,
11 uuid, 12 bytea, 13 int2, 14 int4, 15 json, 16 jsonb, 17 array.

Results arrive as Arrow IPC streams. Errors carry a SQLSTATE.

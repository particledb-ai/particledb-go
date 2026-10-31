#!/usr/bin/env bash
# Runs the ParticleDB SDK conformance suite against this Go SDK.
#
#   ./run_conformance.sh [--mode default|strict] [--results FILE]
#
# With no environment set, the suite runs against a server that is already
# listening on 127.0.0.1 at the default port (7173). Start that server in the
# matching mode yourself (`--mode strict` expects --concurrency-control strict-optimistic
# --durability sync). The cases that kill and restart the server are
# skipped, because the runner does not own it.
#
# Environment:
#   PDB_BIN               server binary: start a fresh server for the run
#                         (on the default port when it is free) and run every
#                         case, including the crash cases
#   PDB_CONF_HOST         host of a running server (default 127.0.0.1)
#   PDB_CONF_PORT         port of a running server (default: none, so the
#                         SDK's default port is used)
#   PDB_CONF_RESP_PORT    RESP port of a running server (default 6379)
#   PDB_CONF_CONTROL      optional hook for a running server: called with
#                         `kill9` or `restart`
#   PDB_CONF_ONLY         debugging: comma-separated case ids to run
#
# The runner builds the SDK and the adapter from source, runs every case for
# the mode and validates the results file (every case reported, every skip
# has a reason). It exits 0 only if no case failed and the results file is
# valid.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE="$(cd "$HERE/.." && pwd)"
# The server's default database port. With PDB_BIN set it is read from the
# binary instead.
DOCUMENTED_DEFAULT_PORT=7173
MODE=default
RESULTS=""
while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODE=$2; shift 2 ;;
    --results) RESULTS=$2; shift 2 ;;
    -h|--help) sed -n '2,27p' "$0"; exit 0 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
case "$MODE" in default|strict) ;; *) echo "unknown mode '$MODE' (default|strict)" >&2; exit 2 ;; esac

WORK=$(mktemp -d "${TMPDIR:-/tmp}/pdb-conf-go.XXXXXX")
[ -n "$RESULTS" ] || RESULTS="$WORK/results.json"
STARTED=0
cleanup() {
  if [ $STARTED -eq 1 ]; then "$HERE/lib/server.sh" stop "$WORK/server" || true; fi
}
trap cleanup EXIT INT TERM

# Build first, so a compile error never leaves a server behind.
( cd "$MODULE" && go build -o "$WORK/adapter" ./conformance/adapter ) || { echo "adapter build failed" >&2; exit 2; }

if [ -n "${PDB_BIN:-}" ]; then
  "$HERE/lib/server.sh" start "$WORK/server" "$PDB_BIN" "$MODE" || exit 1
  STARTED=1
  eval "$("$HERE/lib/server.sh" env "$WORK/server")"
  printf '#!/bin/sh\nexec "%s" "$1" "%s"\n' "$HERE/lib/server.sh" "$WORK/server" > "$WORK/control"
  chmod +x "$WORK/control"
  export PDB_CONF_CONTROL="$WORK/control"
  echo "server: $PDB_BIN on port $PDB_CONF_PORT (default port $PDB_CONF_DEFAULT_PORT), mode $MODE, log $WORK/server/server.log"
  # On the default port the adapter connects without naming a port.
  if [ "$PDB_CONF_PORT" = "$PDB_CONF_DEFAULT_PORT" ]; then unset PDB_CONF_PORT; fi
else
  export PDB_CONF_HOST=${PDB_CONF_HOST:-127.0.0.1}
  export PDB_CONF_DEFAULT_PORT=${PDB_CONF_DEFAULT_PORT:-$DOCUMENTED_DEFAULT_PORT}
  export PDB_CONF_RESP_PORT=${PDB_CONF_RESP_PORT:-6379}
  if ! nc -z "$PDB_CONF_HOST" "${PDB_CONF_PORT:-$PDB_CONF_DEFAULT_PORT}" 2>/dev/null; then
    echo "no server on $PDB_CONF_HOST:${PDB_CONF_PORT:-$PDB_CONF_DEFAULT_PORT}; start one or set PDB_BIN" >&2
    exit 2
  fi
  echo "server: running server on $PDB_CONF_HOST:${PDB_CONF_PORT:-$PDB_CONF_DEFAULT_PORT (default port)}, mode $MODE"
fi
export PDB_CONF_MODE=$MODE PDB_CONF_CASES_DIR="$HERE/cases" PDB_CONF_RESULTS="$RESULTS" PDB_CONF_IDS="$WORK/ids"

python3 "$HERE/lib/conformance.py" ids "$HERE/cases" "$MODE" > "$WORK/ids" || exit 2

"$WORK/adapter"
rc=$?
python3 "$HERE/lib/conformance.py" validate "$HERE/cases" "$MODE" "$RESULTS" || rc=1
python3 "$HERE/lib/conformance.py" table "$HERE/cases" "$RESULTS" > /dev/null || true
echo "results: $RESULTS"
exit $rc

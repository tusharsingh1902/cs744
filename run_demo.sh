#!/bin/bash
#
# End-to-end demo: start PostgreSQL, build, run the server, exercise every
# endpoint, then shut down gracefully.
#
set -euo pipefail

DB_NAME=${DB_NAME:-kvstore}
BASE=${BASE:-http://127.0.0.1:8080}

echo "=============================================="
echo "  Multi-Tier Key-Value Store -- demo"
echo "=============================================="

# --- PostgreSQL ------------------------------------------------------------
if ! pg_isready -q 2>/dev/null; then
    echo "==> starting PostgreSQL"
    if [[ "$(uname)" == "Darwin" ]]; then
        brew services start postgresql@16 2>/dev/null || brew services start postgresql
    else
        sudo service postgresql start
    fi
    for _ in {1..15}; do pg_isready -q 2>/dev/null && break; sleep 1; done
fi
pg_isready -q || { echo "PostgreSQL did not come up"; exit 1; }
echo "==> PostgreSQL ready"

createdb "$DB_NAME" 2>/dev/null && echo "==> created database $DB_NAME" \
                                || echo "==> database $DB_NAME already exists"
# The schema is created by the server itself on startup (init_schema), so there
# is deliberately no CREATE TABLE here -- one source of truth.

# --- build -----------------------------------------------------------------
./build.sh

# --- run -------------------------------------------------------------------
CONN="host=127.0.0.1 port=5432 dbname=${DB_NAME} user=$(whoami)"
./server "$CONN" &
SERVER_PID=$!

# Always shut the server down, even if a curl below fails under `set -e`.
cleanup() {
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        echo
        echo "==> sending SIGTERM (graceful shutdown via self-pipe)"
        kill -TERM "$SERVER_PID"
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

for _ in {1..20}; do
    curl -sf "$BASE/metrics" >/dev/null 2>&1 && break
    sleep 0.25
done
echo "==> server up (pid $SERVER_PID)"

# --- exercise the API ------------------------------------------------------
echo
echo "--- PUT /kv/alpha ---"
curl -s -X PUT --data "first-value" "$BASE/kv/alpha"; echo

echo "--- GET /kv/alpha (cache hit) ---"
curl -s "$BASE/kv/alpha"; echo

echo "--- PUT /kv/alpha (overwrite) ---"
curl -s -X PUT --data "second-value" "$BASE/kv/alpha"; echo
curl -s "$BASE/kv/alpha"; echo

echo "--- GET /kv/missing (404) ---"
curl -s "$BASE/kv/missing"; echo

echo "--- persistence check straight from PostgreSQL ---"
psql "$DB_NAME" -c "SELECT key, value FROM kvstore WHERE key = 'alpha';"

echo "--- WAL contents (write-ahead: logged before the DB commit) ---"
tail -n 5 kvstore.wal || true

echo "--- DELETE /kv/alpha ---"
curl -s -X DELETE "$BASE/kv/alpha"; echo
curl -s "$BASE/kv/alpha"; echo

echo
echo "--- short load run: 4 clients, 10s, mixed ---"
./loadgen --clients 4 --duration 10 --warmup 2 --workload mixed --keyspace 500

echo
echo "--- /metrics ---"
curl -s "$BASE/metrics"

echo
echo "=============================================="
echo "  demo complete"
echo "=============================================="
#!/usr/bin/env bash
# Benchmarks tcpserver.php (phasync, JIT enabled) against node-server.js with wrk.
#
# Usage: benchmarking/run.sh ["100 900 10000"]
#
# Needs wrk and node. For more than ~1,000 connections phasync needs the phasync extension
# (run with vendor/bin/phasync, or pie install phasync/ext-phasync).
# For each run it also samples how many connections the server has actually accepted after
# 5 seconds: a server that leaves connections in the kernel queue serves fewer clients and
# looks faster than it is.
cd "$(dirname "$0")"
ulimit -n 65536
CONNS=${1:-"100 900 10000"}
PHP_FLAGS="-d opcache.enable_cli=1 -d opcache.jit=tracing -d opcache.jit_buffer_size=128M"
if [ -n "$PHASYNC_EXT_SO" ]; then
  PHP_FLAGS="$PHP_FLAGS -d extension=$PHASYNC_EXT_SO"
fi

php $PHP_FLAGS tcpserver.php 8080 > phasync.log 2>&1 &
PHP_PID=$!
node node-server.js 8081 > node.log 2>&1 &
NODE_PID=$!
trap 'kill $PHP_PID $NODE_PID 2>/dev/null' EXIT
sleep 1
cat phasync.log node.log

for c in $CONNS; do
  for s in "phasync:8080" "node:8081"; do
    name=${s%%:*}; port=${s##*:}
    out=$(mktemp)
    wrk -t8 -c"$c" -d10s --latency "http://127.0.0.1:$port/" > "$out" 2>&1 &
    W=$!
    sleep 5
    accepted=$(ss -Htn state established "( sport = :$port )" | wc -l)
    wait $W
    rps=$(awk '/Requests\/sec/{print $2}' "$out")
    p50=$(awk '$1=="50%"{print $2}' "$out")
    p99=$(awk '$1=="99%"{print $2}' "$out")
    max=$(awk '$1=="Latency"{print $4}' "$out")
    err=$(grep -o 'Socket errors.*' "$out")
    rm -f "$out"
    printf "%-8s c=%-6s %11s req/s  accepted@5s=%-6s p50=%-9s p99=%-9s max=%-8s %s\n" \
      "$name" "$c" "$rps" "$accepted" "$p50" "$p99" "$max" "$err"
    sleep 2
  done
done

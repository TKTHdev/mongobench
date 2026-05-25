#!/bin/bash
# Sweep the number of concurrent workers, at fixed key spaces. This is where
# document-level contention should finally become visible.
#
# For each keyspace in KEYSPACES, ramp -workers and record throughput:
#   keyspace=1        -> every worker UPSERTs the SAME document. As workers grow,
#                        WiredTiger document-level write conflicts/serialization
#                        kick in and throughput plateaus early.
#   keyspace=<large>  -> writes hit distinct documents (no contention), so
#                        throughput scales with workers until another ceiling
#                        (write tickets ~128 / CPU / disk).
#
# Overlay the two keyspace curves: the gap = the cost of document-level
# contention. (At low worker counts the benchmark is latency-bound and the two
# curves coincide -- that is why the earlier keyspace sweep looked flat.)
#
# Usage:
#   ./run_workers.sh
#   WORKERS_LIST="1 2 4 8 16 32 64 128 256 512" ./run_workers.sh
#   KEYSPACES="1 1000000" RECORDS=400000 ./run_workers.sh
set -euo pipefail

BIN=${BIN:-./mongobench}
URI=${URI:-mongodb://localhost:27017}
DB=${DB:-mongobench}
RECORDS=${RECORDS:-200000}
COLLECTIONS=${COLLECTIONS:-1}
KEYSPACES=${KEYSPACES:-"1 200000"}
WORKERS_LIST=${WORKERS_LIST:-"1 2 4 8 16 32 64 128 256"}
OUT=${OUT:-results_workers.csv}

if [ ! -x "$BIN" ]; then
    echo "building $BIN ..."
    go build -o "$BIN" .
fi

echo "collections,keyspace,split,workers,batch,ops,errors,elapsed_s,throughput_ops_s,call_avg_ms,call_p50_ms,call_p99_ms" > "$OUT"

for ks in $KEYSPACES; do
    for w in $WORKERS_LIST; do
        echo ">>> keyspace=$ks workers=$w"
        line=$("$BIN" \
            -uri "$URI" -db "$DB" \
            -collections "$COLLECTIONS" \
            -workers "$w" -records "$RECORDS" \
            -keyspace "$ks" -drop \
            | grep '^RESULT,')
        echo "$line" | sed 's/^RESULT,//' \
            | awk -F, '{for(i=1;i<=NF;i++){n=index($i,"=");printf "%s%s",(i>1?",":""),substr($i,n+1)} print ""}' >> "$OUT"
    done
done

echo
echo "=== workers sweep done -> $OUT ==="
column -t -s, "$OUT"
echo
echo "Tip: compare throughput_ops_s across the two keyspace blocks at each worker count."

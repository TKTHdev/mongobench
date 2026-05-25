#!/bin/bash
# Sweep the key-space size (number of distinct documents) while holding workers
# and op-count fixed. This is the document-level concurrency experiment:
#
#   narrow keyspace  -> many workers UPSERT the same few docs -> WiredTiger
#                       document-level write locks serialize them -> low tput
#   wide keyspace    -> writes hit distinct docs -> full parallelism -> high tput
#
# If throughput rises with keyspace, that *is* document-level locking in action
# (and explains why splitting collections didn't help: the lever is the doc).
#
# Usage:
#   ./run_keyspace.sh
#   WORKERS=32 RECORDS=200000 ./run_keyspace.sh
#   KEYSPACES="1 4 16 64 256 1024 100000" ./run_keyspace.sh
set -euo pipefail

BIN=${BIN:-./mongobench}
URI=${URI:-mongodb://localhost:27017}
DB=${DB:-mongobench}
WORKERS=${WORKERS:-16}
RECORDS=${RECORDS:-200000}
COLLECTIONS=${COLLECTIONS:-1}
KEYSPACES=${KEYSPACES:-"1 4 16 64 256 1024 10000 200000"}
OUT=${OUT:-results_keyspace.csv}

if [ ! -x "$BIN" ]; then
    echo "building $BIN ..."
    go build -o "$BIN" .
fi

echo "collections,keyspace,split,workers,batch,ops,errors,elapsed_s,throughput_ops_s,call_avg_ms,call_p50_ms,call_p99_ms" > "$OUT"

for ks in $KEYSPACES; do
    echo ">>> keyspace=$ks"
    line=$("$BIN" \
        -uri "$URI" -db "$DB" \
        -collections "$COLLECTIONS" \
        -workers "$WORKERS" -records "$RECORDS" \
        -keyspace "$ks" -drop \
        | grep '^RESULT,')
    echo "$line" | sed 's/^RESULT,//' \
        | awk -F, '{for(i=1;i<=NF;i++){n=index($i,"=");printf "%s%s",(i>1?",":""),substr($i,n+1)} print ""}' >> "$OUT"
done

echo
echo "=== keyspace sweep done -> $OUT ==="
column -t -s, "$OUT"

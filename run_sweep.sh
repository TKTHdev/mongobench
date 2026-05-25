#!/bin/bash
# Sweep the number of collections and record throughput, holding everything
# else fixed. This is the actual experiment: does splitting writes across more
# collections (on the same mongod) raise write throughput?
#
# Usage:
#   ./run_sweep.sh                       # synthetic data, default params
#   COLLECTIONS="1 2 4 8 16" ./run_sweep.sh
#   WORKERS=32 RECORDS=500000 ./run_sweep.sh
#   DATAFILE=./workData/workload.dat ./run_sweep.sh   # use real YCSB load data
set -euo pipefail

BIN=${BIN:-./mongobench}
URI=${URI:-mongodb://localhost:27017}
DB=${DB:-mongobench}
WORKERS=${WORKERS:-16}
RECORDS=${RECORDS:-200000}
BATCH=${BATCH:-1}
SPLIT=${SPLIT:-roundrobin}
DATAFILE=${DATAFILE:-}
COLLECTIONS=${COLLECTIONS:-"1 2 4 8 16"}
OUT=${OUT:-results.csv}

# Build if the binary is missing.
if [ ! -x "$BIN" ]; then
    echo "building $BIN ..."
    go build -o "$BIN" .
fi

data_flag=""
if [ -n "$DATAFILE" ]; then
    data_flag="-datafile $DATAFILE"
fi

echo "collections,keyspace,split,workers,batch,ops,errors,elapsed_s,throughput_ops_s,call_avg_ms,call_p50_ms,call_p99_ms" > "$OUT"

for k in $COLLECTIONS; do
    echo ">>> collections=$k"
    line=$("$BIN" \
        -uri "$URI" -db "$DB" \
        -collections "$k" -split "$SPLIT" \
        -workers "$WORKERS" -records "$RECORDS" -batch "$BATCH" \
        $data_flag -drop \
        | grep '^RESULT,')
    # strip "RESULT," then keep only the value after each "key=" (robust to
    # digits in key names like call_p50_ms).
    echo "$line" | sed 's/^RESULT,//' \
        | awk -F, '{for(i=1;i<=NF;i++){n=index($i,"=");printf "%s%s",(i>1?",":""),substr($i,n+1)} print ""}' >> "$OUT"
done

echo
echo "=== sweep done -> $OUT ==="
column -t -s, "$OUT"

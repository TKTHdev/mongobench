# mongobench

A standalone MongoDB **write-throughput** benchmark. No consensus, no
networking layer — just a MongoDB client hammering one `mongod` with concurrent
writes. It mirrors the YCSB data shape used by the `woc` repo.

**Question it answers:** does splitting writes across multiple collections (on
the same `mongod`) improve concurrent write throughput?

> Expectation: with the WiredTiger engine, concurrency control is per-document,
> so splitting one collection into many on the *same* server usually does **not**
> raise write throughput much — cache, write tickets, the journal and the disk
> are all instance-wide. This benchmark lets you confirm/measure that.

## Build

```bash
cd mongobench
go mod tidy      # fetches mongo-driver, writes go.sum (needs network once)
go build -o mongobench .
```

## Run a single config

```bash
# 200k synthetic INSERTs, 16 workers, all into ONE collection
./mongobench -collections 1 -workers 16 -records 200000

# same load spread across 8 collections
./mongobench -collections 8 -workers 16 -records 200000
```

Key flags:

| flag | default | meaning |
|------|---------|---------|
| `-uri` | `mongodb://localhost:27017` | MongoDB URI |
| `-db` | `mongobench` | database |
| `-coll` | `usertable` | base collection name (suffixed `_0`, `_1`, …) |
| `-collections` | `1` | **K**: how many collections to spread writes over |
| `-split` | `roundrobin` | `roundrobin` or `hash` (by key) collection assignment |
| `-workers` | `16` | concurrent write goroutines |
| `-records` | `200000` | synthetic doc count (ignored with `-datafile`) |
| `-fields` / `-fieldlen` | `10` / `100` | synthetic doc shape (≈1KB like YCSB) |
| `-batch` | `1` | `1`=InsertOne, `>1`=InsertMany (per collection) |
| `-datafile` | _(none)_ | load write ops from a YCSB `.dat` file instead |
| `-drop` | `true` | drop target collections before the run |
| `-poolsize` | `=workers` | connection pool size |

Each run prints a human summary plus a machine-readable `RESULT,...` line.

## Run the experiment (sweep K)

```bash
./run_sweep.sh                                  # K = 1 2 4 8 16, synthetic
WORKERS=32 RECORDS=500000 ./run_sweep.sh
BATCH=100 ./run_sweep.sh                         # batched inserts
DATAFILE=./workData/workload.dat ./run_sweep.sh  # real YCSB load data
```

Output goes to `results.csv` (and is printed as a table). Compare the
`throughput_ops_s` column across collection counts.

## Using real YCSB data (optional)

The synthetic generator produces YCSB-shaped docs (`_id=userN`, `field0..field9`
of 100B), so you do **not** need YCSB to run this. If you want the exact same
input as the `woc` repo, generate `.dat` files with that repo's
`ycsb/scripts/genData.sh` (needs Java + Maven) and pass the load file via
`-datafile`. The parser matches `mongodb/mgdb_leader.go`, so `INSERT`/`UPDATE`
lines are honored and `READ`/`SCAN` are skipped.

## Notes on a fair comparison

- Workload generation/parsing happens **before** timing; only the write phase is
  measured.
- Keep `-workers`, `-records`, `-batch` fixed and vary only `-collections`.
- Run against a freshly started `mongod` (or rely on `-drop`) so a warm cache
  from a previous run doesn't skew results.
- `throughput_ops_s` is the headline metric. Per-call latency percentiles are
  reported too (note: in batch mode a "call" writes `-batch` docs).

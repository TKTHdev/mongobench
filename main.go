// mongobench is a standalone MongoDB write-throughput benchmark.
//
// It answers one question: does splitting writes across multiple collections
// (on the same mongod) improve concurrent write throughput? It mirrors the
// YCSB data shape used by the woc repo but has no consensus / networking — just
// a MongoDB client hammering a server with concurrent writes.
//
// Compare runs by varying -collections while keeping everything else fixed.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type config struct {
	uri         string
	db          string
	coll        string
	collections int
	split       string
	workers     int
	records     int
	fields      int
	fieldLen    int
	datafile    string
	batch       int
	drop        bool
	poolSize    int
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.uri, "uri", "mongodb://localhost:27017", "MongoDB connection URI")
	flag.StringVar(&cfg.db, "db", "mongobench", "database name")
	flag.StringVar(&cfg.coll, "coll", "usertable", "base collection name (suffixed with _<idx>)")
	flag.IntVar(&cfg.collections, "collections", 1, "number of collections to spread writes across (K)")
	flag.StringVar(&cfg.split, "split", "roundrobin", "collection assignment: roundrobin | hash")
	flag.IntVar(&cfg.workers, "workers", 16, "number of concurrent write goroutines")
	flag.IntVar(&cfg.records, "records", 200000, "synthetic record count (ignored if -datafile set)")
	flag.IntVar(&cfg.fields, "fields", 10, "fields per document (synthetic mode)")
	flag.IntVar(&cfg.fieldLen, "fieldlen", 100, "bytes per field (synthetic mode)")
	flag.StringVar(&cfg.datafile, "datafile", "", "optional YCSB .dat file to load write ops from")
	flag.IntVar(&cfg.batch, "batch", 1, "documents per write call: 1=InsertOne, >1=InsertMany")
	flag.BoolVar(&cfg.drop, "drop", true, "drop target collections before the run")
	flag.IntVar(&cfg.poolSize, "poolsize", 0, "connection pool size (0 = workers)")
	flag.Parse()

	if cfg.collections < 1 {
		log.Fatalf("-collections must be >= 1")
	}
	if cfg.workers < 1 {
		log.Fatalf("-workers must be >= 1")
	}
	if cfg.poolSize <= 0 {
		cfg.poolSize = cfg.workers
	}

	// --- 1. Build the workload up front so generation/parsing is NOT timed. ---
	var ops []Op
	var err error
	if cfg.datafile != "" {
		ops, err = readYCSBFile(cfg.datafile)
		if err != nil {
			log.Fatalf("read datafile: %v", err)
		}
		log.Printf("loaded %d write ops from %s", len(ops), cfg.datafile)
	} else {
		ops = genSynthetic(cfg.records, cfg.fields, cfg.fieldLen)
		log.Printf("generated %d synthetic INSERT ops (%d fields x %dB)", len(ops), cfg.fields, cfg.fieldLen)
	}
	if len(ops) == 0 {
		log.Fatalf("no write ops to run")
	}

	// Precompute the target collection index for every op.
	collOf := make([]int, len(ops))
	for i := range ops {
		if cfg.split == "roundrobin" {
			collOf[i] = i % cfg.collections
		} else {
			collOf[i] = ops[i].collIndex(cfg.collections, "hash")
		}
	}

	// --- 2. Connect. ---
	ctx := context.Background()
	clientOpts := options.Client().
		ApplyURI(cfg.uri).
		SetMaxPoolSize(uint64(cfg.poolSize))
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		log.Fatalf("ping: %v (is mongod running at %s?)", err, cfg.uri)
	}

	db := client.Database(cfg.db)
	colls := make([]*mongo.Collection, cfg.collections)
	for k := 0; k < cfg.collections; k++ {
		name := fmt.Sprintf("%s_%d", cfg.coll, k)
		if cfg.drop {
			if err := db.Collection(name).Drop(ctx); err != nil {
				log.Fatalf("drop %s: %v", name, err)
			}
		}
		colls[k] = db.Collection(name)
	}

	// --- 3. Run the write phase (this is the only timed region). ---
	log.Printf("starting: collections=%d split=%s workers=%d batch=%d ops=%d",
		cfg.collections, cfg.split, cfg.workers, cfg.batch, len(ops))

	var (
		wg       sync.WaitGroup
		errCount int64
		latChunk = make([][]float64, cfg.workers) // per-call latencies (ms), per worker
	)

	// Partition the op slice into contiguous worker ranges.
	chunk := (len(ops) + cfg.workers - 1) / cfg.workers
	start := time.Now()
	for w := 0; w < cfg.workers; w++ {
		lo := w * chunk
		if lo >= len(ops) {
			break
		}
		hi := lo + chunk
		if hi > len(ops) {
			hi = len(ops)
		}
		wg.Add(1)
		go func(w, lo, hi int) {
			defer wg.Done()
			lats := runWorker(ctx, colls, ops, collOf, lo, hi, cfg.batch, &errCount)
			latChunk[w] = lats
		}(w, lo, hi)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// --- 4. Aggregate and report. ---
	var lats []float64
	for _, c := range latChunk {
		lats = append(lats, c...)
	}
	report(cfg, len(ops), elapsed, lats, atomic.LoadInt64(&errCount))
}

// runWorker writes ops[lo:hi]. With batch==1 it issues InsertOne/UpdateOne;
// with batch>1 it buffers INSERTs per collection and flushes via InsertMany
// (UPDATEs are always issued individually). Returns per-call latencies in ms.
func runWorker(ctx context.Context, colls []*mongo.Collection, ops []Op, collOf []int, lo, hi, batch int, errCount *int64) []float64 {
	lats := make([]float64, 0, hi-lo)

	if batch <= 1 {
		for i := lo; i < hi; i++ {
			op := ops[i]
			c := colls[collOf[i]]
			t0 := time.Now()
			var err error
			if op.Kind == OpUpdate {
				_, err = c.UpdateOne(ctx, bson.D{{Key: "_id", Value: op.Key}},
					bson.D{{Key: "$set", Value: toDoc(op.Fields)}})
			} else {
				_, err = c.InsertOne(ctx, buildDoc(op))
			}
			lats = append(lats, msSince(t0))
			if err != nil {
				atomic.AddInt64(errCount, 1)
			}
		}
		return lats
	}

	// Batched INSERT mode: one buffer per collection.
	buffers := make([][]interface{}, len(colls))
	flush := func(k int) {
		if len(buffers[k]) == 0 {
			return
		}
		t0 := time.Now()
		_, err := colls[k].InsertMany(ctx, buffers[k])
		lats = append(lats, msSince(t0))
		if err != nil {
			atomic.AddInt64(errCount, 1)
		}
		buffers[k] = buffers[k][:0]
	}
	for i := lo; i < hi; i++ {
		op := ops[i]
		k := collOf[i]
		if op.Kind == OpUpdate {
			t0 := time.Now()
			_, err := colls[k].UpdateOne(ctx, bson.D{{Key: "_id", Value: op.Key}},
				bson.D{{Key: "$set", Value: toDoc(op.Fields)}})
			lats = append(lats, msSince(t0))
			if err != nil {
				atomic.AddInt64(errCount, 1)
			}
			continue
		}
		buffers[k] = append(buffers[k], buildDoc(op))
		if len(buffers[k]) >= batch {
			flush(k)
		}
	}
	for k := range buffers {
		flush(k)
	}
	return lats
}

func buildDoc(op Op) bson.D {
	doc := bson.D{{Key: "_id", Value: op.Key}}
	for f, v := range op.Fields {
		doc = append(doc, bson.E{Key: f, Value: v})
	}
	return doc
}

func toDoc(fields map[string]string) bson.D {
	doc := bson.D{}
	for f, v := range fields {
		doc = append(doc, bson.E{Key: f, Value: v})
	}
	return doc
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000.0 }

func report(cfg config, totalOps int, elapsed time.Duration, lats []float64, errCount int64) {
	thr := float64(totalOps) / elapsed.Seconds()
	p50, p99, max, avg := percentiles(lats)

	fmt.Printf("\n========== mongobench results ==========\n")
	fmt.Printf("collections     : %d (%s)\n", cfg.collections, cfg.split)
	fmt.Printf("workers         : %d\n", cfg.workers)
	fmt.Printf("write mode      : %s\n", modeLabel(cfg.batch))
	fmt.Printf("total ops       : %d\n", totalOps)
	fmt.Printf("errors          : %d\n", errCount)
	fmt.Printf("elapsed         : %.3fs\n", elapsed.Seconds())
	fmt.Printf("throughput      : %.0f ops/sec\n", thr)
	fmt.Printf("per-call latency: avg=%.3fms  p50=%.3fms  p99=%.3fms  max=%.3fms\n", avg, p50, p99, max)
	fmt.Printf("========================================\n")

	// Machine-readable line for sweep aggregation.
	fmt.Printf("RESULT,collections=%d,split=%s,workers=%d,batch=%d,ops=%d,errors=%d,elapsed_s=%.3f,throughput_ops_s=%.0f,call_avg_ms=%.3f,call_p50_ms=%.3f,call_p99_ms=%.3f\n",
		cfg.collections, cfg.split, cfg.workers, cfg.batch, totalOps, errCount,
		elapsed.Seconds(), thr, avg, p50, p99)
}

func modeLabel(batch int) string {
	if batch <= 1 {
		return "InsertOne (single)"
	}
	return fmt.Sprintf("InsertMany (batch=%d)", batch)
}

func percentiles(lats []float64) (p50, p99, max, avg float64) {
	if len(lats) == 0 {
		return
	}
	sorted := make([]float64, len(lats))
	copy(sorted, lats)
	sort.Float64s(sorted)
	var sum float64
	for _, v := range sorted {
		sum += v
	}
	avg = sum / float64(len(sorted))
	p50 = sorted[pctIdx(len(sorted), 0.50)]
	p99 = sorted[pctIdx(len(sorted), 0.99)]
	max = sorted[len(sorted)-1]
	return
}

func pctIdx(n int, p float64) int {
	i := int(math.Ceil(p*float64(n))) - 1
	if i < 0 {
		i = 0
	}
	if i >= n {
		i = n - 1
	}
	return i
}

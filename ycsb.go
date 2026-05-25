package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"os"
	"strings"
)

// Op is a single write operation to apply against MongoDB.
// It mirrors the subset of mongodb.Query (in the woc repo) that matters for a
// write-only benchmark: INSERT and UPDATE.
type Op struct {
	Kind   OpKind
	Key    string            // becomes the document _id
	Fields map[string]string // field0..fieldN -> value
}

type OpKind int

const (
	OpInsert OpKind = iota
	OpUpdate
)

// collIndex picks which collection (0..k-1) this op lands in.
func (o Op) collIndex(k int, split string) int {
	if k <= 1 {
		return 0
	}
	if split == "hash" {
		h := fnv.New32a()
		_, _ = h.Write([]byte(o.Key))
		return int(h.Sum32() % uint32(k))
	}
	// roundrobin is handled by caller using the slice index; hash here is the
	// only key-derived strategy. Fall back to hash if asked for anything else.
	h := fnv.New32a()
	_, _ = h.Write([]byte(o.Key))
	return int(h.Sum32() % uint32(k))
}

// genSynthetic builds `records` INSERT ops with `fields` fields of `fieldLen`
// bytes each. The field payload is shared (content is irrelevant to write
// throughput; only the document size matters), so generation stays cheap.
func genSynthetic(records, fields, fieldLen int) []Op {
	payload := strings.Repeat("x", fieldLen)
	ops := make([]Op, records)
	for i := 0; i < records; i++ {
		f := make(map[string]string, fields)
		for j := 0; j < fields; j++ {
			f[fmt.Sprintf("field%d", j)] = payload
		}
		ops[i] = Op{Kind: OpInsert, Key: fmt.Sprintf("user%d", i), Fields: f}
	}
	return ops
}

// readYCSBFile parses a YCSB "basic" binding .dat file (as produced by the woc
// repo's ycsb/scripts/genData.sh) and returns the write ops (INSERT/UPDATE).
// READ/SCAN/DELETE lines are skipped. The parsing mirrors lineToQuery in
// mongodb/mgdb_leader.go so files generated for woc work unchanged.
func readYCSBFile(path string) ([]Op, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var ops []Op
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 8*1024*1024)
	for sc.Scan() {
		ln := sc.Text()
		switch {
		case strings.HasPrefix(ln, "INSERT"):
			op, err := parseInsert(ln)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)
		case strings.HasPrefix(ln, "UPDATE"):
			op, err := parseUpdate(ln)
			if err != nil {
				return nil, err
			}
			ops = append(ops, op)
		default:
			// READ / SCAN / DELETE / comments: not writes, skip.
			continue
		}
	}
	return ops, sc.Err()
}

// parseInsert mirrors mongodb/mgdb_leader.go lineToQuery (INSERT branch).
// Format: INSERT <table> <key> [ field0=... field1=... ... ]
func parseInsert(ln string) (Op, error) {
	params := strings.SplitN(ln, " ", 4)
	if len(params) < 4 {
		return Op{}, fmt.Errorf("malformed INSERT line: %q", ln)
	}
	key := params[2]
	raw := params[3]
	fields := make(map[string]string)
	// strip the surrounding "[ ... ]]" the same way the woc parser does:
	// rawValue[1:len-2] then split on " field".
	if len(raw) < 2 {
		return Op{Kind: OpInsert, Key: key, Fields: fields}, nil
	}
	inner := raw[1 : len(raw)-2]
	for _, rawField := range strings.Split(inner, " field") {
		if rawField == "" {
			continue
		}
		kv := strings.SplitN(rawField, "=", 2)
		if len(kv) != 2 {
			continue
		}
		fields["field"+kv[0]] = kv[1]
	}
	return Op{Kind: OpInsert, Key: key, Fields: fields}, nil
}

// parseUpdate mirrors mongodb/mgdb_leader.go lineToQuery (UPDATE branch).
// Format: UPDATE <table> <key> [ field=value ]
func parseUpdate(ln string) (Op, error) {
	params := strings.SplitN(ln, " ", 4)
	if len(params) < 4 {
		return Op{}, fmt.Errorf("malformed UPDATE line: %q", ln)
	}
	key := params[2]
	raw := params[3]
	fields := make(map[string]string)
	if len(raw) > 4 {
		kv := strings.SplitN(raw[2:len(raw)-2], "=", 2)
		if len(kv) == 2 {
			fields[kv[0]] = kv[1]
		}
	}
	return Op{Kind: OpUpdate, Key: key, Fields: fields}, nil
}

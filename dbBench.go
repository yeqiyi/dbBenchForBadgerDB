package main

import (
	"badgerBench/bDB"
	"bufio"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
)

// Comma-separated list of operations to run in the specified order
//
//	Actual benchmarks:
//	   fillseq       -- write N values in sequential key order in async mode
//	   fillrandom    -- write N values in random key order in async mode
//	   overwrite     -- overwrite N values in random key order in async mode
//	   fillsync      -- write N/100 values in random key order in sync mode
//	   fill100K      -- write N/1000 100K values in random order in async mode
//	   deleteseq     -- delete N keys in sequential order
//	   deleterandom  -- delete N keys in random order
//	   readseq       -- read N times sequentially
//	   readreverse   -- read N times in reverse order
//	   readrandom    -- read N times in random order
//	   readmissing   -- read N missing keys in random order
//	   readhot       -- read N times in random order from 1% section of DB
//	   seekrandom    -- N random seeks
//	   seekordered   -- N ordered seeks
//	   open          -- cost of opening a DB
//	   crc32c        -- repeated crc32c of 4K of data
//	Meta operations:
//	   compact     -- Compact the entire DB
//	   stats       -- Print DB stats
//	   sstables    -- Print sstable info
//	   heapprofile -- Dump a heap profile (if supported by this port)
var FLAGS_benchmarks []string = []string{
	"fillseq",
	"fillsync",
	"fillrandom",
	"overwrite",
	"readrandom",
	"readrandom",
	"readseq",
	"readreverse",
	"fill100k",
}

var default_opt = badger.DefaultOptions("")

// Number of key/values to place in database
var FLAGS_num int = 1000000

// Number of read operations to do. If negative, do FLAGS_num reads.
var FLAGS_reads int = -1

// Number of concurrent threads to run
var FLAGS_threads int = 1

// Size of each value
var FLAGS_value_size int = 16 << 10

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
var FLAGS_write_buffer_size int64 = 0

// Number of memtable
var FLAGS_memtable_num = 0

// value threshold to trigger key/value separate
var FLAGS_value_threshold = 0

// max entries per value log
var FLAGS_vlog_max_entries uint32 = 0

// key size
var FLAGS_key_size int = 16

// db name
var FLAGS_db string = "/tmp/BadgerBench"

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
var FLAGS_use_existing_db = false

func PrintEnv() {
	fmt.Fprintf(os.Stderr, "BadgerDB  \n")
	now := time.Now()
	fmt.Fprintf(os.Stderr, "Date:        %s\n", now.String())
	if runtime.GOOS == "linux" {
		if file, err := os.Open("/proc/cpuinfo"); err == nil {
			defer file.Close()
			numCpus := 0
			var cpuType, cacheSize string
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				if sepIdx := strings.Index(scanner.Text(), ":"); sepIdx != -1 {
					key := scanner.Text()[:sepIdx-1]
					val := scanner.Text()[sepIdx+1:]
					if key == "model name" {
						numCpus++
						cpuType = val
					} else if key == "cache size" {
						cacheSize = val
					}
				}
			}
			fmt.Fprintf(os.Stderr, "CPU:         %d * %s\n", numCpus, cpuType)
			fmt.Fprintf(os.Stderr, "CPUCache:   %s\n", cacheSize)
		}
	}

}

type SharedState struct {
	mu    sync.Mutex
	cv    *sync.Cond
	total int

	numInitialized int
	numDone        int
	start          bool
}

func MakeSharedState(total int) *SharedState {
	stat := new(SharedState)
	stat.total = total
	stat.cv = sync.NewCond(&stat.mu)
	stat.numInitialized = 0
	stat.numDone = 0
	stat.start = false
	return stat
}

// Per-thread state for concurrent executions of the same benchmark.
type ThreadState struct {
}

type Benchmark struct {
	db                *bDB.BadgerDBWrapper
	num               int // total num of entries
	valueSize         int
	entriesPerBatch   int
	reads             int
	totalThreadsCount int
}

func (bm *Benchmark) PrintHeader() {
	PrintEnv()
	fmt.Fprintf(os.Stdout, "Keys:        %d bytes each\n", FLAGS_key_size)
	fmt.Fprintf(os.Stdout, "Values:      %d bytes each\n", FLAGS_value_size)
	fmt.Fprintf(os.Stdout, "Entries:     %d\n", bm.num)
	fmt.Fprintf(os.Stdout, "RawSize:     %.1f MB (estimated)\n",
		float64((FLAGS_key_size+bm.valueSize)*bm.num)/1048576.0)
	fmt.Fprintf(os.Stdout, "------------------------------------------------\n")
}

func MakeBenchmark() *Benchmark {
	bm := new(Benchmark)
	{
		bm.db = nil
		bm.num = FLAGS_num
		bm.valueSize = FLAGS_value_size
		bm.entriesPerBatch = 1
		if FLAGS_reads < 0 {
			bm.reads = FLAGS_num
		} else {
			bm.reads = FLAGS_reads
		}
		bm.totalThreadsCount = 0

		if !FLAGS_use_existing_db {
			os.Remove(FLAGS_db)
		}
	}
	return bm
}

func (bm *Benchmark) Run() {

}

func Init() {
	FLAGS_write_buffer_size = default_opt.MaxTableSize
	FLAGS_vlog_max_entries = default_opt.ValueLogMaxEntries
	FLAGS_memtable_num = default_opt.NumMemtables
	FLAGS_value_threshold = default_opt.ValueThreshold
}

func main() {
	Init()
	{
		var benchmarks string
		flag.StringVar(&benchmarks, "benchmarks", strings.Join(FLAGS_benchmarks, `,`), "benchmarks")
		FLAGS_benchmarks = strings.Split(benchmarks, ",")

		flag.IntVar(&FLAGS_num, "num", FLAGS_num, "Number of key/values to place in database")
		flag.IntVar(&FLAGS_value_size, "value_size", FLAGS_value_size, "Size of each value")
		flag.IntVar(&FLAGS_value_threshold, "value_threshold", FLAGS_value_threshold, "value threshold to trigger key/value separate")
		flag.Int64Var(&FLAGS_write_buffer_size, "write_buffer_size", FLAGS_write_buffer_size, "Number of bytes to buffer in memtable before compacting")
		flag.IntVar(&FLAGS_threads, "threads", FLAGS_threads, "Number of concurrent threads to run")
		flag.IntVar(&FLAGS_memtable_num, "mem_table_num", FLAGS_memtable_num, "Number of memtable")
		flag.StringVar(&FLAGS_db, "db", FLAGS_db, "database path")
	}
	flag.Parse()
	bm := MakeBenchmark()
	bm.PrintHeader()
}

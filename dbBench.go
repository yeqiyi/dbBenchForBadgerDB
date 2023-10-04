package main

import (
	"badgerBench/bDB"
	"bufio"
	"flag"
	"fmt"
	"io"
	"math/rand"
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
var FLAGS_num int = 1000

// Number of read operations to do. If negative, do FLAGS_num reads.
var FLAGS_reads int = -1

// Number of concurrent threads to run
var FLAGS_threads int = 1

// Size of each value
var FLAGS_value_size int = 100

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
	fmt.Fprintf(os.Stderr, "BadgerDB     v4.2.0\n")
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

type Stats struct {
	start        float64
	finish       float64
	seconds      float64
	done         int
	nextReport   int
	bytes        int64
	lastOPFinish float64
	msg          string
}

func (s *Stats) Start() {
	s.nextReport = 100
	s.done = 0
	s.bytes = 0
	s.seconds = 0
	s.msg = ""
	now := time.Now().UnixMicro()
	s.lastOPFinish = float64(now)
	s.start = s.lastOPFinish
	s.finish = s.lastOPFinish
}

func (s *Stats) Merge(other *Stats) {
	s.done += other.done
	s.bytes += other.bytes
	s.seconds += other.seconds
	if other.start < s.start {
		s.start = other.start
	}
	if other.finish > s.finish {
		s.finish = other.finish
	}
	if s.msg == "" {
		s.msg = other.msg
	}
}

func (s *Stats) Stop() {
	s.finish = float64(time.Now().UnixMicro())
	s.seconds = (s.finish - s.start) * 1e-6
}

func AppendWithSpace(str *string, msg string) {
	if msg == "" {
		return
	}
	if *str != "" {
		*str += " "
	}
	*str += msg
}

func FFlush(w io.Writer) {
	f := bufio.NewWriter(w)
	f.Flush()
}

func (s *Stats) AddMsg(msg string) {
	AppendWithSpace(&s.msg, msg)
}

func (s *Stats) AddBytes(n int64) {
	s.bytes += n
}

func (s *Stats) FinishedSingleOp() {
	s.done++
	if s.done >= s.nextReport {
		if s.nextReport < 1000 {
			s.nextReport += 100
		} else if s.nextReport < 5000 {
			s.nextReport += 500
		} else if s.nextReport < 10000 {
			s.nextReport += 1000
		} else if s.nextReport < 50000 {
			s.nextReport += 5000
		} else if s.nextReport < 100000 {
			s.nextReport += 10000
		} else if s.nextReport < 500000 {
			s.nextReport += 50000
		} else {
			s.nextReport += 100000
		}
		fmt.Fprintf(os.Stderr, "... finished %d ops%30s\r", s.done, "")
		FFlush(os.Stderr)
	}
}

func (s *Stats) Report(name string) {
	if s.done < 1 {
		s.done = 1
	}

	var extra string
	if s.bytes > 0 {
		elapsed := (s.finish - s.start) * 1e-6
		extra = fmt.Sprintf("%6.1f MB/s", (float64(s.bytes)/1048576.)/elapsed)
	}

	AppendWithSpace(&extra, s.msg)

	fmt.Fprintf(os.Stdout, "%-12s : %11.3f micros/op;%s%s\n",
		name, s.seconds*1e6/float64(s.done), func() string {
			if extra != "" {
				return " "
			}
			return ""
		}(), extra)
	FFlush(os.Stdout)
}

func MakeStat() Stats {
	s := Stats{}
	s.Start()
	return s
}

// Per-thread state for concurrent executions of the same benchmark.
type ThreadState struct {
	tid    int        // 0..n-1 when running in n threads
	rd     *rand.Rand // Has different seeds for different threads
	stats  Stats
	shared *SharedState
}

func MakeThreadState(tid int, seed int64) *ThreadState {
	ts := new(ThreadState)
	ts.rd = rand.New(rand.NewSource(seed))
	ts.stats = MakeStat()
	ts.tid = tid
	ts.shared = nil
	return ts
}

// ============================================
//
//	Helper for quickly generation random data
//
// ============================================
func RandomString(rnd *rand.Rand, len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = byte(' ' + rand.Int31n(95))
	}
	return string(bytes)
}

func GenKey(k int) string {
	return fmt.Sprintf("%016d", k)
}

// ======================================
//			Benchmark
// ======================================

type ThreadArg struct {
	bm     *Benchmark
	shared *SharedState
	thread *ThreadState
	method func(*Benchmark, *ThreadState)
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

func ThreadBody(v interface{}) {
	arg := v.(*ThreadArg)
	shared := arg.shared
	thread := arg.thread
	{
		shared.cv.L.Lock()
		shared.numInitialized++
		if shared.numInitialized >= shared.total {
			shared.cv.Broadcast()
		}
		for !shared.start {
			shared.cv.Wait()
		}
		shared.cv.L.Unlock()
	}

	thread.stats.Start()
	arg.method(arg.bm, thread)
	thread.stats.Stop()

	{
		shared.cv.L.Lock()
		shared.numDone++
		if shared.numDone >= shared.total {
			shared.cv.Broadcast()
		}
		shared.cv.L.Unlock()
	}
}

func (bm *Benchmark) Open() {
	opt := badger.DefaultOptions(FLAGS_db)
	opt.MaxTableSize = FLAGS_write_buffer_size
	opt.ValueLogMaxEntries = FLAGS_vlog_max_entries
	opt.NumMemtables = FLAGS_memtable_num
	opt.ValueThreshold = FLAGS_value_threshold

	var err error
	if bm.db, err = bDB.MakeDB(opt); err != nil {
		fmt.Fprintf(os.Stderr, "err occurs when open db: %s\n", err.Error())
		os.Exit(1)
	}
	if err = bm.db.Open(); err != nil {
		fmt.Fprintf(os.Stderr, "err occurs when open db: %s\n", err.Error())
		os.Exit(1)
	}
}

func (bm *Benchmark) RunBenchmark(n int, name string, method func(*Benchmark, *ThreadState)) {
	shared := MakeSharedState(n)

	args := make([]ThreadArg, n)
	for i := 0; i < n; i++ {
		args[i].bm = bm
		args[i].method = method
		args[i].shared = shared
		bm.totalThreadsCount++
		// Seed the thread's random state deterministically based upon thread
		// creation across all benchmarks. This ensures that the seeds are unique
		// but reproducible when rerunning the same set of benchmarks.
		args[i].thread = MakeThreadState(i, int64(1000+bm.totalThreadsCount /*seed*/))
		args[i].thread.shared = shared
		go ThreadBody(&args[i])
	}

	shared.cv.L.Lock()
	for shared.numInitialized < n {
		shared.cv.Wait()
	}

	shared.start = true
	shared.cv.Broadcast()
	for shared.numDone < n {
		shared.cv.Wait()
	}
	shared.cv.L.Unlock()

	for i := 1; i < n; i++ {
		args[0].thread.stats.Merge(&args[i].thread.stats)
	}
	args[0].thread.stats.Report(name)

}

// ======================================
//
//	Work Function
//
// ======================================
func (bm *Benchmark) DoWrite(thread *ThreadState, seq bool) {
	if bm.num == FLAGS_num {
		msg := fmt.Sprintf("(%d ops)", bm.num)
		thread.stats.AddMsg(msg)
	}

	bytes := 0
	rnd := rand.New(rand.NewSource(301))
	
	for i := 0; i < bm.num; i++ {
		var k int
		if seq {
			k = i
		} else {
			k = thread.rd.Intn(FLAGS_num)
		}
		key := GenKey(k)
		if err := bm.db.Put(key, RandomString(rnd, bm.valueSize)); err != nil {
			fmt.Fprintf(os.Stderr, "put error: %s\n", err.Error())
			os.Exit(1)
		}
		bytes += bm.valueSize + len(key)
		thread.stats.FinishedSingleOp()
	}
	thread.stats.AddBytes(int64(bytes))
}

func (bm *Benchmark) WriteSeq(thread *ThreadState) {
	bm.DoWrite(thread, true)
}

func (bm *Benchmark) WriteRandom(thread *ThreadState) {
	bm.DoWrite(thread, false)
}

func (bm *Benchmark) Run() {
	bm.PrintHeader()
	bm.Open()
	bm.num = FLAGS_num
	if FLAGS_reads < 0 {
		bm.reads = FLAGS_num
	} else {
		bm.reads = FLAGS_reads
	}
	bm.valueSize = FLAGS_value_size
	bm.entriesPerBatch = 1

	var method func(*Benchmark, *ThreadState)
	freshDB := false
	numThreads := FLAGS_threads

	for _, benchmark := range FLAGS_benchmarks {
		switch benchmark {
		case "fillseq":
			freshDB = true
			method = (*Benchmark).WriteSeq
		case "fillrandom":
			freshDB = true
			method = (*Benchmark).WriteRandom
		case "overwrite":
			freshDB = false
			method = (*Benchmark).WriteRandom
		case "fillsync":
			freshDB = true
			bm.num /= 1000
		case "readseq":
		case "readreverse":
		case "readrandom":
		case "fill100k":
			freshDB = true

		default:
			if benchmark != "" {
				fmt.Fprintf(os.Stderr, "unknown benchmark '%s'\n", benchmark)
			}
		}
		if freshDB {
			if err := bm.db.DestroyDB(); err!= nil{
				fmt.Fprintf(os.Stderr, "failed to drop db: %s\n", err.Error())
				os.Exit(1)
			}
		}
	
		if method != nil {
			bm.RunBenchmark(numThreads, benchmark, method)
		}
	}
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
	bm.Run()
}

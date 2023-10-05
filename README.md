# dbBench for badgerDB

-------

The `db_bench` written for [badgerDB](https://github.com/dgraph-io/badger) (just like [leveldb](https://github.com/google/leveldb/blob/main/benchmarks/db_bench.cc))
Test the performance of badgerdb under basic R/W workload

>	Actual supported benchmarks:
>	   fillseq       -- write N values in sequential key order in async mode
>    fillrandom    -- write N values in random key order in async mode
>    overwrite     -- overwrite N values in random key order in async mode
>    fillsync      -- write N/100 values in random key order in sync mode
>    fill100K      -- write N/1000 100K values in random order in async mode
>    readseq       -- read N times sequentially
>    readreverse   -- read N times in reverse order
>    readrandom    -- read N times in random order
>	   readhot       -- read N times in random order from 1% section of DB

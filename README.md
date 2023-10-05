# dbBench for badgerDB

-------

The `db_bench` written for [badgerDB](https://github.com/dgraph-io/badger) (just like [leveldb](https://github.com/google/leveldb/blob/main/benchmarks/db_bench.cc))
Test the performance of badgerdb under basic R/W workload

## Parameters:
 - `db` : path of database
 - `num`: Number of key/values to place in database
 - `value_size`: Size of each value
 - `value_threshold`: value threshold to trigger key/value separate
 - `write_buffer_size`: size of memtables
 - `threads`: Number of concurrent threads to run
 - `mem_table_num`: Number of memtables
 - `num_level0`: Number of tables at level0
 - `num_level0_stall`: Number of stalled tables at level0

##	Actual supported benchmarks:  
 -	`fillseq`       -- write N values in sequential key order in async mode
 -	`fillrandom`    -- write N values in random key order in async mode  
 -  `overwrite`     -- overwrite N values in random key order in async mode  
 -  `fillsync`      -- write N/100 values in random key order in sync mode  
 -  `fill100K`      -- write N/1000 100K values in random order in async mode  
 -  `readseq`       -- read N times sequentially  
 -  `readreverse`   -- read N times in reverse order  
 -  `readrandom`    -- read N times in random order  
 -  `readhot`       -- read N times in random order from 1% section of DB  

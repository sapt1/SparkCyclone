# PySpark Benchmark

## Generate Test Data
```py
spark-submit --master spark://spark-master:7077  generate_csv.py /path/to/test/data/file -r num_rows -p num_partitions
```

## Shuffle Benchmark
```py
spark-submit --master spark://spark-master:7077 shuffle_benchmark.py /path/to/test/data/file -r num_partitions -n 'benchmark-job-name' -o 'output' -sl 11001 -t repart
```

## Pick Level
- DISK_ONLY = 10001
- DISK_ONLY_2 = 10002
- MEMORY_AND_DISK = 11001
- MEMORY_AND_DISK_2 = 11002
- MEMORY_AND_DISK_SER = 11001
- MEMORY_AND_DISK_SER_2 = 11002
- MEMORY_ONLY = 01001
- MEMORY_ONLY_2 = 01002
- MEMORY_ONLY_SER = 01001
- MEMORY_ONLY_SER_2 = 01002
- OFF_HEAP = 11101
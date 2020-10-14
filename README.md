# Roadmap of SeqDSv2

- Read Path
    - [x] read a directory path 
    - [x] read multiples seq files
    - read regex pattern
- Read optimization ( filter push down, limit count)
- Write Path
- Vectorized Read
- DevOps:
    - [x] scala style
    - [ ] coverage Travis CI
- [x] Benchmark with RDD API


- Read Path with Spark v2.3, 2.4 and 3
- support MapType



## Notes:
Fix Array[Byte] 


## Motivation:
- The SeqDataSourceV2 automatically detects the type unlike the RDD API that requires prior knowledge.
- The SeqDataSourceV2 is faster than the RDD API (See Benchmark section).


## Supported types:
    
| Spark Types   | Hadoop          |
| ------------- | ---------------:|
| LongType      | LongWritable    |
| DoubleType    | DoubleWritable  |
| FloatType     | FloatWritable   |
|  IntegerType  | IntWritable     |
| BooleanType   | BooleanWritable |
| NullType      | NullWritable    |
| StringType    | BytesWritable   |
| StringType    | Text            |

## Usage
`#Todo`

## Benchmark
The benchmark uses the [MovieLens 25M Dataset](https://grouplens.org/datasets/movielens/25m/) saved as `sequence` file.
The key and value of the dataset are `movieId` and `rating` saved as String ([org.apache.hadoop.Text](https://hadoop.apache.org/docs/r2.8.0/api/org/apache/hadoop/io/Text.html)).  
The following queries are used in the benchmark.

**aggQuery**: 
```sql
SELECT COUNT(DISTINCT value)
FROM dataset 
GROUP BY key
```

**count**:
````sql
SELECT COUNT(*)
FROM dataset
````
**filterQuery**:
```sql
SELECT COUNT(*)
FROM dataset
WHERE value= "4.0"
```

N.B: The lower score is the better.
```console
Benchmark                         Mode  Cnt   Score   Error  Units
TestBenchmark.aggQueryWithDS        ss    5  11.720 ± 1.575   s/op
TestBenchmark.aggQueryWithRDD       ss    5  13.898 ± 0.569   s/op
TestBenchmark.countWithDS           ss    5   8.840 ± 0.772   s/op
TestBenchmark.countWithRDD          ss    5  11.649 ± 0.538   s/op
TestBenchmark.filterQueryWithDS     ss    5   9.312 ± 0.548   s/op
TestBenchmark.filterQueryWithRDD    ss    5  11.840 ± 0.467   s/op
```
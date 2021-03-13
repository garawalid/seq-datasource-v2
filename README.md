# SeqDataSourceV2
[![Build Status](https://travis-ci.com/garawalid/seq-datasource-v2.svg?token=SMJd5DBDDJrYEpCNWqiF&branch=master)](https://travis-ci.com/garawalid/seq-datasource-v2)

## Motivation:
- The SeqDataSourceV2 automatically detects the type unlike the RDD API that requires prior knowledge.
- The SeqDataSourceV2 is faster than the RDD API (See Benchmark at `SeqDataSourceV2Benchmark`).


## Supported types:
The following list contains the type mapping and the supported types by this Data Source.  
Some types support the vectorized read optimisation (aka Arrow optimization)

| Spark Types   | Spark (Vectorized Read Path) | Hadoop          |
| ------------- | ------------------------|---------------:|
| LongType      | Supported | LongWritable    |
| DoubleType    | Supported |DoubleWritable  |
| FloatType     | Supported |FloatWritable   |
|  IntegerType  | Supported |IntWritable     |
| BooleanType   | Supported |BooleanWritable |
| NullType      | **Not Supported** |NullWritable    |
| StringType    | **Not Supported** | BytesWritable   |
| StringType    | **Not Supported** | Text            |

**N.B**:   
- The vectorized read path is disabled by default. You can turn it by setting `spark.sql.seq.enableVectorizedReader` to true.
```scala
val spark = SparkSession
          .builder()
          .master("local[1]")
          .config("spark.sql.seq.enableVectorizedReader", "true")
          .getOrCreate()
```

- If one column doesn't support vectorized read path, the SeqDataSourceV2 will fall back to normal read path.  
Example: 
    - The following schema (key : IntegerType, value: FloatType) supports vectorized read path. 
    - The following schema (key : IntegerType, value: StringType) doesn't support vectorized read path. 

- It's possible to control the number of rows of the batch in the vectorized read path with `spark.sql.seq.columnarReaderBatchSize`.  
By default, the size of the batch is `4096` rows.
## Usage
`#Todo`

It's possible to pass a schema to DataFrame API. There are few rules around **scehma**.
- The filed names must be **key** and/or **value**.
> The name **key** will project the key field of the Seq file. The same goes for the **value**
- The filed type should match the type of the seq file.

```scala
    val schema = new StructType()
      .add("key", IntegerType, true)
      .add("value", LongType, true)
    val df = spark.read.format("seq").schema(schema).load("path")

```
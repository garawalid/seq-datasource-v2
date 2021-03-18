# SeqDataSourceV2

![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/garawalid/seq-datasource-v2)
[![Spark version](https://img.shields.io/badge/spark-2.4-brightgreen.svg)](https://spark.apache.org/downloads.html)
[![Build Status](https://travis-ci.com/garawalid/seq-datasource-v2.svg?token=SMJd5DBDDJrYEpCNWqiF&branch=master)](https://travis-ci.com/garawalid/seq-datasource-v2)
![GitHub](https://img.shields.io/github/license/garawalid/seq-datasource-v2)

The SeqDataSourceV2 package allows reading [Hadoop Sequence File](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/SequenceFile.html) from Spark SQL.  
It's compatible only with Spark 2.4

## Features:
- The SeqDataSourceV2 automatically detects the type unlike the RDD API that requires prior knowledge.
- The SeqDataSourceV2 is 1.3x faster than the RDD API (See Benchmark at `SeqDataSourceV2Benchmark`).


## Supported types:
The following list contains the type mapping and the supported types by this Data Source.  
Some types support the vectorized read optimization (aka Arrow optimization)

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

- If one column doesn't support vectorized read path, the SeqDataSourceV2 will fall back to the normal read path.  
Example: 
    - The following schema (key : IntegerType, value: FloatType) supports vectorized read path. 
    - The following schema (key : IntegerType, value: StringType) doesn't support vectorized read path. 

- It's possible to control the number of rows of the batch in the vectorized read path with `spark.sql.seq.columnarReaderBatchSize`.  
By default, the size of the batch is `4096` rows.
## Usage

#### Option 1: Include the jar in the Spark-Submit
Example with spark-submit:
```bash
$ spark-submit --class Main --jars seq-datasource-v2-0.2.0.jar Example-SNAPSHOT.jar
```
Example with pyspark:
```bash
$ pyspark --jars seq-datasource-v2-0.2.0.jar
```

#### Option 2: Include the package in the Spark-Submit
Example with spark-submit:
```bash
$ spark-submit --packages garawalid:seq-datasource-v2:0.2.0
```

#### Option 3: Import the package as a dependency
With Maven
```xml

<dependency>
  <groupId>org.gwalid</groupId>
  <artifactId>seq-datasource-v2</artifactId>
  <version>0.2.0</version>
</dependency>
```

## Examples

**Scala API**
```scala

    val spark = SparkSession.builder()
      .master("local[0]")
      .getOrCreate()

    val df = spark.read.format("seq").load("data.seq")
    df.show()

```
**Python API**
```python
    df = spark.read.format("seq").load("data.seq")
    df.printSchema()

```

It's possible to pass a schema to DataFrame API. There are few rules around **schema**.
- The filed names must be **key** and/or **value**.
> The name **key** will project the key field of the Seq file. The same goes for the **value**
- The filed type should match the type of the seq file.

```scala
    val schema = new StructType()
      .add("key", IntegerType, true)
      .add("value", LongType, true)
    val df = spark.read.format("seq").schema(schema).load("path")

```

## Contributing
You are welcome to submit pull requests with any changes for this repository at any time. I'll be very glad to see any contributions.


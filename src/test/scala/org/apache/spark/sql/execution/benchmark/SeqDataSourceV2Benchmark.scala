package org.apache.spark.sql.execution.benchmark

import java.nio.file.Files
import java.util.Properties

import org.apache.hadoop.fs.Path
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types._
import org.apache.spark.util.Benchmark


object SeqDataSourceV2Benchmark {
  val tempDirFile = Files.createTempDirectory(this.getClass.getName).toFile
  val tempDir: String = tempDirFile.toString


  private def benchmark(spark: SparkSession, kType: DataType, vType: DataType,
                        values: Int, tableDF: String, tableRDD: String,
                        aggOperation: String, condition: String): Unit = {

    val singleColScan = new Benchmark(s"SQL Single ${kType.sql} Column Scan", values)


    singleColScan.addCase("Seq RDD") { _ =>
      spark.sql(s"select ${aggOperation}(key) from ${tableRDD}").collect()
    }

    singleColScan.addCase("SeqDSv2") { _ =>
      spark.conf.set("spark.sql.seq.enableVectorizedReader", "false")
      spark.sql(s"select ${aggOperation}(key) from ${tableDF}").collect()
    }

    singleColScan.addCase("SeqDSv2 Vectorized") { _ =>
      spark.conf.set("spark.sql.seq.enableVectorizedReader", "true")
      spark.sql(s"select ${aggOperation}(key) from ${tableDF}").collect()
    }

    singleColScan.run()

    val doubleColScan = new Benchmark(s"SQL Two Column (${kType.sql},${vType.sql} )  Scan", values)

    doubleColScan.addCase("Seq RDD") { _ =>
      spark.sql(s"select  ${aggOperation}(key), ${aggOperation}(value) from ${tableRDD}").collect()
    }

    doubleColScan.addCase("SeqDSv2") { _ =>
      spark.conf.set("spark.sql.seq.enableVectorizedReader", "false")
      spark.sql(s"select  ${aggOperation}(key), ${aggOperation}(value) from ${tableDF}").collect()
    }

    doubleColScan.addCase("SeqDSv2 Vectorized") { _ =>
      spark.conf.set("spark.sql.seq.enableVectorizedReader", "true")
      spark.sql(s"select  ${aggOperation}(key), ${aggOperation}(value) from ${tableDF}").collect()
    }

    doubleColScan.run()

    val filterNullBench = new Benchmark(s"SQL NULL Filter ${kType.sql} Column", values)

    filterNullBench.addCase("Seq RDD") { _ =>
      spark.sql(s"select key from ${tableRDD} where key is null").collect()
    }


    filterNullBench.addCase("SeqDSv2") { _ =>
      spark.conf.set("spark.sql.seq.enableVectorizedReader", "false")
      spark.sql(s"select key from ${tableDF} where key is null").collect()
    }

    filterNullBench.addCase("SeqDSv2 Vectorized") { _ =>
      spark.conf.set("spark.sql.seq.enableVectorizedReader", "true")
      spark.sql(s"select key from ${tableDF} where key is null").collect()
    }

    filterNullBench.run()

    val filterBench = new Benchmark(s"SQL Filter ${kType.sql} Column ", values)

    filterBench.addCase("Seq RDD") { _ =>
      spark.sql(s"select key from ${tableRDD} where key = ${condition}").collect()
    }

    filterBench.addCase("SeqDSv2") { _ =>
      spark.conf.set("spark.sql.seq.enableVectorizedReader", "false")
      spark.sql(s"select key from ${tableDF} where key = ${condition}").collect()
    }

    filterBench.addCase("SeqDSv2 Vectorized") { _ =>
      spark.conf.set("spark.sql.seq.enableVectorizedReader", "true")
      spark.sql(s"select key from ${tableDF} where key = ${condition}").collect()
    }

    filterBench.run()

  }

  def main(args: Array[String]): Unit = {

    val prop = new Properties()
    prop.setProperty("log4j.rootLogger", "INFO")
    org.apache.log4j.PropertyConfigurator.configure(prop)

    val conf = new SparkConf()
      .setAppName("DataSourceReadBenchmark")
      .set("spark.master", "local[3]")
      .setIfMissing("spark.driver.memory", "2g")
      .setIfMissing("spark.executor.memory", "1g")
      .setIfMissing("spark.ui.enabled", "false")
      .setIfMissing("spark.sql.seq.enableVectorizedReader", "false")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val values = 1024 * 1024 * 15

    // DoubleType & Double
    var kType: DataType = DoubleType
    var vType: DataType = IntegerType
    var tableRDD = s"tableRDD_${kType.sql}_${vType.sql}"
    var tableDF = s"tableDF_${kType.sql}_${vType.sql}"


    var df = spark.range(values).map(_ => Random.nextInt)
      .select($"value".cast(kType).as("key"), $"value".cast(vType).as("value"))

    var seqPath = new Path(tempDir, "data")
      .suffix(s"/sample-${kType.sql}-${vType.sql}").toString

    df.repartition(50).rdd
      .map(x => (x.getDouble(0), x.getInt(1))).saveAsSequenceFile(seqPath)


    spark.read.format("seq").load(seqPath)
      .createOrReplaceTempView(tableDF)

    var rdd: RDD[Row] = spark.sparkContext
      .sequenceFile[Double, Int](seqPath)
      .map(x => Row(x._1, x._2))

    var schema = new StructType()
      .add(StructField("key", kType, true))
      .add(StructField("value", vType, true))

    spark.createDataFrame(rdd, schema).createOrReplaceTempView(tableRDD)

    var condition = rdd.take(1).head.get(0).toString

    benchmark(spark, kType, vType, values, tableDF, tableRDD, "sum", condition)
    /*
    OpenJDK 64-Bit Server VM 1.8.0_282-8u282-b08-0ubuntu1~20.04-b08 on Linux 5.8.0-44-generic
    Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
    SQL Single DOUBLE Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Seq RDD                                       1633 / 1661          9.6         103.8       1.0X
    SeqDSv2                                       1316 / 1356         11.9          83.7       1.2X
    SeqDSv2 Vectorized                            1343 / 1359         11.7          85.4       1.2X


    SQL Two Column (DOUBLE,INT )  Scan:      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Seq RDD                                       1717 / 1718          9.2         109.2       1.0X
    SeqDSv2                                       1295 / 1310         12.1          82.3       1.3X
    SeqDSv2 Vectorized                            1381 / 1385         11.4          87.8       1.2X


    SQL NULL Filter DOUBLE Column:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Seq RDD                                       1673 / 1674          9.4         106.4       1.0X
    SeqDSv2                                       1318 / 1322         11.9          83.8       1.3X
    SeqDSv2 Vectorized                            1316 / 1344         12.0          83.6       1.3X


    SQL Filter DOUBLE Column :               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Seq RDD                                       1761 / 1761          8.9         111.9       1.0X
    SeqDSv2                                       1215 / 1235         12.9          77.3       1.4X
    SeqDSv2 Vectorized                            1294 / 1321         12.2          82.3       1.4X

     */

    // LongType & FloatType

    kType = LongType
    vType = FloatType
    tableRDD = s"tableRDD_${kType.sql}_${vType.sql}"
    tableDF = s"tableDF_${kType.sql}_${vType.sql}"


    df = spark.range(values).map(_ => Random.nextInt)
      .select($"value".cast(kType).as("key"), $"value".cast(vType).as("value"))

    seqPath = new Path(tempDir, "data")
      .suffix(s"/sample-${kType.sql}-${vType.sql}").toString

    df.repartition(50).rdd
      .map(x => (x.getLong(0), x.getFloat(1))).saveAsSequenceFile(seqPath)


    spark.read.format("seq").load(seqPath)
      .createOrReplaceTempView(tableDF)

    rdd = spark.sparkContext
      .sequenceFile[Long, Float](seqPath)
      .map(x => Row(x._1, x._2))

    schema = new StructType()
      .add(StructField("key", kType, true))
      .add(StructField("value", vType, true))

    spark.createDataFrame(rdd, schema).createOrReplaceTempView(tableRDD)

    condition = rdd.take(1).head.get(0).toString

    benchmark(spark, kType, vType, values, tableDF, tableRDD, "sum", condition)
    /*

    OpenJDK 64-Bit Server VM 1.8.0_282-8u282-b08-0ubuntu1~20.04-b08 on Linux 5.8.0-44-generic
    Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
    SQL Single BIGINT Column Scan:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Seq RDD                                       1779 / 1810          8.8         113.1       1.0X
    SeqDSv2                                       1383 / 1410         11.4          88.0       1.3X
    SeqDSv2 Vectorized                            1423 / 1426         11.1          90.4       1.3X


    SQL Two Column (BIGINT,FLOAT )  Scan:    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Seq RDD                                       1845 / 1851          8.5         117.3       1.0X
    SeqDSv2                                       1435 / 1448         11.0          91.3       1.3X
    SeqDSv2 Vectorized                            1542 / 1581         10.2          98.0       1.2X


    SQL NULL Filter BIGINT Column:           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Seq RDD                                       1705 / 1738          9.2         108.4       1.0X
    SeqDSv2                                       1294 / 1306         12.2          82.3       1.3X
    SeqDSv2 Vectorized                            1374 / 1377         11.4          87.4       1.2X


    SQL Filter BIGINT Column :               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Seq RDD                                       1822 / 1836          8.6         115.8       1.0X
    SeqDSv2                                       1354 / 1356         11.6          86.1       1.3X
    SeqDSv2 Vectorized                            1430 / 1432         11.0          90.9       1.3X
     */


    // BooleanType & IntType

    kType = BooleanType
    vType = IntegerType
    tableRDD = s"tableRDD_${kType.sql}_${vType.sql}"
    tableDF = s"tableDF_${kType.sql}_${vType.sql}"


    df = spark.range(values).map(_ => Random.nextBoolean)
      .withColumnRenamed("value", "key")
      .withColumn("value", when($"key" === true, 1).otherwise(0))

    seqPath = new Path(tempDir, "data")
      .suffix(s"/sample-${kType.sql}-${vType.sql}").toString

    df.repartition(100).rdd
      .map(x => (x.getBoolean(0), x.getInt(1))).saveAsSequenceFile(seqPath)


    spark.read.format("seq").load(seqPath)
      .createOrReplaceTempView(tableDF)

    rdd = spark.sparkContext
      .sequenceFile[Boolean, Int](seqPath)
      .map(x => Row(x._1, x._2))

    schema = new StructType()
      .add(StructField("key", kType, true))
      .add(StructField("value", vType, true))

    spark.createDataFrame(rdd, schema).createOrReplaceTempView(tableRDD)

    condition = rdd.take(1).head.get(0).toString

    benchmark(spark, kType, vType, values, tableDF, tableRDD, "count", condition)
  /*

    OpenJDK 64-Bit Server VM 1.8.0_282-8u282-b08-0ubuntu1~20.04-b08 on Linux 5.8.0-44-generic
    Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz

    SQL Single BOOLEAN Column Scan:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Seq RDD                                       1773 / 1783          8.9         112.7       1.0X
    SeqDSv2                                       1280 / 1300         12.3          81.4       1.4X
    SeqDSv2 Vectorized                            1315 / 1326         12.0          83.6       1.3X


    SQL Two Column (BOOLEAN,INT )  Scan:     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Seq RDD                                       1854 / 1860          8.5         117.9       1.0X
    SeqDSv2                                       1184 / 1208         13.3          75.3       1.6X
    SeqDSv2 Vectorized                            1350 / 1352         11.7          85.8       1.4X


    SQL NULL Filter BOOLEAN Column:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Seq RDD                                       1730 / 1735          9.1         110.0       1.0X
    SeqDSv2                                       1242 / 1249         12.7          79.0       1.4X
    SeqDSv2 Vectorized                            1272 / 1288         12.4          80.9       1.4X


    SQL Filter BOOLEAN Column :              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Seq RDD                                       3216 / 5145          4.9         204.5       1.0X
    SeqDSv2                                       2651 / 2738          5.9         168.5       1.2X
    SeqDSv2 Vectorized                            2548 / 2558          6.2         162.0       1.3X

   */
  }
}

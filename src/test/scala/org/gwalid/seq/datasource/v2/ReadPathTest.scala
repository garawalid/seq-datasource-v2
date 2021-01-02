package org.gwalid.seq.datasource.v2

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.sql.SparkSession

class ReadPathTest extends FunSuite with BeforeAndAfterAll {

  val seqFileGenerator = new SeqFileGenerator()
  val tempDirFile = Files.createTempDirectory(this.getClass.getName).toFile
  val tempDir: String = tempDirFile.toString

  override def beforeAll(): Unit = {
    // org.apache.log4j.BasicConfigurator.configure()
  }

  test("Read DataFrame : Int & Long") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val filePath = new Path(tempDir, "data").suffix("/sample.seq")

    seqFileGenerator.generateIntLong(filePath)

    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.format("seq").load(filePath.toString)

    println(s"The number of partitions is ${df.rdd.getNumPartitions}")
    val resultKey: Seq[Int] = df.select("key")
      .collect().map(_ (0).asInstanceOf[Int]).toSeq.sorted
    val expectedKey = seqFileGenerator.keyData.asInstanceOf[Seq[Int]].sorted

    val resultValue: Seq[Long] = df.select("value")
      .collect()
      .map(_ (0).asInstanceOf[Long]).toSeq.sorted
    val expectedValue = seqFileGenerator.valueData.asInstanceOf[Seq[Long]].sorted
    assert(resultKey == expectedKey)
    assert(resultValue == expectedValue)
  }

  test("Read DataFrame Float & Boolean") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    val filePath = new Path(tempDir, "data").suffix("/sample-float-boolean.seq")

    seqFileGenerator.generateFloatBoolean(filePath)

    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.format("seq").load(filePath.toString)

    val resultKey: Seq[Float] = df.select("key")
      .collect()
      .map(_ (0).asInstanceOf[Float]).toSeq.sorted
    val expectedKey = seqFileGenerator.keyData.asInstanceOf[Seq[Float]].sorted

    val resultValue: Seq[Boolean] = df.select("value")
      .collect()
      .map(_ (0).asInstanceOf[Boolean]).toSeq.sorted
    val expectedValue = seqFileGenerator.valueData.asInstanceOf[Seq[Boolean]].sorted

    assert(resultKey == expectedKey)
    assert(resultValue == expectedValue)
  }

  test("Read DataFrame Double & Int") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val filePath = new Path(tempDir, "data").suffix("/sample-double-int.seq")

    seqFileGenerator.generateDoubleInt(filePath)

    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.format("seq").load(filePath.toString)

    println(s"The number of partitions is ${df.rdd.getNumPartitions}")
    val resultKey: Seq[Double] = df.select("key")
      .collect()
      .map(_ (0).asInstanceOf[Double]).toSeq.sorted
    val expectedKey = seqFileGenerator.keyData.asInstanceOf[Seq[Double]].sorted

    val resultValue: Seq[Int] = df.select("value")
      .collect()
      .map(_ (0).asInstanceOf[Int]).toSeq.sorted
    val expectedValue = seqFileGenerator.valueData.asInstanceOf[Seq[Int]].sorted

    assert(resultKey == expectedKey)
    assert(resultValue == expectedValue)
  }

  test("Read DataFrame Null & Bytes") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val filePath = new Path(tempDir, "data").suffix("/sample-null-bytes.seq")

    seqFileGenerator.generateNullBytes(filePath)
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.format("seq").load(filePath.toString)

    val resultKey = df.select("key")
      .collect()
      .map(_ (0)).toSeq
    val expectedKey = seqFileGenerator.keyData

    val resultValue: Seq[String] = df.select("value")
      .collect()
      .map(_ (0).asInstanceOf[String]).toSeq.sorted
    val expectedValue: Seq[String] = seqFileGenerator.valueData.asInstanceOf[Seq[Seq[Byte]]]
      .map(x => new String(x.toArray[Byte], StandardCharsets.UTF_8)).sorted
    assert(resultKey == expectedKey)
    assert(resultValue == expectedValue)
  }

  test("Read DataFrame Text & Int") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val filePath = new Path(tempDir, "data").suffix("/sample-text-int.seq")

    seqFileGenerator.generateTextInt(filePath)

    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.format("seq").load(filePath.toString)

    val resultKey: Seq[String] = df.select("key")
      .collect()
      .map(_ (0).asInstanceOf[String]).toSeq.sorted
    val expectedKey = seqFileGenerator.keyData.asInstanceOf[Seq[String]].sorted

    val resultValue: Seq[Int] = df.select("value")
      .collect()
      .map(_ (0).asInstanceOf[Int]).toSeq.sorted
    val expectedValue = seqFileGenerator.valueData.asInstanceOf[Seq[Int]].sorted

    assert(resultKey == expectedKey)
    assert(resultValue == expectedValue)
  }

  test("ReadPath with multiple partitions") {

    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val dataPath = new Path(tempDir, "data").suffix("/samples")
    val nbFiles = 6
    val filesPath = for (i <- 0 until nbFiles) yield dataPath.suffix(s"/part-${i}")

    filesPath.foreach(filePath => seqFileGenerator.generateDoubleInt(filePath))

    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.format("seq").load(dataPath.toString)
    assert(df.rdd.getNumPartitions == nbFiles)
    assert(df.count() == nbFiles * 100)

  }


}

package org.gwalid.seq.datasource.v2

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, NullType, StringType, StructType}


class ReadPathWithSchema extends FunSuite {

  val seqFileGenerator = new SeqFileGenerator()
  val tempDirFile = Files.createTempDirectory(this.getClass.getName).toFile
  val tempDir: String = tempDirFile.toString


  test("Read Seq file with different types") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()


    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val filePath = new Path(tempDir, "data").suffix("/sample-int-long.seq")

    seqFileGenerator.generateIntLong(filePath)

    spark.sparkContext.setLogLevel("WARN")

    val customSchema = new StructType()
      .add("key", StringType, true)
      .add("value", StringType, true)

    assertThrows[IllegalArgumentException]{
      val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)
      df.show()
    }

  }

  test("Request only one column") {

    val spark = SparkSession.builder().master("local[1]").getOrCreate()


    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val filePath = new Path(tempDir, "data").suffix("/sample-int-long.seq")

    seqFileGenerator.generateIntLong(filePath)

    spark.sparkContext.setLogLevel("WARN")

    val customSchema = new StructType()
      .add("value", LongType, true)
    val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)

    val resultValue: Seq[Long] = df.select("value")
      .collect()
      .map(_ (0).asInstanceOf[Long]).toSeq.sorted
    val expectedValue = seqFileGenerator.valueData.asInstanceOf[Seq[Long]].sorted

    assert(df.schema == customSchema)
    assert(resultValue == expectedValue)

  }
  test("Read path with different names") {
    // Note: This test should raise an exception. We accept only key, value.

    val spark = SparkSession.builder().master("local[1]").getOrCreate()

    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val filePath = new Path(tempDir, "data").suffix("/sample-int-long.seq")

    seqFileGenerator.generateIntLong(filePath)

    spark.sparkContext.setLogLevel("WARN")

    val customSchema = new StructType()
      .add("col_1", IntegerType, true)
      .add("col_2", LongType, true)


    assertThrows[IllegalArgumentException] {
      val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)
      df.show()
    }

  }

  test("Read path with schema (IntegerType, LongType)") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()


    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val filePath = new Path(tempDir, "data").suffix("/sample-int-long.seq")

    seqFileGenerator.generateIntLong(filePath)

    spark.sparkContext.setLogLevel("WARN")

    val customSchema = new StructType()
      .add("key", IntegerType, true)
      .add("value", LongType, true)
    val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)

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

  test("Read path with schema (StringType, IntegerType)") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()

    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val filePath = new Path(tempDir, "data").suffix("/sample-text-int.seq")

    seqFileGenerator.generateTextInt(filePath)

    spark.sparkContext.setLogLevel("WARN")

    val customSchema = new StructType()
      .add("key", StringType, true)
      .add("value", IntegerType, true)
    val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)


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

  test("Read path with schema (NullType, StringType)") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()

    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val filePath = new Path(tempDir, "data").suffix("/sample-null-bytes.seq")

    seqFileGenerator.generateNullBytes(filePath)

    spark.sparkContext.setLogLevel("WARN")

    val customSchema = new StructType()
      .add("key", NullType, true)
      .add("value", StringType, true)
    val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)


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

  test("Read path with inverted schema (IntegerType, LongType)") {
    // Todo: Fix this test
    val spark = SparkSession.builder().master("local[1]").getOrCreate()


    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val filePath = new Path(tempDir, "data").suffix("/sample-int-long.seq")

    seqFileGenerator.generateIntLong(filePath)

    spark.sparkContext.setLogLevel("WARN")

    val customSchema = new StructType()
      .add("value", LongType, true)
      .add("key", IntegerType, true)
    val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)

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
}

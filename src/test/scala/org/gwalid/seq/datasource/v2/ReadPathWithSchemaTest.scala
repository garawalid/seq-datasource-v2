package org.gwalid.seq.datasource.v2

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Properties

import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, NullType, StringType, StructType}
import org.hadoop.io.SeqFileGenerator


class ReadPathWithSchemaTest extends FunSuite with BeforeAndAfterAll {

  val seqFileGenerator = new SeqFileGenerator()
  val tempDirFile = Files.createTempDirectory(this.getClass.getName).toFile
  val tempDir: String = tempDirFile.toString
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val prop = new Properties()
    prop.setProperty("log4j.rootLogger", "WARN")
    org.apache.log4j.PropertyConfigurator.configure(prop)

    spark = SparkSession.builder().master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  test("Read Seq file with different types") {
    val filePath = new Path(tempDir, "data").suffix("/sample-int-long.seq")
    seqFileGenerator.generateIntLong(filePath)

    val customSchema = new StructType()
      .add("key", StringType, true)
      .add("value", StringType, true)

    assertThrows[IllegalArgumentException] {
      val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)
      df.show()
    }

  }

  test("Request only one column") {
    val filePath = new Path(tempDir, "data").suffix("/sample-int-long.seq")
    seqFileGenerator.generateIntLong(filePath)

    val customSchema = new StructType()
      .add("value", LongType, true)
    val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)

    assert(df.schema == customSchema)
    assert(SeqAssertHelper.getValueDataAs[Long](df) == seqFileGenerator.getValueDataAs[Long])

  }
  test("Read path with different names") {
    // Note: This test should raise an exception. We accept only key, value.
    val filePath = new Path(tempDir, "data").suffix("/sample-int-long.seq")
    seqFileGenerator.generateIntLong(filePath)

    val customSchema = new StructType()
      .add("col_1", IntegerType, true)
      .add("col_2", LongType, true)

    assertThrows[IllegalArgumentException] {
      val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)
      df.show()
    }

  }

  test("Read path with schema (IntegerType, LongType)") {
    val filePath = new Path(tempDir, "data").suffix("/sample-int-long.seq")
    seqFileGenerator.generateIntLong(filePath)

    val customSchema = new StructType()
      .add("key", IntegerType, true)
      .add("value", LongType, true)
    val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)

    assert(SeqAssertHelper.getKeyDataAs[Int](df) == seqFileGenerator.getKeyDataAs[Int])
    assert(SeqAssertHelper.getValueDataAs[Long](df) == seqFileGenerator.getValueDataAs[Long])
  }

  test("Read path with schema (StringType, IntegerType)") {
    val filePath = new Path(tempDir, "data").suffix("/sample-text-int.seq")
    seqFileGenerator.generateTextInt(filePath)

    val customSchema = new StructType()
      .add("key", StringType, true)
      .add("value", IntegerType, true)
    val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)

    assert(SeqAssertHelper.getKeyDataAs[String](df) == seqFileGenerator.getKeyDataAs[String])
    assert(SeqAssertHelper.getValueDataAs[Int](df) == seqFileGenerator.getValueDataAs[Int])
  }

  test("Read path with schema (NullType, StringType)") {
    val filePath = new Path(tempDir, "data").suffix("/sample-null-bytes.seq")
    seqFileGenerator.generateNullBytes(filePath)

    val customSchema = new StructType()
      .add("key", NullType, true)
      .add("value", StringType, true)
    val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)

    assert(SeqAssertHelper.getKeyData(df) == seqFileGenerator.getKeyData)
    assert(SeqAssertHelper.getValueDataAs[String](df) == seqFileGenerator.getValueDataAsString)
  }

  test("Read path with inverted schema (IntegerType, LongType)") {
    val filePath = new Path(tempDir, "data").suffix("/sample-int-long.seq")
    seqFileGenerator.generateIntLong(filePath)

    val customSchema = new StructType()
      .add("value", LongType, true)
      .add("key", IntegerType, true)
    val df = spark.read.format("seq").schema(customSchema).load(filePath.toString)

    assert(SeqAssertHelper.getKeyDataAs[Int](df) == seqFileGenerator.getKeyDataAs[Int])
    assert(SeqAssertHelper.getValueDataAs[Long](df) == seqFileGenerator.getValueDataAs[Long])
  }
}

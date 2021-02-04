package org.gwalid.seq.datasource.v2

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Properties

import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.SparkSession

class ReadPathTest extends FunSuite with BeforeAndAfterAll {
  val seqFileGenerator = new SeqFileGenerator()
  val tempDirFile = Files.createTempDirectory(this.getClass.getName).toFile
  val tempDir: String = tempDirFile.toString
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val prop = new Properties()
    prop.setProperty("log4j.rootLogger", "WARN")
    org.apache.log4j.PropertyConfigurator.configure(prop)

    spark = SparkSession.builder().master("local[1]")
      .config("spark.sql.seq.enableVectorizedReader", "false").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  test("Read DataFrame : Int & Long") {
    val filePath = new Path(tempDir, "data").suffix("/sample-int-long.seq")
    seqFileGenerator.generateIntLong(filePath)
    val df = spark.read.format("seq").load(filePath.toString)

    assert(SeqAssertHelper.getKeyDataAs[Int](df) == seqFileGenerator.getKeyDataAs[Int])
    assert(SeqAssertHelper.getValueDataAs[Long](df) == seqFileGenerator.getValueDataAs[Long])
  }

  test("Read DataFrame Float & Boolean") {
    val filePath = new Path(tempDir, "data").suffix("/sample-float-boolean.seq")
    seqFileGenerator.generateFloatBoolean(filePath)
    val df = spark.read.format("seq").load(filePath.toString)

    assert(SeqAssertHelper.getKeyDataAs[Float](df) == seqFileGenerator.getKeyDataAs[Float])
    assert(SeqAssertHelper.getValueDataAs[Boolean](df) == seqFileGenerator.getValueDataAs[Boolean])
  }

  test("Read DataFrame Double & Int") {

    val filePath = new Path(tempDir, "data").suffix("/sample-double-int.seq")
    seqFileGenerator.generateDoubleInt(filePath)
    val df = spark.read.format("seq").load(filePath.toString)

    assert(SeqAssertHelper.getKeyDataAs[Double](df) == seqFileGenerator.getKeyDataAs[Double])
    assert(SeqAssertHelper.getValueDataAs[Int](df) == seqFileGenerator.getValueDataAs[Int])
  }

  test("Read DataFrame Null & Bytes") {
    val filePath = new Path(tempDir, "data").suffix("/sample-null-bytes.seq")
    seqFileGenerator.generateNullBytes(filePath)
    val df = spark.read.format("seq").load(filePath.toString)

    assert(SeqAssertHelper.getKeyData(df) == seqFileGenerator.getKeyData)
    assert(SeqAssertHelper.getValueDataAs[String](df) == seqFileGenerator.getValueDataAsString)
  }

  test("Read DataFrame Text & Int") {
    val filePath = new Path(tempDir, "data").suffix("/sample-text-int.seq")
    seqFileGenerator.generateTextInt(filePath)
    val df = spark.read.format("seq").load(filePath.toString)

    assert(SeqAssertHelper.getKeyDataAs[String](df) == seqFileGenerator.getKeyDataAs[String])
    assert(SeqAssertHelper.getValueDataAs[Int](df) == seqFileGenerator.getValueDataAs[Int])
  }

  test("ReadPath with multiple partitions") {
    val dataPath = new Path(tempDir, "data").suffix("/samples")
    val nbFiles = 6
    val filesPath = for (i <- 0 until nbFiles) yield dataPath.suffix(s"/part-${i}")
    filesPath.foreach(filePath => seqFileGenerator.generateDoubleInt(filePath))
    val df = spark.read.format("seq").load(dataPath.toString)

    assert(df.rdd.getNumPartitions == nbFiles)
    assert(df.count() == nbFiles * 100)

  }


}

package org.gwalid.seq.datasource.v2

import java.nio.file.Files
import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class VectorizedReadPathTest extends FunSuite with BeforeAndAfterAll{

  val seqFileGenerator = new SeqFileGenerator()
  val tempDirFile = Files.createTempDirectory(this.getClass.getName).toFile
  val tempDir: String = tempDirFile.toString
  var spark: SparkSession = _


  override def beforeAll(): Unit = {
    val prop = new Properties()
    prop.setProperty("log4j.rootLogger", "WARN")
    org.apache.log4j.PropertyConfigurator.configure(prop)

    spark = SparkSession.builder().master("local[1]")
      .config("spark.sql.seq.enableVectorizedReader", "true").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  test("Dummy test") {

    val filePath = new Path(tempDir, "data").suffix("/sample.seq")

    seqFileGenerator.generateIntLong(filePath)

    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.format("seq").load(filePath.toString)

    df.show(10)
    println(s"df count ${df.count()}")

  }


  test("Read DataFrame : Int & Long") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val filePath = new Path(tempDir, "data").suffix("/sample-int-long.seq")

    seqFileGenerator.generateIntLong(filePath)
    spark.sparkContext.setLogLevel("WARN")
    val df = spark.read.format("seq").load(filePath.toString)

    assert(SeqAssertHelper.getKeyDataAs[Int](df) == seqFileGenerator.getKeyDataAs[Int])
    assert(SeqAssertHelper.getValueDataAs[Long](df) == seqFileGenerator.getValueDataAs[Long])
  }

  test("Fallback test") {
    // Fallback test when vectorized read path is not supported
  }

}

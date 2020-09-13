package org.gwalid.seq.datasource.v2

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
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

    df.show()
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

  test("Read DataFrame Float, Boolean") {
    // Fixme: Compelte this test
    val filePath = new Path(tempDir, "data").suffix("/sample-float-boolean.seq")
    seqFileGenerator.generateFloatBoolean(filePath)
  }

  test("list files") {
    // Todo: delete me!
    val filePath = new Path("tmp", "data").suffix("/sample-float-boolean.seq")
    seqFileGenerator.generateFloatBoolean(filePath)
    val resFiles: Seq[FileStatus] = filePath.getFileSystem(new Configuration())
      .listStatus(new Path(tempDir, "data")).toSeq

    val seqPattern = ".seq$".r

    resFiles.filter(file => seqPattern.findFirstIn(file.getPath.toString).isDefined)
      .foreach(x => println(x.getPath))
  }

  test("Read DataFrame Double & Int") {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    org.apache.log4j.BasicConfigurator.configure() // Fixme
    val filePath = new Path(tempDir, "data").suffix("/sample-double-int.seq")

    seqFileGenerator.generateDoubleInt(filePath)

    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.format("seq").load(filePath.toString)

    df.show()
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
    assert(df.count() == nbFiles * 100 )

  }

  test("ReadPath with multiple partitions (old)") {
    // Todo: Move this to jmh
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    org.apache.log4j.BasicConfigurator.configure() // Fixme

    spark.sparkContext.setLogLevel("WARN")

    // Test path with / and without.
    val paths = Seq("/home/nops/Projects/data/ml-10M-seq", "/home/nops/Projects/data/ml-10M-seq/")

    paths.foreach(path => {
      val df = spark.read.format("seq").load(path)
      // fixme: Enable tests
      // assert(df.count() == 10000054)
      // assert(df.rdd.getNumPartitions == 300)

    })


    val df = spark.read.format("seq").load()
    df.show()
    println(s"count : ${df.count()}")
    assert(df.count() == 10000054)
    assert(df.rdd.getNumPartitions == 300)


  }


}

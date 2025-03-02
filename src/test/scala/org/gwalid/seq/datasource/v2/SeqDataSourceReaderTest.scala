package org.gwalid.seq.datasource.v2

import java.nio.file.Files
import java.util

import scala.util.Random

import org.apache.hadoop.fs.Path
import org.hadoop.io.SeqFileGenerator
import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.v2.DataSourceOptions

class SeqDataSourceReaderTest extends FunSuite {
  val seqFileGenerator = new SeqFileGenerator()
  val tempDirFile = Files.createTempDirectory(this.getClass.getName).toFile
  val tempDir: String = tempDirFile.toString

  test("fsListAllSeqFiles") {
    val dataPath = new Path(tempDir, "data")

    val textFilePath = dataPath.suffix("/sample-il.text")
    val floatBooleanFilePath = dataPath.suffix("/sample-fb.seq")
    val IntLongFilePath = dataPath.suffix("/sample-il.seq")

    seqFileGenerator.generateFloatBoolean(floatBooleanFilePath)
    seqFileGenerator.generateIntLong(IntLongFilePath)
    generateRandomTextFile(textFilePath)

    val optionsMap: util.Map[String, String] = new util.HashMap[String, String]()
    optionsMap.put("path", dataPath.toString)
    val options: DataSourceOptions = new DataSourceOptions(optionsMap)

    // The SeqDataSourceReader need an active spark context
    val _ = SparkSession.builder().master("local[1]").getOrCreate()
    val seqDSReader = new SeqDataSourceReader(options)

    val files = seqDSReader
      .listAllFiles()
      .map(_.toString.replace("file:", ""))
      .sorted

    val expectedFiles =
      Seq(textFilePath.toString, floatBooleanFilePath.toString, IntLongFilePath.toString).sorted
    assert(files == expectedFiles)
  }

  private def generateRandomTextFile(path: Path): Unit = {

    val writer = new java.io.PrintWriter(path.toString)
    var i = 0;
    while (i < 10) {
      writer.write(Random.alphanumeric.take(10).mkString(""))
      i = i + 1
    }
    writer.close()
  }

}

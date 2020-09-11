package org.gwalid.SeqDataSourceV2

import java.nio.file.Files
import java.util
import java.util.Map

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.scalatest.FunSuite

import scala.util.Random

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

    val seqDSReader = new SeqDataSourceReader(options)

    val files = seqDSReader
      .listAllFiles()
      .map(_.toString.replace("file:",""))
      .sorted

    val expectedFiles = Seq(textFilePath.toString,floatBooleanFilePath.toString,IntLongFilePath.toString).sorted
    assert(files == expectedFiles)
  }

  def generateRandomTextFile(path: Path): Unit = {

    val writer = new java.io.PrintWriter(path.toString)
    var i = 0;
    while (i < 10) {
      writer.write(Random.alphanumeric.take(10).mkString(""))
      i = i + 1
    }
    writer.close()
  }

}

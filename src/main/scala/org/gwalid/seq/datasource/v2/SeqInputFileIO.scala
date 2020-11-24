package org.gwalid.seq.datasource.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class SeqInputFileIO(path: String) extends Serializable {

  private lazy val currentPath: Path = new Path(path)
  private lazy val sizeInBytes = computeSizeInBytes()

  def getPath: Path = currentPath

  def getSizeInByte: Long = sizeInBytes

  def computeSizeInBytes(): Long = {

    val conf = new Configuration() // Fixme: Get it from SparkSession instead
    val fs = currentPath.getFileSystem(conf)
    fs.listStatus(currentPath).head.getLen
  }

  def getNumRows: Int = {
    // Estimates the number of rows: We divide the total size by 30 Bytes
    math.ceil(sizeInBytes / 30).toInt
  }



}

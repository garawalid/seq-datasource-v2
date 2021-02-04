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
    val size = fs.listStatus(currentPath).head.getLen
    size
  }

  def getNumRows: Long = {
    // Estimates the number of rows: We divide the total size by 30 Bytes
    println(s"getNumRows: ${sizeInBytes / 30L}")
    sizeInBytes / 30L
  }




}

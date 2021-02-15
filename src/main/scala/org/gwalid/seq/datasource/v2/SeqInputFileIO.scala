package org.gwalid.seq.datasource.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

class SeqInputFileIO(uri: String) extends Serializable {

  private lazy val sizeInBytes = computeSizeInBytes()

  def getURI: String = uri

  def getSizeInByte: Long = sizeInBytes

  def computeSizeInBytes(): Long = {
    val conf = SparkSession.active.sessionState.newHadoopConf()
    val currentPath: Path = new Path(uri)
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

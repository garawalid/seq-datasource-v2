package org.gwalid.SeqDataSourceV2

import java.util

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{SequenceFile, Writable}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class SeqDataSourceReader(options: DataSourceOptions) extends DataSourceReader {

  val conf = SparkSession.active.sessionState.newHadoopConf()
  val filesPath = listAllFiles()

  // Todo: Maybe we don't need the broadcasting
  val seqFileIO:Broadcast[Seq[SeqInputFileIO]] = SparkSession.active.sparkContext.broadcast(listAllSeqFiles())


  override def readSchema(): StructType = {
    val kvTypes = getDataSchema()
    val keySparkType = TypeHelper.convertHadoopToSpark(kvTypes.head)
    val valueSparkType = TypeHelper.convertHadoopToSpark(kvTypes.last)
    new StructType()
      .add(StructField("key", keySparkType, nullable = true))
      .add(StructField("value", valueSparkType, nullable = true))
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val inputPartitions = new util.ArrayList[InputPartition[InternalRow]]();
    if (options.paths.length == 0) throw new RuntimeException("There is no path to read. Please set a Path.") // fixme: move it to a proper place
    // fixme: Read only one path

    seqFileIO.value.foreach( seqInputFile => inputPartitions.add(new SeqInputPartition(seqInputFile)))

    inputPartitions
  }

  // Todo: Delete this method
  def getDataSchemaOld(paths: Array[String]): Seq[Class[_ <:Writable]] = {
    /*
    Collect the schema from random samples.

     */
    // Quick fix: collect the schema from the first file.

    val filePath = new Path(paths(0))
    val conf = new Configuration

    val fileOption = SequenceFile.Reader.file(filePath)
    val bufferOption = SequenceFile.Reader.bufferSize(10) // Todo : adjust this value!

    // use OnlyHeaderOption to speed up schema collecting!
    val reader = new SequenceFile.Reader(conf, fileOption, bufferOption)

    val keyClass = reader.getKeyClass.asSubclass(classOf[Writable])
    val valueClass = reader.getValueClass.asSubclass(classOf[Writable])
    reader.close()

    Seq(keyClass, valueClass)

  }

  def listAllFiles(): Seq[Path] ={
    val inputPaths:Array[Path] = options.paths().map(new Path(_))
    if(!inputPaths.isEmpty) {
      val fs = inputPaths.head.getFileSystem(conf)
      fs.listStatus(inputPaths).map(x => x.getPath)
    }else{
      Seq.empty[Path]
    }
  }

  def listAllSeqFiles(): Seq[SeqInputFileIO] ={
    val inputPaths:Array[Path] = options.paths().map(new Path(_))
    if(!inputPaths.isEmpty) {
      val fs = inputPaths.head.getFileSystem(conf)
      // List all files and exclude those who starts with _ like _SUCCESS
      fs.listStatus(inputPaths)
        .filter(x => (x.isFile) && (!FilenameUtils.getBaseName(x.getPath.toString).startsWith("_")) )
        .map(x => new SeqInputFileIO(x.getPath.toString))
    }else{
      Seq.empty[SeqInputFileIO]
    }
  }

  private def getDataSchema(): Seq[Class[_ <:Writable]] ={
    // Given a list of path files, check if 10% of these files are valid sequence format and they have the same schema

    // get 2% samples from all files.
    val io = seqFileIO.value
    val maxFiles = Math.ceil(io.length * 0.02).toInt

    var kClassSample = ArrayBuffer.empty[Class[_<:Writable]]
    var vClassSample = ArrayBuffer.empty[Class[_ <:Writable]]

    for(i <- 0 until maxFiles){
      val randomIndex = Random.nextInt(io.length)
      val pathFile = io(randomIndex).getPath()

      val fileOption = SequenceFile.Reader.file(pathFile)
      val bufferOption = SequenceFile.Reader.bufferSize(500) // Todo : adjust this value!
      // Todo: Speed reading with OnlyHeaderOption.class
      val reader = new SequenceFile.Reader(conf, fileOption, bufferOption)
      kClassSample += reader.getKeyClass.asSubclass(classOf[Writable])
      vClassSample += reader.getValueClass.asSubclass(classOf[Writable])
      reader.close()

    }

    // keyClass and valueClass should be the same for the sample

    val sameKClass = kClassSample.map(kClass =>  kClass == kClassSample.head).reduce(_&&_)
    val sameVClass = vClassSample.map(vClass => vClass == vClassSample.head).reduce(_&&_)
    if(!sameKClass | !sameVClass){
      throw new RuntimeException("The sequence files doesn't have the same schema!")
    }

    Seq(kClassSample.head, vClassSample.head)
  }

}

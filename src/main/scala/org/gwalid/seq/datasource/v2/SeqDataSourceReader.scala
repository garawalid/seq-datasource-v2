package org.gwalid.seq.datasource.v2

import java.util

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{SequenceFile, Writable}

import org.apache.spark.SerializableWritable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, Statistics, SupportsReportStatistics, SupportsScanColumnarBatch}
import org.apache.spark.sql.types.{NullType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch


class SeqDataSourceReader(options: DataSourceOptions,
                          requestedSchema: Option[StructType] = None)
  extends DataSourceReader with SupportsReportStatistics
    with SupportsScanColumnarBatch with Logging {

  private lazy val conf = SparkSession.active.sessionState.newHadoopConf()
  private lazy val serializableConf = new SerializableWritable[Configuration](conf)
  private val filesPath = listAllFiles()
  private val seqFileIO: Seq[SeqInputFileIO] = listAllSeqFiles()

  lazy val dataSchema: StructType = getDataSchema


  override def readSchema(): StructType = {
    schemaSanity()

    if (requestedSchema.isDefined) {
      requestedSchema.get
    } else {
      dataSchema
    }
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val inputPartitions = new util.ArrayList[InputPartition[InternalRow]]()
    seqFileIO.foreach(seqInputFile =>
      inputPartitions.add(new SeqInputPartition(seqInputFile, requestedSchema, serializableConf)))

    inputPartitions
  }

  override def planBatchInputPartitions(): util.List[InputPartition[ColumnarBatch]] = {
    val batchInputPartitions = new util.ArrayList[InputPartition[ColumnarBatch]]()

    val vrBatchSize = getColumnarReaderBatchSize()
    seqFileIO.foreach(seqInputFile => {
      val ip = new SeqBatchInputPartition(seqInputFile, vrBatchSize, serializableConf)
      batchInputPartitions.add(ip)
    })

    batchInputPartitions
  }

  def listAllFiles(): Seq[Path] = {
    val inputPaths: Array[Path] = options.paths().map(new Path(_))
    if (!inputPaths.isEmpty) {
      val fs = inputPaths.head.getFileSystem(conf)
      fs.listStatus(inputPaths).map(x => x.getPath)
    } else {
      Seq.empty[Path]
    }
  }

  def listAllSeqFiles(): Seq[SeqInputFileIO] = {
    val inputPaths: Array[Path] = options.paths().map(new Path(_))
    if (!inputPaths.isEmpty) {
      val fs = inputPaths.head.getFileSystem(conf)
      // List all files and exclude those who starts with _ like _SUCCESS
      fs.listStatus(inputPaths)
        .filter(x => (x.isFile) && (!FilenameUtils.getBaseName(x.getPath.toString).startsWith("_")))
        .map(x => new SeqInputFileIO(x.getPath.toString))
    } else {
      Seq.empty[SeqInputFileIO]
    }
  }


  private def getDataSchema: StructType = {
    // If requestedSchema is None: Given a list of path files, check if 10% of these files are
    // valid sequence format and they have the same schema
    // If requestedSchema is not None: Collect the schema only from one file.

    // get 2% samples from all files.
    val maxFiles = requestedSchema match {
      case None => Math.ceil(seqFileIO.length * 0.02).toInt
      case _ => 1
    }

    var kClassSample = ArrayBuffer.empty[Class[_ <: Writable]]
    var vClassSample = ArrayBuffer.empty[Class[_ <: Writable]]

    for (_ <- 0 until maxFiles) {
      val randomIndex = Random.nextInt(seqFileIO.length)
      val pathFile = new Path(seqFileIO(randomIndex).getURI)

      val fileOption = SequenceFile.Reader.file(pathFile)
      // Todo: Speed reading with OnlyHeaderOption.class
      val reader = new SequenceFile.Reader(conf, fileOption)
      kClassSample += reader.getKeyClass.asSubclass(classOf[Writable])
      vClassSample += reader.getValueClass.asSubclass(classOf[Writable])
      reader.close()

    }
    assert(kClassSample.nonEmpty && vClassSample.nonEmpty)

    // keyClass and valueClass should be the same for the sample
    val sameKClass = kClassSample.map(kClass => kClass == kClassSample.head).reduce(_ && _)
    val sameVClass = vClassSample.map(vClass => vClass == vClassSample.head).reduce(_ && _)
    if (!sameKClass | !sameVClass) {
      throw new RuntimeException("The sequence files doesn't have the same schema!")
    }

    val kvTypes = Seq(kClassSample.head, vClassSample.head)

    val keySparkType = TypeHelper.convertHadoopToSpark(kvTypes.head)
    val valueSparkType = TypeHelper.convertHadoopToSpark(kvTypes.last)
    new StructType()
      .add(StructField("key", keySparkType, nullable = true))
      .add(StructField("value", valueSparkType, nullable = true))
  }

  private def schemaSanity(): Unit = {
    // Assert that the requestedSchema is the same as the dataSchema

    // Check if the requestedSchema respects the name rules.
    // The name rule : The tuple (key, value) is mandatory because it helps us distinguish
    // what we are requesting (whether the key or the value in the Seq file).
    if (requestedSchema.isDefined) {
      val requestedNames = requestedSchema.get.map(_.name)
      requestedNames.length match {
        case 2 => if (requestedNames.toSet != Set("key", "value")) throw new
            IllegalArgumentException(s"The name of requested schema should be 'key' and 'value'." +
              s" The current names are ${requestedNames.head}, ${requestedNames.last}.")
        case 1 => if (requestedNames != Seq("key") && requestedNames != Seq("value")) throw
          new IllegalArgumentException(s"The name of requested schema should be 'key' or 'value'." +
            s" The current name is ${requestedNames.head}.")
        case _ => throw new IllegalArgumentException("The requested schema is empty.")
      }
    }

    // Check the requestedTypes against the dataTypes
    if (requestedSchema.isDefined) {
      val reqTypes = requestedSchema.get.map(_.dataType)
      val currTypes = dataSchema.map(_.dataType)
      val isValidSchema = reqTypes.length match {
        case len if len == 2 && requestedSchema.get.head.name == "key" =>
          reqTypes.head == currTypes.head && reqTypes.last == currTypes.last
        case len if len == 2 && requestedSchema.get.head.name == "value" =>
          reqTypes.head == currTypes.last && reqTypes.last == currTypes.head
        case len if len == 1 && requestedSchema.get.head.name == "key" =>
          reqTypes.head == currTypes.head
        case len if len == 1 && requestedSchema.get.head.name == "value" =>
          reqTypes.last == currTypes.last
        case 0 => true
        case _ => false
      }

      if (!isValidSchema) throw new IllegalArgumentException("The requested schema " +
        "should have the same type as the data schema. " +
        s"The requested schema: ${requestedSchema.get}, the data schema: ${dataSchema}")


    }

  }

  override def estimateStatistics(): Statistics = {
    // estimate Statistics for all files
    val totalSizeInBytes = seqFileIO.map(f => f.getSizeInByte).sum
    val totalNumRows = seqFileIO.map(f => f.getNumRows).sum
    new SeqDataSourceStatistics(totalSizeInBytes, totalNumRows.toLong)
  }

  override def enableBatchRead(): Boolean = {
    val enableVectorizedReader = SparkSession.active.conf
      .getOption("spark.sql.seq.enableVectorizedReader")
    if (enableVectorizedReader.isDefined) {
      if (enableVectorizedReader.get.toLowerCase == "true") {
        val types = dataSchema.map(_.dataType)
        val supportedTypes = !types.contains(StringType) && !types.contains(NullType)
        supportedTypes match {
          case true => logDebug("Vectorized read path is enabled")
          case false => logWarning("Cannot enable vectorized read path. Falling " +
            "back to normal read path")
        }
        supportedTypes
      } else {
        false
      }
    } else {
      false
    }
  }

  private def getColumnarReaderBatchSize(): Int = {
    val batchSize = SparkSession.active.conf.
      get("spark.sql.seq.columnarReaderBatchSize", "4096")

    try {
      batchSize.toInt
    } catch {
      case _: Exception => logWarning(
        s"""Cannot cast spark.sql.seq.columnarReaderBatchSize:
           | ${batchSize} to integer. This property is set to default
           | value: 4096""".stripMargin.replaceAll("\n", ""))
        4096
    }

  }

}

package org.gwalid.SeqDataSourceV2

import java.io.EOFException
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ByteWritable, BytesWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, NullWritable, SequenceFile, Text, Writable}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.DoubleWriters


class SeqInputPartitionReader(seqInputFile:SeqInputFileIO ) extends InputPartitionReader[InternalRow] {

  val conf = new Configuration() // Fixme

  val reader: SequenceFile.Reader = getReader
  var key: Writable = ReflectionUtils.newInstance(reader.getKeyClass, conf).asInstanceOf[Writable]
  var value: Writable = ReflectionUtils.newInstance(reader.getValueClass, conf).asInstanceOf[Writable]

  override def next(): Boolean = {
try {
  reader.next(key, value)
    } catch {
      case _: EOFException => false
      case e => throw e
    }

  }

  override def get(): InternalRow = {

    InternalRow(extractValue(key), extractValue(value))


  }

  override def close(): Unit = reader.close()

  private def getReader: SequenceFile.Reader = {

    val fileOption = SequenceFile.Reader.file(seqInputFile.getPath())
    val bufferOption = SequenceFile.Reader.bufferSize(500) // Todo : adjust this value!

    new SequenceFile.Reader(conf, fileOption, bufferOption)

  }


  private def extractValue(writable: Writable) = {
    // Todo: Add other types
    writable match {
      case x: LongWritable => x.get()
      case x: DoubleWritable => x.get()
      case x: FloatWritable => x.get()
      case x: IntWritable => x.get()
      case x: NullWritable => null
      case x: BytesWritable => UTF8String.fromString(new String(x.getBytes)) // fixme: from Bytes => UTF8String directly
      case x : Text => UTF8String.fromString(x.toString)
      case x => throw new RuntimeException(s"${x} is not implemented yet!")
    }

  }

}

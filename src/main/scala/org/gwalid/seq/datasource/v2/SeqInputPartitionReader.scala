package org.gwalid.seq.datasource.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.unsafe.types.UTF8String


class SeqInputPartitionReader(seqInputFile: SeqInputFileIO)
  extends InputPartitionReader[InternalRow] {

  val conf = new Configuration() // Todo: Ship the conf from the driver to the executor!

  val reader: SequenceFile.Reader = getReader
  var key: Writable = WritableHelper
    .newInstance(reader.getKeyClass.asInstanceOf[Class[Writable]], conf)

  var value: Writable = WritableHelper
    .newInstance(reader.getValueClass.asInstanceOf[Class[Writable]], conf)

  override def next(): Boolean = reader.next(key, value)

  override def get(): InternalRow = InternalRow(extractValue(key), extractValue(value))

  override def close(): Unit = reader.close()

  private def getReader: SequenceFile.Reader = {

    val fileOption = SequenceFile.Reader.file(seqInputFile.getPath)

    new SequenceFile.Reader(conf, fileOption)

  }


  private def extractValue(writable: Writable) = {
    // Todo: move it to WritableHelper
    // Todo: Add ArrayWritable, ByteWritable, MapWritable
    writable match {
      case x: LongWritable => x.get()
      case x: DoubleWritable => x.get()
      case x: FloatWritable => x.get()
      case x: IntWritable => x.get()
      case x: BooleanWritable => x.get()
      case _: NullWritable => null
      case x: BytesWritable => UTF8String.fromBytes(x.copyBytes())
      case x: Text => UTF8String.fromString(x.toString)
      case x: ArrayWritable => extractFromArrayOfWritable(x.get())
      case x => throw new RuntimeException(s"${x.getClass} is not implemented yet!")
    }

  }

  def extractFromArrayOfWritable(arrayWritable: Array[Writable]): ArrayData = {
    // Always cast the element of the array to String.
    val arrayStr = arrayWritable
      .map(extractValue(_).toString)
      .map(UTF8String.fromString)
    ArrayData.toArrayData(arrayStr)
  }
}

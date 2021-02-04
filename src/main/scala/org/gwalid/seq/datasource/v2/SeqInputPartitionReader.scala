package org.gwalid.seq.datasource.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType


class SeqInputPartitionReader(seqInputFile: SeqInputFileIO, requestedSchema: Option[StructType])
  extends InputPartitionReader[InternalRow] {

  val conf = new Configuration() // Todo: Ship the conf from the driver to the executor!

  val reader: SequenceFile.Reader = getReader
  var key: Writable = WritableHelper
    .newInstance(reader.getKeyClass.asInstanceOf[Class[Writable]], conf)

  var value: Writable = WritableHelper
    .newInstance(reader.getValueClass.asInstanceOf[Class[Writable]], conf)

  override def next(): Boolean = reader.next(key, value)

  override def get(): InternalRow = projectInternalRow()

  override def close(): Unit = reader.close()

  private def getReader: SequenceFile.Reader = {

    val fileOption = SequenceFile.Reader.file(seqInputFile.getPath)

    new SequenceFile.Reader(conf, fileOption)

  }

  private def projectInternalRow(): InternalRow = {
    // Project the schema and build the correct InternalRow.
    if (requestedSchema.isDefined) {
      if (requestedSchema.get.length == 1) {
        val requestedField = requestedSchema.get.last.name
        val internalRow = requestedField match {
          case "key" => InternalRow(WritableHelper.extractValue(key))
          case "value" => InternalRow(WritableHelper.extractValue(value))
        }
        internalRow
      } else {
        // Assure the correct order
        val schemaNames = requestedSchema.get.map(_.name)
        schemaNames match {
          case Seq("key", "value") =>
            InternalRow(WritableHelper.extractValue(key), WritableHelper.extractValue(value))
          case Seq("value", "key") =>
            InternalRow(WritableHelper.extractValue(value), WritableHelper.extractValue(key))
        }

      }
    } else {
      InternalRow(WritableHelper.extractValue(key), WritableHelper.extractValue(value))
    }

  }


}

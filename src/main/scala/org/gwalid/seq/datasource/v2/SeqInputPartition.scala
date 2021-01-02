package org.gwalid.seq.datasource.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

class SeqInputPartition(seqInputFileIO: SeqInputFileIO, requestedSchema: Option[StructType])
  extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new SeqInputPartitionReader(seqInputFileIO, requestedSchema);
  }
}

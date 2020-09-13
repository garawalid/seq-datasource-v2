package org.gwalid.seq.datasource.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}

class SeqInputPartition(seqInputFileIO: SeqInputFileIO) extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new SeqInputPartitionReader(seqInputFileIO);
  }
}

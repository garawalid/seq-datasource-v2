package org.gwalid.seq.datasource.v2

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SerializableWritable
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.vectorized.ColumnarBatch

class SeqBatchInputPartition(seqInputFileIO: SeqInputFileIO, vectorizedReaderBatchSize: Int,
                             serializableConf: SerializableWritable[Configuration])
  extends InputPartition[ColumnarBatch] {

  override def createPartitionReader(): InputPartitionReader[ColumnarBatch] = {
    new SeqBatchInputPartitionReader(seqInputFileIO, vectorizedReaderBatchSize, serializableConf)
  }
}

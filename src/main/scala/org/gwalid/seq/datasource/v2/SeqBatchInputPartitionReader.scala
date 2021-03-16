package org.gwalid.seq.datasource.v2

import java.util

import scala.collection.JavaConversions._

import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{SequenceFile, Writable}

import org.apache.spark.SerializableWritable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}



class SeqBatchInputPartitionReader(seqInputFileIO: SeqInputFileIO, vectorizedReaderBatchSize: Int,
                                   serializableConf: SerializableWritable[Configuration])
  extends InputPartitionReader[ColumnarBatch] with Logging {

  private val reader = getReader

  lazy private val conf = serializableConf.value

  private val keyClass: Class[Writable] = reader.getKeyClass.asInstanceOf[Class[Writable]]
  private val valueClass: Class[Writable] = reader.getValueClass.asInstanceOf[Class[Writable]]

  private var currentBatchPosition: Int = 0
  private val batches: Seq[ColumnarBatch] = fillBatches()
  private val numBatch = batches.length

  override def next(): Boolean = currentBatchPosition < numBatch

  override def get(): ColumnarBatch = {
    val newBatch = batches(currentBatchPosition)
    currentBatchPosition += 1
    newBatch
  }

  override def close(): Unit = {
    reader.close()
    batches.foreach(x => x.close())
  }

  private def getReader: SequenceFile.Reader = {
    val bufferOption = SequenceFile.Reader.bufferSize(128*1024)
    val fileOption = SequenceFile.Reader.file(new Path(seqInputFileIO.getURI))
    new SequenceFile.Reader(conf, fileOption, bufferOption)

  }

  private def fillBatches(): Seq[ColumnarBatch] = {
    val batches: util.ArrayList[ColumnarBatch] = new util.ArrayList[ColumnarBatch]

    val kw: Writable = WritableHelper.newInstance(keyClass, conf)
    val vw: Writable = WritableHelper.newInstance(valueClass, conf)

    var keyVector = ArrowHelper.buildVectorFrom(keyClass, "key",
      new RootAllocator(Long.MaxValue))
    var valueVector = ArrowHelper.buildVectorFrom(valueClass, "value",
      new RootAllocator(Long.MaxValue))

    // Vector allocation
    keyVector.allocateNew(vectorizedReaderBatchSize)
    valueVector.allocateNew(vectorizedReaderBatchSize)

    var position: Int = 0
    while (reader.next(kw, vw)) {
      if (position < vectorizedReaderBatchSize) {
        val kValue = WritableHelper.extractValue(kw)
        val vValue = WritableHelper.extractValue(vw)

        ArrowHelper.fillVector(keyVector, position, kValue)
        ArrowHelper.fillVector(valueVector, position, vValue)

        position += 1
      } else {
        // Finish filling
        keyVector.setValueCount(position)
        valueVector.setValueCount(position)

        val arrowKVector = new ArrowColumnVector(keyVector)
        val arrowVVector = new ArrowColumnVector(valueVector)
        val batch = new ColumnarBatch(Array(arrowKVector, arrowVVector))
        batch.setNumRows(position)
        batches.add(batch)

        // Create new Vectors
        keyVector = ArrowHelper.buildVectorFrom(keyClass, "key",
          new RootAllocator(Long.MaxValue))
        valueVector = ArrowHelper.buildVectorFrom(valueClass, "value",
          new RootAllocator(Long.MaxValue))

        position = 0

        // Vector allocation
        keyVector.allocateNew(vectorizedReaderBatchSize)
        valueVector.allocateNew(vectorizedReaderBatchSize)

        val kValue = WritableHelper.extractValue(kw)
        val vValue = WritableHelper.extractValue(vw)

        ArrowHelper.fillVector(keyVector, position, kValue)
        ArrowHelper.fillVector(valueVector, position, vValue)

        position += 1
      }
    }
    // Finish filling
    keyVector.setValueCount(position)
    valueVector.setValueCount(position)

    val arrowKVector = new ArrowColumnVector(keyVector)
    val arrowVVector = new ArrowColumnVector(valueVector)
    val batch = new ColumnarBatch(Array(arrowKVector, arrowVVector))
    batch.setNumRows(position)
    batches.add(batch)

    batches.toSeq
  }


}



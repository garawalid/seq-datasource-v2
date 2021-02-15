package org.gwalid.seq.datasource.v2

import java.util

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BaseFixedWidthVector, BaseVariableWidthVector, BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, ValueVector, VarBinaryVector}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BooleanWritable, BytesWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, MapWritable, NullWritable, SequenceFile, Text, Writable}
import org.apache.spark.SerializableWritable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import scala.collection.JavaConversions._

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
    val fileOption = SequenceFile.Reader.file(new Path(seqInputFileIO.getURI))
    new SequenceFile.Reader(conf, fileOption)

  }

  private def createBatch(startPosition: Integer, endPosition: Integer): ColumnarBatch = {
    val kw: Writable = WritableHelper.newInstance(keyClass, conf)
    val vw: Writable = WritableHelper.newInstance(valueClass, conf)

    val keyAllocator = new RootAllocator(Long.MaxValue)
    val valueAllocator = new RootAllocator(Long.MaxValue)
    val keyVector = ArrowHelper.buildVectorFrom(keyClass, "key", keyAllocator)
    val valueVector = ArrowHelper.buildVectorFrom(valueClass, "value", valueAllocator)

    // Vector allocation
    keyVector.allocateNew(vectorizedReaderBatchSize)
    valueVector.allocateNew(vectorizedReaderBatchSize)

    val vectorSize = endPosition - startPosition
    // Fill Vector
    for (i <- 0 until vectorSize) {
      reader.next(kw, vw)

      val k = WritableHelper.extractValue(kw)
      val v = WritableHelper.extractValue(vw)

      ArrowHelper.fillVector(keyVector, i, k)
      ArrowHelper.fillVector(valueVector, i, v)


    }

    // Finish filling
    keyVector.setValueCount(vectorSize)
    valueVector.setValueCount(vectorSize)

    val arrowKVector = new ArrowColumnVector(keyVector)
    val arrowVVector = new ArrowColumnVector(valueVector)
    new ColumnarBatch(Array(arrowKVector, arrowVVector))

  }

  def fillBatches(): Seq[ColumnarBatch] = {
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



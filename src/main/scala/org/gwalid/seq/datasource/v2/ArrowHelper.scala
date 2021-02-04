package org.gwalid.seq.datasource.v2

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BaseFixedWidthVector, BaseVariableWidthVector, BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, ValueVector, VarBinaryVector}
import org.apache.hadoop.io.{BooleanWritable, BytesWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, Text, Writable}

object ArrowHelper {

  def buildVectorFrom(writable: Class[_ <: Writable], vectorName: String,
                      rootAllocator: RootAllocator): ValueVector = {
    writable match {
      case lw if lw == classOf[LongWritable] => new BigIntVector(vectorName, rootAllocator)
      case dw if dw == classOf[DoubleWritable] => new Float8Vector(vectorName, rootAllocator)
      case fw if fw == classOf[FloatWritable] => new Float4Vector(vectorName, rootAllocator)
      case lw if lw == classOf[IntWritable] => new IntVector(vectorName, rootAllocator)
      case bw if bw == classOf[BooleanWritable] => new BitVector(vectorName, rootAllocator)
      case bw if bw == classOf[BytesWritable] => new VarBinaryVector(vectorName, rootAllocator)
      case t if t == classOf[Text] => new VarBinaryVector(vectorName, rootAllocator)
      case dt => throw new UnsupportedOperationException(s"Unsupported data type : ${dt}")
    }

  }

  def fillVector(vector: ValueVector, position: Int, value: Any): Unit = {
    // Todo: Support others
    vector match {
      case kv: IntVector => kv.asInstanceOf[IntVector].set(position, value.asInstanceOf[Int])
      case kv: BigIntVector => kv.asInstanceOf[BigIntVector].set(position, value.asInstanceOf[Long])
    }
  }

  def allocateVector(vector: ValueVector, size: Int): Unit = {
    vector match {
      case kv: BaseFixedWidthVector =>
        kv.asInstanceOf[BaseFixedWidthVector].allocateNew(size)
      case kv: BaseVariableWidthVector => kv.allocateNew()
    }

  }

}

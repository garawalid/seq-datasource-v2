package org.gwalid.seq.datasource.v2

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BaseFixedWidthVector, BaseVariableWidthVector, BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, ValueVector, VarBinaryVector}
import org.apache.hadoop.io.{BooleanWritable, BytesWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, Text, Writable}


object ArrowHelper {

  def buildVectorFrom(writable: Class[_ <: Writable], vectorName: String,
                      rootAllocator: RootAllocator): BaseFixedWidthVector = {
    writable match {
      case lw if lw == classOf[IntWritable] => new IntVector(vectorName, rootAllocator)
      case lw if lw == classOf[LongWritable] => new BigIntVector(vectorName, rootAllocator)
      case fw if fw == classOf[FloatWritable] => new Float4Vector(vectorName, rootAllocator)
      case dw if dw == classOf[DoubleWritable] => new Float8Vector(vectorName, rootAllocator)
      case bw if bw == classOf[BooleanWritable] => new BitVector(vectorName, rootAllocator)
      case dt => throw new UnsupportedOperationException(s"Unsupported data type : ${dt}")
    }

  }

  def fillVector(vector: ValueVector, position: Int, value: Any): Unit = {
    vector match {
      case v: IntVector => v.asInstanceOf[IntVector].set(position, value.asInstanceOf[Int])
      case v: BigIntVector => v.asInstanceOf[BigIntVector].set(position, value.asInstanceOf[Long])
      case v: Float4Vector => v.asInstanceOf[Float4Vector].set(position, value.asInstanceOf[Float])
      case v: Float8Vector => v.asInstanceOf[Float8Vector].set(position, value.asInstanceOf[Double])
      case v: BitVector =>
        v.asInstanceOf[BitVector].set(position, convertBooleanToInt(value.asInstanceOf[Boolean]))
    }
  }

  private def convertBooleanToInt(value: Boolean): Int = {
    if (value) {
      1
    } else {
      0
    }
  }

}

package org.gwalid.seq.datasource.v2

import org.apache.hadoop.io._

import org.apache.spark.sql.types._

object TypeHelper {

  def convertHadoopToSpark(hadoopType: Class[_ <: Writable]): DataType = {
    hadoopType match {
      case lw if lw == classOf[LongWritable] => LongType
      case dw if dw == classOf[DoubleWritable] => DoubleType
      case fw if fw == classOf[FloatWritable] => FloatType
      case iw if iw == classOf[IntWritable] => IntegerType
      case bw if bw == classOf[BooleanWritable] => BooleanType
      case nw if nw == classOf[NullWritable] => NullType
      case bw if bw == classOf[BytesWritable] => StringType //  Fixme: Array[Byte]
      case t if t == classOf[Text] => StringType
      case mw if mw == classOf[MapWritable]
      => throw new NotImplementedError("Not implemented yet!")
      // Todo: ArrayType, MapType, ByteType
      case hadoopType =>
        throw new NotImplementedError(s"The ${hadoopType} type is not implemented yet!")
    }

  }

}

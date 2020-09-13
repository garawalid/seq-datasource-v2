package org.gwalid.seq.datasource.v2

import org.apache.hadoop.io._

import org.apache.spark.sql.types._

object TypeHelper {

  def convertHadoopToSpark(hadoopType: Class[_ <: Writable]): DataType = {
    hadoopType match {
      case hadoopType if hadoopType == classOf[LongWritable] => LongType
      case hadoopType if hadoopType == classOf[DoubleWritable] => DoubleType
      case hadoopType if hadoopType == classOf[FloatWritable] => FloatType
      case hadoopType if hadoopType == classOf[IntWritable] => IntegerType
      case hadoopType if hadoopType == classOf[BooleanWritable] => BooleanType
      case hadoopType if hadoopType == classOf[NullWritable] => NullType
      case hadoopType if hadoopType == classOf[BytesWritable] => StringType
      case hadoopType if hadoopType == classOf[Text] => StringType
      case hadoopType if hadoopType == classOf[ArrayWritable] | hadoopType == classOf[MapWritable] |
        hadoopType == classOf[ByteWritable] => throw new NotImplementedError("Not implemented yet!")
      // Todo: ArrayType, MapType, ByteType
      case hadoopType =>
        throw new NotImplementedError(s"The ${hadoopType} type is not implemented yet!")
    }

  }

}

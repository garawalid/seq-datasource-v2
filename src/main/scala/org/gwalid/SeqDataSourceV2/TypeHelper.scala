package org.gwalid.SeqDataSourceV2

import org.apache.hadoop.io.{BooleanWritable, BytesWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, NullWritable, Text, Writable}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, NullType, StringType}

object TypeHelper {

  def convertHadoopToSpark(hadoopType: Class[_ <: Writable]): DataType = {
    hadoopType match {
      case hadoopType if hadoopType == classOf[LongWritable] => LongType
      case hadoopType if hadoopType == classOf[DoubleWritable] => DoubleType
      case hadoopType if hadoopType == classOf[FloatWritable] => FloatType
      case hadoopType if hadoopType == classOf[IntWritable] => IntegerType
      case hadoopType if hadoopType == classOf[BooleanWritable] => BooleanType
      case hadoopType if hadoopType == classOf[NullWritable] => NullType
      case hadoopType if hadoopType == classOf[BytesWritable] => StringType // todo: check if there is no bytes in SparkTypes
      case hadoopType if hadoopType == classOf[Text] => StringType
      case hadoopType => throw new NotImplementedError(s"The ${hadoopType} type is not implemented yet!")
    }

  }

}

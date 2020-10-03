package org.gwalid.seq.datasource.v2

import scala.collection.mutable

import org.apache.hadoop.io.{ArrayWritable, BooleanWritable, BytesWritable, ByteWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, MapWritable, NullWritable, Text, Writable}
import org.scalatest.FunSuite

import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, LongType, NullType, StringType}


class TypeHelperTest extends FunSuite {

  test("convertHadoopToSpark") {

    val typeMapping = new mutable.HashMap[Class[_ <: Writable], DataType]
    typeMapping.put(classOf[LongWritable], LongType)
    typeMapping.put(classOf[DoubleWritable], DoubleType)
    typeMapping.put(classOf[FloatWritable], FloatType)
    typeMapping.put(classOf[IntWritable], IntegerType)
    typeMapping.put(classOf[BooleanWritable], BooleanType)
    typeMapping.put(classOf[NullWritable], NullType)
    typeMapping.put(classOf[BytesWritable], StringType)
    typeMapping.put(classOf[Text], StringType)

    typeMapping.foreach(tm => assert(TypeHelper.convertHadoopToSpark(tm._1) == tm._2))

    val notSupportedTypeMapping = Seq[Class[_ <: Writable]](classOf[ArrayWritable],
      classOf[MapWritable], classOf[ByteWritable])


    notSupportedTypeMapping.foreach(tm => {
      intercept[NotImplementedError] {
        TypeHelper.convertHadoopToSpark(tm)
      }
    })

  }
}

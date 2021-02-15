package org.gwalid.seq.datasource.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ArrayWritable, BooleanWritable, BytesWritable, DoubleWritable, FloatWritable, IntWritable, LongWritable, NullWritable, Text, Writable}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.unsafe.types.UTF8String


object WritableHelper {

  def newInstance(writableClass: Class[Writable], conf: Configuration): Writable = {
    // Create instance of Writable. If the input is ArrayWritable, we force it to TextArrayWritable.
    writableClass match {
      case writableClass if writableClass == classOf[ArrayWritable] =>
        val res = new ArrayWritable(classOf[Text])
        ReflectionUtils.setConf(res, conf)
        res
      case _ => ReflectionUtils.newInstance(writableClass, conf)
    }

  }

  def extractValue(writable: Writable): Any = {
    writable match {
      case x: LongWritable => x.get()
      case x: DoubleWritable => x.get()
      case x: FloatWritable => x.get()
      case x: IntWritable => x.get()
      case x: BooleanWritable => x.get()
      case _: NullWritable => null
      case x: BytesWritable => UTF8String.fromBytes(x.copyBytes())
      case x: Text => UTF8String.fromString(x.toString)
      case x => throw new RuntimeException(s"${x.getClass} is not implemented yet!")
    }

  }
}

package org.gwalid.seq.datasource.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{ArrayWritable, IntWritable, Text, Writable}
import org.apache.hadoop.util.ReflectionUtils


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
}

package org.gwalid.SeqDataSourceV2

import org.apache.hadoop.io.{BytesWritable, DoubleWritable, FloatWritable, IntWritable}
import org.apache.spark.sql.types.BooleanType
import org.scalatest.FunSuite

class TypeHelperTest extends FunSuite {

  test("convertHadoopToSpark"){
    // Todo: Finish this test
    Seq(classOf[IntWritable],classOf[FloatWritable], classOf[DoubleWritable], classOf[BytesWritable],classOf[BooleanType])


  }
}

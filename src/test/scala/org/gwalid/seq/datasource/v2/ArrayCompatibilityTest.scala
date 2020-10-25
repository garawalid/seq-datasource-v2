package org.gwalid.seq.datasource.v2

import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StructType}
import org.scalatest.FunSuite

class ArrayCompatibilityTest extends FunSuite {

  test("write Array of Int") {
    val ilKV = Seq((1, Array(1, 2, 3)), (2, Array(2, 3, 4)), (4, Array(5, 6, 7)))
    val tempDirFile = Files.createTempDirectory(this.getClass.getName).toFile
    val tempDir: String = tempDirFile.toString
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("WARN")


    val ilExpSchema = new StructType()
      .add("key", IntegerType, nullable = true)
      .add("value", ArrayType(IntegerType, true), nullable = true)
    val ilPath = new Path(tempDir, "data").suffix("/sample-int-long").toString

    sc.parallelize(ilKV).saveAsSequenceFile(ilPath)
    sc.parallelize(ilKV).foreach(println(_))
  }
}

package org.gwalid.seq.datasource.v2

import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, ByteType, DoubleType, FloatType, IntegerType, LongType, NullType, StringType, StructType}


class CompatibilityTest extends FunSuite {

  val tempDirFile = Files.createTempDirectory(this.getClass.getName).toFile
  val tempDir: String = tempDirFile.toString
  val spark = SparkSession.builder().master("local[1]").getOrCreate()
  val sc = spark.sparkContext
  spark.sparkContext.setLogLevel("WARN")

  test("Test SeqDataSourceV2 and Spark RDD write path") {
    // Test SeqDataSourceV2 read path compatibility with Spark RDD write path
    val ilKV = Seq((1, 5L), (2, 6L), (2, 7L), (3, 8L), (2, 9L), (1, 10L))
    val ilExpSchema = new StructType()
      .add("key", IntegerType, nullable = true)
      .add("value", LongType, nullable = true)
    val ilPath = new Path(tempDir, "data").suffix("/sample-int-long").toString
    sc.parallelize(ilKV).saveAsSequenceFile(ilPath)
    assertDSReadPath(ilKV, ilExpSchema, ilPath)

    val fbKV = Seq((1.0, true), (2.0, false), (2.0, true), (3.0, true), (2.0, true), (1.0, false))
    val fbExpSchema = new StructType()
      .add("key", FloatType, nullable = true)
      .add("value", BooleanType, nullable = true)
    val fbPath = new Path(tempDir, "data").suffix("/sample-float-bool").toString
    sc.parallelize(fbKV).saveAsSequenceFile(fbPath)
    assertDSReadPath(fbKV, fbExpSchema, fbPath)

    val idKV = Seq((1, 5.0), (2, 6.0), (3, 7.0), (4, 8.0), (5, 9.0))
    val idExpSchema = new StructType()
      .add("key", IntegerType, nullable = true)
      .add("value", DoubleType, nullable = true)
    val idPath = new Path(tempDir, "data").suffix("/sample-int-double").toString
    sc.parallelize(idKV).saveAsSequenceFile(idPath)
    assertDSReadPath(idKV, idExpSchema, idPath)

    val nbKV = Seq((null, 5.0), (null, 6.0), (null, 7.0), (null, 8.0), (null, 9.0))
    val nbExpSchema = new StructType()
      .add("key", NullType, nullable = true)
      .add("value", DoubleType, nullable = true)
    val nbPath = new Path(tempDir, "data").suffix("/sample-null-double").toString
     sc.parallelize(nbKV).saveAsSequenceFile(nbPath)
     assertDSReadPath(nbKV, nbExpSchema, nbPath)

    val tiKV = Seq(("A", 1), ("B", 2), ("C", 3), ("D", 4), ("E", 5))
    val tiExpSchema = new StructType()
      .add("key", StringType, nullable = true)
      .add("value", IntegerType, nullable = true)
    val tiPath = new Path(tempDir, "data").suffix("/sample-text-int").toString
    sc.parallelize(tiKV).saveAsSequenceFile(tiPath)
    assertDSReadPath(tiKV, tiExpSchema, tiPath)
  }

  private def assertDSReadPath(kvData: Seq[(Any, Any)], expectedSchema: StructType,
                               path: String) = {

    val df = spark.read.format("seq").load(path)
    val expectedKV = df.collect().map(x => (x(0), x(1))).toSeq

    assert(kvData == expectedKV)
    assert(df.schema == expectedSchema)

  }

}

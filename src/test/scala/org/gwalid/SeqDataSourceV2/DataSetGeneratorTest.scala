package org.gwalid.SeqDataSourceV2

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DataSetGeneratorTest extends FunSuite {
  test("") {
    // download and convert https://grouplens.org/datasets/movielens/ to SeqFiles with Spark
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    val raw = spark
      .sparkContext
      .textFile("/home/nops/Projects/data/ml-10M/ratings.dat")
      .repartition(300)
      .map(x => x.split("::"))
      .map(x => (x(1), x(3)))


    println(raw.getNumPartitions)


    raw.saveAsSequenceFile("/home/nops/Projects/data/ml-10M-seq")
  }

}

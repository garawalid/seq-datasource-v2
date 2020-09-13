package org.gwalid.seq.datasource.v2

import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession


class DataSetGeneratorTest extends FunSuite {
  // Todo: Move this class to benchmark jmh
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

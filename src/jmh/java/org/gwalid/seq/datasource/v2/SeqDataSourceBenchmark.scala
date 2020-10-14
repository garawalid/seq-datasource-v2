package org.gwalid.seq.datasource.v2

import java.io.File
import java.net.URL

import net.lingala.zip4j.ZipFile
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.Text

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, countDistinct}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


class SeqDataSourceBenchmark(var spark: SparkSession) {

  private val datasetName = "ml-25m" // Todo: Replace it with bigger Dataset ml-25m
  private val datasetSeqPath = "/tmp/data/" + datasetName + "/ratings.seq"


  def countWithRDD(): Unit = {
    val df = readPathRDD()
    df.count
  }


  def countWithDS(): Unit = {
    val df = spark.read.format("seq").load(datasetSeqPath)
    df.count
  }

  def aggQueryWithRDD(): Unit = {
    val df = readPathRDD()
    df.groupBy("key")
      .agg(countDistinct("value"))
      .count()

  }

  def aggQueryWithDS(): Unit = {
    val df = spark.read.format("seq").load(datasetSeqPath)
    df.groupBy("key")
      .agg(countDistinct("value"))
      .count()

  }

  def filterQueryWithRDD(): Unit = {
    val df = readPathRDD()
    df.filter(col("value") === "4.0")
      .count()
  }

  def filterQueryWithDS(): Unit = {
    val df = spark.read.format("seq").load(datasetSeqPath)
    df.filter(col("value") === "4.0")
      .count()
  }


  private def readPathRDD(): DataFrame = {
    val rdd: RDD[Row] = spark.sparkContext
      .sequenceFile[Text, Text](datasetSeqPath)
      .map(x => Row(new String(x._1.copyBytes()), new String(x._2.copyBytes())))

    val schema = new StructType()
      .add(StructField("key", StringType, true))
      .add(StructField("value", StringType, true))

    spark.createDataFrame(rdd, schema)

  }

  private def convertCSVtoSeq(datasetName: String): Unit = {
    System.out.println("Converting rating.csv to rating.seq")
    val datasetPath = "/tmp/data/" + datasetName + "/ratings.csv"
    val dfMl = spark.read.format("csv").option("header", true).load(datasetPath)

    dfMl
      .select("movieId", "rating").rdd
      .map(x => (x.getAs[String]("movieId"), x.getAs[String]("rating")))
      .saveAsSequenceFile(datasetSeqPath);
    System.out.println("The path of rating.seq is " + datasetSeqPath)
  }


  def prepareDataset(): Unit = {
    // Downland movielens dataset
    try {
      val datasetPath = new File("/tmp/data/" + datasetName)
      if (!datasetPath.isDirectory) {
        val zipPath = new File("/tmp/ml.zip")
        System.out.println("Download the dataset in" + zipPath.toString)
        val url = new URL("http://files.grouplens.org/datasets/movielens/" + datasetName + ".zip")
        FileUtils.copyURLToFile(url, zipPath)
        print("Extracting the zip in /tmp/data")
        val mlDataSet = new ZipFile(zipPath.toString)
        mlDataSet.extractAll("/tmp/data")
        convertCSVtoSeq(datasetName)
      } else System.out.println("Skip downloading the dataset.")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }


}

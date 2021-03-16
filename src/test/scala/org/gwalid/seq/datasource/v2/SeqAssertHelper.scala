package org.gwalid.seq.datasource.v2

import org.apache.spark.sql.DataFrame

object SeqAssertHelper {
  def getKeyDataAs[T: Ordering](df: DataFrame): Seq[T] = {
    df.select("key").collect().map(_ (0).asInstanceOf[T]).toSeq.sorted
  }

  def getValueDataAs[T: Ordering](df: DataFrame): Seq[T] = {
    df.select("value").collect().map(_ (0).asInstanceOf[T]).toSeq.sorted
  }

  def getKeyData(df: DataFrame): Seq[Any] = {
    df.select("key").collect().map(_ (0)).toSeq
  }

  def getValueData(df: DataFrame): Seq[Any] = {
    df.select("value").collect().map(_ (0)).toSeq
  }

}

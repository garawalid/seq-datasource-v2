package org.gwalid.seq.datasource.v2

import java.util.OptionalLong

import org.apache.spark.sql.sources.v2.reader.Statistics

class SeqDataSourceStatistics(size: Long, numRows: Long) extends Statistics {

  override def sizeInBytes(): OptionalLong = OptionalLong.of(size)

  override def numRows(): OptionalLong = OptionalLong.of(numRows)


}

package org.gwalid.SeqDataSourceV2

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}


class SeqDataSourceV2 extends DataSourceV2 with ReadSupport with DataSourceRegister{
  override def createReader(options: DataSourceOptions): DataSourceReader = {

  new SeqDataSourceReader(options)
  }

  override def shortName(): String = "seq"
}

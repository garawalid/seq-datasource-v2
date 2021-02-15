package org.gwalid.seq.datasource.v2

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.StructType


class SeqDataSourceV2 extends DataSourceV2 with ReadSupport with DataSourceRegister {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    createReader(null, options)
  }

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    new SeqDataSourceReader(options, Option(schema))
  }

  override def shortName(): String = "seq"
}

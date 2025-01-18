package io.kination.vine

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class VineTable(schema: StructType) extends Table with SupportsRead with SupportsWrite {

  override def name(): String = "VineTable"

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = {
    java.util.EnumSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.STREAMING_WRITE)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    // TODO: "schema" and schema inside "LogicalWriteInfo" can be different
    // - schema: schema defined by metadata.json
    // - info.schema: schema defined by user, when writing data from spark
    var schema = this.schema()
    
    if (schema == null || schema.isEmpty) {
      schema = info.schema()
    }
    new VineDataSourceWriteBuilder(schema, info)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    // println("call VineDataSourceReader from VineTable")
    new VineDataSourceReader(options, schema)
  }
}

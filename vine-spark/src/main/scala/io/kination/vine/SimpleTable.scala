package io.kination.vine

import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType

import java.util

class SimpleTable(schema: StructType) extends Table with SupportsWrite {

  override def name(): String = "SimpleTable"

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = {
    java.util.EnumSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE)
  }


  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new MyDataSourceWriteBuilder(info)
  }
}

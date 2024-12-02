package io.kination.vine

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class SimpleTable(schema: StructType) extends Table with SupportsRead with SupportsWrite {

  override def name(): String = "SimpleTable"

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = {
    java.util.EnumSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE)
  }


  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
//    new MyDataSourceWriteBuilder(info)
    println("call VineDataSourceWriter from simpletable")
    new VineDataSourceWriteBuilder(info)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    println("call VineDataSourceReader from simpletable")
    new VineDataSourceReader(options)
  }
}

package io.kination.vine;

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class VineDataSource extends TableProvider with SupportsRead with SupportsWrite {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    StructType(Seq(
      StructField("id", StringType),
      StructField("name", StringType)
    ))
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    new SimpleTable(schema)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new VineDataSourceReader(options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new VineDataSourceWriteBuilder(info)
  }

  override def name(): String = ???

  override def schema(): StructType = ???

  override def capabilities(): util.Set[TableCapability] = ???
}

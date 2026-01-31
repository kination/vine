package io.kination.vine

import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class VineDataSourceReader(options: CaseInsensitiveStringMap, schema: StructType) extends ScanBuilder {
  override def build(): Scan = {
    val rootPath = options.get("path")
    val arrowData = VineModule.readDataArrow(f"$rootPath/result")
    new VineDataSourceScan(arrowData, schema)
  }
}

class VineDataSourceScan(arrowData: Array[Byte], schema: StructType) extends Scan {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = {
    new VineBatchReader(arrowData, schema)
  }
}

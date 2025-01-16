package io.kination.vine

import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class VineDataSourceReader(options: CaseInsensitiveStringMap, schema: StructType) extends ScanBuilder {
  // TODO: parse dynamically based on schema inside metadata
  override def build(): Scan = {
    val rootPath = options.get("path")
    val rawData = VineModule.readData(f"$rootPath/result")
    new VineDataSourceScan(rawData, schema)
  }
}

class VineDataSourceScan(rawData: String, schema: StructType) extends Scan {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = {
    new VineBatchReader(rawData)
  }
}

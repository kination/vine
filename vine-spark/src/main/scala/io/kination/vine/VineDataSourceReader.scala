package io.kination.vine

import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class VineDataSourceReader(options: CaseInsensitiveStringMap) extends ScanBuilder {
  // TODO: parse dynamically based on schema inside metadata
  private val schema = StructType(Seq(
    StructField("id", StringType),
    StructField("name", StringType)
  ))

  override def build(): Scan = {
    val rawData = VineModule.readData("vine-test/result")
    new VineDataSourceScan(rawData, schema)
  }
}

class VineDataSourceScan(rawData: String, schema: StructType) extends Scan {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = {
    new VineBatchReader(rawData)
  }
}

package io.kination.vine

import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class VineDataSourceReader(options: CaseInsensitiveStringMap) extends ScanBuilder {
  private val schema = StructType(Seq(
    StructField("id", StringType),
    StructField("name", StringType)
  ))

  override def build(): Scan = new VineDataSourceScan(schema, options)
}

class VineDataSourceScan(schema: StructType, options: CaseInsensitiveStringMap) extends Scan with Batch {

  override def planInputPartitions(): Array[InputPartition] = {
//    Array(new SimpleInputPartition)
    null
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new SimplePartitionReaderFactory
  }

  override def readSchema(): StructType = schema
}

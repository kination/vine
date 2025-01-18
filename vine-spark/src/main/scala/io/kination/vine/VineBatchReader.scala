package io.kination.vine

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType


class VineBatchReader(rawData: String, schema: StructType) extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new VineInputPartition(rawData))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new VinePartitionReaderFactory(schema)
  }
}

package io.kination.vine

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType


class VineBatchReader(arrowData: Array[Byte], schema: StructType) extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new VineInputPartition(arrowData))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new VinePartitionReaderFactory(schema)
  }
}

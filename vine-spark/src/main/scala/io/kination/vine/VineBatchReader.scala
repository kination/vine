package io.kination.vine

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}

class VineBatchReader(rawData: String) extends Batch {

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new VineInputPartition(rawData))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new VinePartitionReaderFactory
  }
}

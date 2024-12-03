package io.kination.vine

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.unsafe.types.UTF8String

class VinePartitionReaderFactory extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new VinePartitionReader(partition.asInstanceOf[VineInputPartition].rawData)
  }
}

class VinePartitionReader(rawData: String) extends PartitionReader[InternalRow] {
  // TODO: think of good rawData format
  private val rows = rawData.split("\n").filter(_.nonEmpty).toList.map { line =>
    line.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\""))
  }

  private val iterator = rows.iterator

  override def next(): Boolean = iterator.hasNext

  override def get(): InternalRow = {
    // TODO: parse dynamically based on schema inside metadata
    val fields = iterator.next()
    val id = fields(0)
    val name = fields(1)
    new GenericInternalRow(
      Array[Any](
        UTF8String.fromString(id),
        UTF8String.fromString(name)
      )
    )
  }

  override def close(): Unit = {}
}

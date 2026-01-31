package io.kination.vine

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

/**
 * Create Vine partition readers.
 */
class VinePartitionReaderFactory(schema: StructType) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new VinePartitionReader(partition.asInstanceOf[VineInputPartition].arrowData, schema)
  }
}

/**
 * Converts Arrow IPC data (from JNI) to InternalRows.
 * Supports all Vine/Vortex types.
 */
class VinePartitionReader(arrowData: Array[Byte], schema: StructType) extends PartitionReader[InternalRow] {

  private val encoder = RowEncoder(schema).resolveAndBind()
  private val internalRows = if (arrowData != null && arrowData.nonEmpty) {
    val rows = VineArrowBridge.arrowIpcToRows(arrowData, schema)
    rows.map(row => encoder.createSerializer().apply(row))
  } else {
    Seq.empty[InternalRow]
  }

  private val iterator = internalRows.iterator

  override def next(): Boolean = iterator.hasNext

  override def get(): InternalRow = iterator.next()

  override def close(): Unit = {}
}

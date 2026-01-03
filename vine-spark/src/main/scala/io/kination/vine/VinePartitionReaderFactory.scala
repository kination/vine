package io.kination.vine

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Create Vine partition readers.
 */
class VinePartitionReaderFactory(schema: StructType) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new VinePartitionReader(partition.asInstanceOf[VineInputPartition].rawData, schema)
  }
}

/**
 * Converts CSV data (from JNI) to InternalRows.
 * Supports all Vine/Vortex types.
 */
class VinePartitionReader(rawData: String, schema: StructType) extends PartitionReader[InternalRow] {

  private val rows = rawData.split("\n").filter(_.nonEmpty).toList.map { line =>
    line.split(",", -1).map(_.trim.stripPrefix("\"").stripSuffix("\""))
  }

  private val iterator = rows.iterator

  override def next(): Boolean = iterator.hasNext

  override def get(): InternalRow = {
    val fields = iterator.next()
    val values = schema.fields.zipWithIndex.map { case (field, idx) =>
      val value = if (idx < fields.length) fields(idx) else ""

      if (value.isEmpty) {
        null  // Handle nulls
      } else {
        parseValue(value, field.dataType)
      }
    }
    new GenericInternalRow(values.toArray)
  }

  override def close(): Unit = {}

  /**
   * Parse string value to appropriate Spark internal type.
   * Supports all Vine/Vortex types.
   */
  private def parseValue(value: String, dataType: DataType): Any = {
    VineTypeUtils.parseValue(value, dataType)
  }
}

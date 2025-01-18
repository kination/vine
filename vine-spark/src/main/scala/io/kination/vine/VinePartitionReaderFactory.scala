package io.kination.vine

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types.StructType


class VinePartitionReaderFactory(schema: StructType) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new VinePartitionReader(partition.asInstanceOf[VineInputPartition].rawData, schema)
  }
}

class VinePartitionReader(rawData: String, schema: StructType) extends PartitionReader[InternalRow] {
  // TODO: think of good rawData format
  private val rows = rawData.split("\n").filter(_.nonEmpty).toList.map { line =>
    line.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\""))
  }

  private val iterator = rows.iterator

  override def next(): Boolean = iterator.hasNext

  override def get(): InternalRow = {
    val fields = iterator.next()
    val values = schema.fields.zipWithIndex.map { case (field, idx) =>
      field.dataType match {
        case StringType => UTF8String.fromString(fields(idx))
        case IntegerType => fields(idx).toInt
        case _ => UTF8String.fromString(fields(idx)) // fallback
      }
    }
    new GenericInternalRow(values.toArray)
  }

  override def close(): Unit = {}
}

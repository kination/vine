package io.kination.vine

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.unsafe.types.UTF8String

class SimplePartitionReaderFactory extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new SimplePartitionReader
  }
}

class SimplePartitionReader extends PartitionReader[InternalRow] {
  // TODO: put actual path from configs
//  private val data = VineJNI.readDataFromVine("vine-test/result").split("\n").iterator
  private val data = VineModule.readDataFromVine("vine-test/result").split("\n").iterator

  override def next(): Boolean = data.hasNext

  override def get(): InternalRow = {
    val row = data.next().split(",")
    InternalRow(UTF8String.fromString(row(0)), UTF8String.fromString(row(1)))
  }

  override def close(): Unit = {}
}

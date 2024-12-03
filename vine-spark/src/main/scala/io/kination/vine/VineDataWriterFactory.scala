package io.kination.vine

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}

import scala.collection.mutable.ListBuffer

class VineDataWriterFactory extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new VineDataWriter
  }
}

class VineDataWriter extends DataWriter[InternalRow] {
  // TODO:
  //  - This is only supporting for 2 col only. Make data based on schema
  //  - Put actual path from configs
  //  - Think of setting up buffer
  //  - Think of how-to make commit based on multiple partition
  private val buffer = ListBuffer[String]()
  private val bufferSize = 10

  override def write(record: InternalRow): Unit = {
    val data = record.getString(0) + "," + record.getString(1)
    buffer += data
    if (buffer.size >= bufferSize) {
      flushBuffer()
    }
  }

  override def commit(): WriterCommitMessage = {
    if (buffer.nonEmpty) {
      flushBuffer()
    }
    null
  }

  override def abort(): Unit = {}

  override def close(): Unit = {}

  private def flushBuffer(): Unit = {
    val mergeBuffer = buffer.mkString("\n")
    VineModule.writeData("vine-test/result", mergeBuffer)
    buffer.clear()
  }
}

package io.kination.vine

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}

import scala.collection.mutable.ListBuffer

class SimpleDataWriterFactory extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new SimpleDataWriter
  }
}

class SimpleDataWriter extends DataWriter[InternalRow] {
  private val buffer = ListBuffer[String]()
  private val bufferSize = 10

  override def write(record: InternalRow): Unit = {
    // TODO: put actual path from configs
    val data = record.getString(0) + "," + record.getString(1)
    buffer += data
    if (buffer.size >= bufferSize) {
      flushBuffer()
    }
//    VineJNI.writeDataToVine("vine-test/result", data)
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

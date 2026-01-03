package io.kination.vine

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer

/**
 * Factory to create data writers for Spark DataSource V2.
 */
class VineDataWriterFactory(
    schema: StructType,
    info: PhysicalWriteInfo,
    path: String
) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new VineDataWriter(schema, info, path)
  }
}

/**
 * Spark DataSource V2 writer that writes InternalRows to Vine tables.
 */
class VineDataWriter(
    schema: StructType,
    info: PhysicalWriteInfo,
    path: String
) extends DataWriter[InternalRow] {

  private val buffer = ListBuffer[String]()
  private val bufferSize = 1000  // TODO: Optimize buffer for better performance

  override def write(record: InternalRow): Unit = {
    val data = formatRecord(record)
    buffer += data

    if (buffer.size >= bufferSize) {
      flushBuffer()
    }
  }

  override def commit(): WriterCommitMessage = {
    if (buffer.nonEmpty) {
      flushBuffer()
    }
    VineWriterCommitMessage(path, buffer.size)
  }

  override def abort(): Unit = {
    buffer.clear()
  }

  override def close(): Unit = {
    // Nothing to do - buffer is flushed on commit
  }

  /**
   * Format InternalRow to CSV string for JNI.
   * Supports all Vine/Vortex types.
   */
  private def formatRecord(record: InternalRow): String = {
    VineTypeUtils.formatInternalRow(record, schema)
  }

  private def flushBuffer(): Unit = {
    if (buffer.nonEmpty) {
      val mergeBuffer = buffer.mkString("\n")
      VineModule.batchWrite(path, mergeBuffer)
      buffer.clear()
    }
  }
}

/**
 * Commit message containing write statistics.
 */
case class VineWriterCommitMessage(path: String, rowsWritten: Int) extends WriterCommitMessage

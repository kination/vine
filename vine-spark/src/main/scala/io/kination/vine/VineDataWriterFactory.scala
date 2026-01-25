package io.kination.vine

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer

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

  // Buffer for Arrow-based transfer (stores InternalRows)
  private val rowBuffer = ArrayBuffer[InternalRow]()
  private val batchSize = VineArrowConfig.DEFAULT_BATCH_SIZE
  private var totalRowsWritten = 0

  override def write(record: InternalRow): Unit = {
    // Copy the record since InternalRow may be reused
    rowBuffer += record.copy()

    if (rowBuffer.size >= batchSize) {
      flushBuffer()
    }
  }

  override def commit(): WriterCommitMessage = {
    if (rowBuffer.nonEmpty) {
      flushBuffer()
    }
    VineWriterCommitMessage(path, totalRowsWritten)
  }

  override def abort(): Unit = {
    rowBuffer.clear()
  }

  override def close(): Unit = {
    // TODO: Buffer is flushed on commit
  }

  private def flushBuffer(): Unit = {
    if (rowBuffer.nonEmpty) {
      val arrowBytes = VineArrowBridge.internalRowsToArrowIpc(rowBuffer.toSeq, schema)
      VineModule.batchWriteArrow(path, arrowBytes)

      totalRowsWritten += rowBuffer.size
      rowBuffer.clear()
    }
  }
}

/**
 * Commit message containing write statistics.
 */
case class VineWriterCommitMessage(path: String, rowsWritten: Int) extends WriterCommitMessage

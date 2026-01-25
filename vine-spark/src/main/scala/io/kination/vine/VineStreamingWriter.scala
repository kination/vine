package io.kination.vine

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType


/**
 * Streaming writer for incremental data ingestion to Vine tables.
 *
 * Optimized for 'continuous data streams' where batches arrive over time.
 * Supports explicit control over flushing and file rotation.
 */
class VineStreamingWriter(path: String) extends AutoCloseable {

  private val writerId: Long = VineModule.createStreamingWriter(path)
  private var closed = false

  /**
   * Append DataFrame batch to stream using Arrow IPC format.
   *
   * @param df DataFrame to append
   */
  def appendBatch(df: DataFrame): Unit = {
    ensureOpen()
    val rows = df.collect().toSeq
    if (rows.nonEmpty) {
      appendRows(rows, df.schema)
    }
  }

  /**
   * Append rows batch to stream using Arrow IPC format.
   *
   * @param rows Rows to append
   * @param schema Schema of the rows
   */
  def appendRows(rows: Seq[Row], schema: StructType): Unit = {
    ensureOpen()
    if (rows.isEmpty) return

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)
    VineModule.streamingAppendBatchArrow(writerId, arrowBytes)
  }

  /**
   * Flush pending writes.
   * Closes current file and opens new file on next write.
   *
   * Call this periodically to:
   * - Control file size
   * - Make data visible to readers
   * - Checkpoint progress
   */
  def flush(): Unit = {
    ensureOpen()
    VineModule.streamingFlush(writerId)
  }

  /**
   * Close the writer and finalize all pending writes.
   * This must be called after 'writing'.
   *
   * After closing, the writer cannot be used anymore.
   */
  override def close(): Unit = {
    if (!closed) {
      VineModule.streamingClose(writerId)
      closed = true
    }
  }

  /**
   * Check if writer is still open.
   */
  private def ensureOpen(): Unit = {
    if (closed) {
      throw new IllegalStateException(
        s"VineStreamingWriter for path '$path' is already closed"
      )
    }
  }
}

object VineStreamingWriter {
  /**
   * Create streaming writer for the given path.
   */
  def apply(path: String): VineStreamingWriter = {
    new VineStreamingWriter(path)
  }
}

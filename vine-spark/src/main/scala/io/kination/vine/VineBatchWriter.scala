package io.kination.vine

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType

/**
 * Batch writer for bulk data ingestion.
 */
object VineBatchWriter {

  /**
   * Write DataFrame using Arrow IPC format.
   *
   * @param path Directory path to Vine table (must contain vine_meta.json)
   * @param df DataFrame to write
   */
  def write(path: String, df: DataFrame): Unit = {
    val rows = df.collect().toSeq
    if (rows.nonEmpty) {
      writeRows(path, rows, df.schema)
    }
  }

  /**
   * Write collection of rows using Arrow IPC format.
   *
   * @param path Directory path to write Vine table
   * @param rows Collection of rows
   * @param schema Schema of the rows
   */
  def writeRows(path: String, rows: Seq[Row], schema: StructType): Unit = {
    if (rows.isEmpty) return

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)
    VineModule.batchWriteArrow(path, arrowBytes)
  }
}

package io.kination.vine

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType

/**
 * Batch writer for bulk data ingestion
 */
object VineBatchWriter {

  /**
   * Write DataFrame
   *
   * @param path Directory path to Vine table (must contain vine_meta.json)
   * @param df DataFrame to write
   */
  def write(path: String, df: DataFrame): Unit = {
    val data = formatDataFrame(df)
    VineModule.batchWrite(path, data)
  }

  /**
   * Write collection of rows
   *
   * @param path Directory path to write Vine table
   * @param rows Collection of rows
   * @param schema Schema of the rows
   */
  def writeRows(path: String, rows: Seq[Row], schema: StructType): Unit = {
    val data = formatRows(rows, schema)
    VineModule.batchWrite(path, data)
  }

  // TODO: Replace with binary format (Arrow) for better performance.
  private def formatDataFrame(df: DataFrame): String = {
    df.collect().map(row => formatRow(row, df.schema)).mkString("\n")
  }

  private def formatRows(rows: Seq[Row], schema: StructType): String = {
    rows.map(row => formatRow(row, schema)).mkString("\n")
  }

  /**
   * Format a single row to CSV.
   * Supports all Vine/Vortex types.
   */
  private def formatRow(row: Row, schema: StructType): String = {
    VineTypeUtils.formatRow(row, schema)
  }
}

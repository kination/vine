package io.kination.vine

import io.kination.vine.catalog.{CatalogConfig, HiveMetastoreClient}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType

import java.nio.file.{Files, Paths}
import java.time.LocalDate
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Batch writer for bulk data ingestion.
 */
object VineBatchWriter {

  /**
   * Write DataFrame
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
   * Write DataFrame with optional Hive Metastore catalog integration.
   *
   * @param path Directory path to output(vine table)
   * @param df DataFrame to write
   * @param catalogConfig Optional catalog configuration for HMS integration
   * @param tableName Optional table name for HMS registration
   * 
   */
  def writeWithCatalog(
    path: String,
    df: DataFrame,
    catalogConfig: Option[CatalogConfig],
    tableName: Option[String] = None
  ): Unit = {
    val rows = df.collect().toSeq
    if (rows.isEmpty) return

    // Write data
    writeRows(path, rows, df.schema)

    // Register partition in HMS if catalog is configured
    catalogConfig.foreach { config =>
      tableName.foreach { tbl =>
        registerPartitionInCatalog(path, tbl, df.schema, config)
      }
    }
  }

  /**
   * Write collection of rows
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

  /**
   * Write collection of rows with catalog integration.
   *
   * @param path Directory path to write Vine table
   * @param rows Collection of rows
   * @param schema Schema of the rows
   * @param catalogConfig Optional catalog configuration
   * @param tableName Optional table name for HMS
   * 
   */
  def writeRowsWithCatalog(
    path: String,
    rows: Seq[Row],
    schema: StructType,
    catalogConfig: Option[CatalogConfig],
    tableName: Option[String] = None
  ): Unit = {
    if (rows.isEmpty) return

    // Write data
    writeRows(path, rows, schema)

    // Register partition in HMS if catalog is configured
    catalogConfig.foreach { config =>
      tableName.foreach { tbl =>
        registerPartitionInCatalog(path, tbl, schema, config)
      }
    }
  }

  /**
   * Register newly written partition in Hive Metastore.
   * Uses the current date to identify the partition.
   */
  private def registerPartitionInCatalog(
    basePath: String,
    tableName: String,
    schema: StructType,
    config: CatalogConfig
  ): Unit = {
    val currentDate = LocalDate.now().toString // YYYY-MM-DD
    val partitionPath = Paths.get(basePath, currentDate).toString

    // Check if partition directory exists
    if (!Files.exists(Paths.get(partitionPath))) {
      println(s"Partition directory does not exist: $partitionPath, skipping HMS registration")
      return
    }

    val registerPartition = () => {
      val client = HiveMetastoreClient(config)
      try {
        // Ensure table is registered
        if (!client.tableExists(extractDbName(tableName, config), extractTableName(tableName))) {
          client.registerTable(tableName, schema, basePath) match {
            case Success(_) => println(s"Registered table $tableName in HMS")
            case Failure(e) => println(s"Failed to register table $tableName: ${e.getMessage}")
          }
        }

        // Register partition
        client.registerPartition(tableName, currentDate, partitionPath) match {
          case Success(_) => println(s"Registered partition $currentDate for table $tableName in HMS")
          case Failure(e) => println(s"Failed to register partition: ${e.getMessage}")
        }
      } finally {
        client.close()
      }
    }

    // Register asynchronously if enabled
    if (config.enableAsync) {
      implicit val ec: ExecutionContext = ExecutionContext.global
      Future {
        registerPartition()
      }
    } else {
      registerPartition()
    }
  }

  private def extractDbName(fullTableName: String, config: CatalogConfig): String = {
    val parts = fullTableName.split("\\.")
    if (parts.length == 2) parts(0) else config.database
  }

  private def extractTableName(fullTableName: String): String = {
    val parts = fullTableName.split("\\.")
    if (parts.length == 2) parts(1) else fullTableName
  }
}

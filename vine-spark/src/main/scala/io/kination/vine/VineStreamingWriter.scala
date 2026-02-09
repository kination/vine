package io.kination.vine

import io.kination.vine.catalog.{CatalogConfig, HiveMetastoreClient}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType

import java.nio.file.{Files, Paths}
import java.time.LocalDate
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


/**
 * Streaming writer for incremental data ingestion
 *
 * Optimized on 'continuous data streams' where batches arrive over time.
 * Supports explicit control over flushing and file rotation.
 *
 * @param path Base path to Vine table
 * @param catalogConfig Optional Hive Metastore catalog configuration
 * @param tableName Optional table name for HMS registration
 * 
 */
class VineStreamingWriter(
  path: String,
  catalogConfig: Option[CatalogConfig] = None,
  tableName: Option[String] = None
) extends AutoCloseable {

  private val writerId: Long = VineModule.createStreamingWriter(path)
  private var closed = false
  private var lastSchema: Option[StructType] = None
  private val writtenPartitions = mutable.Set[String]()

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

    // Track schema for catalog registration
    if (lastSchema.isEmpty) {
      lastSchema = Some(schema)
    }

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)
    VineModule.streamingAppendBatchArrow(writerId, arrowBytes)
  }

  /**
   * Flush pending writes.
   * Close current file, and open new file on next write.
   *
   * Call this periodically to:
   * - Control file size
   * - Make data visible to readers
   * - Checkpoint progress
   *
   * If catalog is configured, this will also register partitions in HMS.
   * 
   */
  def flush(): Unit = {
    ensureOpen()
    VineModule.streamingFlush(writerId)

    // Register partitions in catalog after flush
    registerPendingPartitions()
  }

  /**
   * Close the writer and finalize all pending writes.
   * This must be called after 'writing'.
   *
   * After closing, the writer cannot be used anymore.
   * If catalog is configured, this will register any pending partitions in HMS.
   * 
   */
  override def close(): Unit = {
    if (!closed) {
      // Ensure final flush
      VineModule.streamingFlush(writerId)  
      
      VineModule.streamingClose(writerId)
      closed = true

      // Register any remaining partitions
      registerPendingPartitions()
    }
  }

  /**
   * Register partitions that have been written since last registration.
   */
  private def registerPendingPartitions(): Unit = {
    (catalogConfig, tableName, lastSchema) match {
      case (Some(config), Some(tbl), Some(schema)) =>
        val currentDate = LocalDate.now().toString
        val partitionPath = Paths.get(path, currentDate).toString

        // Only register if partition exists and hasn't been registered yet
        if (Files.exists(Paths.get(partitionPath)) && !writtenPartitions.contains(currentDate)) {
          writtenPartitions.add(currentDate)

          val registerPartition = () => {
            val client = HiveMetastoreClient(config)
            try {
              // Ensure table is registered
              val dbName = extractDbName(tbl, config)
              val tblName = extractTableName(tbl)

              if (!client.tableExists(dbName, tblName)) {
                client.registerTable(tbl, schema, path) match {
                  case Success(_) => println(s"Registered table $tbl in HMS")
                  case Failure(e) => println(s"Failed to register table $tbl: ${e.getMessage}")
                }
              }

              // Register partition
              client.registerPartition(tbl, currentDate, partitionPath) match {
                case Success(_) => println(s"Registered partition $currentDate for table $tbl in HMS")
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

      case _ =>
        // No catalog configured or no data written yet
        ()
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
   * Create streaming writer for the given path without catalog integration.
   */
  def apply(path: String): VineStreamingWriter = {
    new VineStreamingWriter(path, None, None)
  }

  /**
   * Create streaming writer with Hive Metastore catalog integration.
   *
   * @param path Base path to Vine table
   * @param catalogConfig Catalog configuration
   * @param tableName Table name for HMS registration (e.g., "default.events")
   */
  def withCatalog(
    path: String,
    catalogConfig: CatalogConfig,
    tableName: String
  ): VineStreamingWriter = {
    new VineStreamingWriter(path, Some(catalogConfig), Some(tableName))
  }
}

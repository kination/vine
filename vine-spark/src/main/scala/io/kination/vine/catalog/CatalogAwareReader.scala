package io.kination.vine.catalog

import io.kination.vine.{VineModule, VineArrowBridge, VineTypeUtils}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

/**
 * Catalog reader that can use HMS partition metadata for optimized reads.
 *
 * This reader provides two main benefits over direct file reading:
 * 1. Partition pruning using HMS metadata (faster for selective queries)
 * 2. Schema reading from HMS instead of vine_meta.json
 */
object CatalogAwareReader {

  /**
   * Read specific partitions from HMS-registered table.
   *
   * @param spark SparkSession
   * @param catalogConfig HMS configuration
   * @param tableName Full table name (e.g., "default.events")
   * @param partitionFilter Optional partition filter (e.g., Some("2024-12-26") or None for all)
   * @return DataFrame containing the data
   */
  def read(
    spark: SparkSession,
    catalogConfig: CatalogConfig,
    tableName: String,
    partitionFilter: Option[String] = None
  ): DataFrame = {
    val client = HiveMetastoreClient(catalogConfig)

    try {
      val Array(dbName, tblName) = parseTableName(tableName, catalogConfig)

      // Get table metadata from HMS
      if (!client.tableExists(dbName, tblName)) {
        throw new IllegalArgumentException(
          s"Table $tableName not found in Hive Metastore. " +
          "Please register the table first using HiveMetastoreClient.registerTable()"
        )
      }

      // Get partitions to read
      val partitions = partitionFilter match {
        case Some(partition) =>
          if (client.partitionExists(dbName, tblName, partition)) {
            Seq(partition)
          } else {
            throw new IllegalArgumentException(s"Partition $partition not found for table $tableName")
          }
        case None =>
          // Read all partitions
          client.listPartitions(tableName).map { partSpec =>
            // Parse "date=2024-12-26" -> "2024-12-26"
            partSpec.split("=")(1)
          }
      }

      if (partitions.isEmpty) {
        // No partitions found, return empty DataFrame
        // We need schema - try to get from first available metadata
        val schema = getSchemaFromHMS(client, dbName, tblName)
        return spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      }

      // Read data from selected partitions
      val allRows = partitions.flatMap { partition =>
        readPartition(tableName, partition, catalogConfig)
      }

      // Get schema from HMS
      val schema = getSchemaFromHMS(client, dbName, tblName)

      spark.createDataFrame(spark.sparkContext.parallelize(allRows), schema)

    } finally {
      client.close()
    }
  }

  /**
   * Read data from a specific partition.
   */
  private def readPartition(
    tableName: String,
    partition: String,
    config: CatalogConfig
  ): Seq[Row] = {
    val client = HiveMetastoreClient(config)

    try {
      val Array(dbName, tblName) = parseTableName(tableName, config)

      // Get partition location from HMS
      val partitionSpec = s"date=$partition"

      // For now, construct path manually (HMS partition location would be used in full implementation)
      // In a complete implementation, we'd get this from HMS partition metadata
      val basePath = s"/data/$tblName"  // This should come from HMS table location
      val partitionPath = s"$basePath/$partition"

      // Read data using Arrow IPC
      val arrowBytes = VineModule.readDataArrow(partitionPath)

      if (arrowBytes == null || arrowBytes.isEmpty) {
        return Seq.empty
      }

      // Get schema from HMS
      val schema = getSchemaFromHMS(client, dbName, tblName)

      VineArrowBridge.arrowIpcToRows(arrowBytes, schema)

    } finally {
      client.close()
    }
  }

  /**
   * Get Spark schema from HMS table metadata.
   */
  private def getSchemaFromHMS(
    client: HiveMetastoreClient,
    dbName: String,
    tableName: String
  ): StructType = {
    // In a full implementation, this would read the HMS table schema
    // For now, we'll note this as a TODO
    // The HMS client would need a method like:
    // client.getTableSchema(dbName, tableName)

    // Placeholder: In reality, you'd convert HMS FieldSchema to Spark StructType
    throw new UnsupportedOperationException(
      "Schema reading from HMS not yet implemented. " +
      "Please use VineReader.read() which reads from vine_meta.json, " +
      "or query via Spark SQL using Catalog."
    )
  }

  private def parseTableName(fullName: String, config: CatalogConfig): Array[String] = {
    val parts = fullName.split("\\.")
    if (parts.length == 2) {
      parts
    } else {
      Array(config.database, fullName)
    }
  }
}

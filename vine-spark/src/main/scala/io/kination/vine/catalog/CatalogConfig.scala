package io.kination.vine.catalog

import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Configuration for Vine Catalog integration with Hive Metastore.
 *
 * @param metastoreUri Hive Metastore Thrift URI (e.g., "thrift://localhost:9083")
 * @param autoRegisterPartitions Whether to automatically register partitions on write
 * @param enableAsync Whether to register partitions asynchronously to avoid blocking writes
 * @param database Default database name
 */
case class CatalogConfig(
  metastoreUri: String,
  autoRegisterPartitions: Boolean = true,
  enableAsync: Boolean = true,
  database: String = "default"
)

object CatalogConfig {
  // Configuration keys
  val METASTORE_URI_KEY = "hive.metastore.uris"
  val AUTO_REGISTER_PARTITIONS_KEY = "auto-register-partitions"
  val ENABLE_ASYNC_KEY = "enable-async"
  val DATABASE_KEY = "database"

  /**
   * Create config from Spark's CaseInsensitiveStringMap.
   */
  def fromOptions(options: CaseInsensitiveStringMap): Option[CatalogConfig] = {
    Option(options.get(METASTORE_URI_KEY)).map { uri =>
      CatalogConfig(
        metastoreUri = uri,
        autoRegisterPartitions = options.getBoolean(AUTO_REGISTER_PARTITIONS_KEY, true),
        enableAsync = options.getBoolean(ENABLE_ASYNC_KEY, true),
        database = options.getOrDefault(DATABASE_KEY, "default")
      )
    }
  }
}

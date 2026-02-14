package io.kination.vine.catalog

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.{Partition, Table => HiveTable}
import org.apache.spark.sql.types.StructType

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Wrapper of Hive MetaStore Client
 *
 * @param config Vine catalog configuration
 */
class HiveMetastoreClient(config: CatalogConfig) {
  private val hiveConf = createHiveConf(config)
  @volatile private var client: Option[HiveMetaStoreClient] = None

  /**
   * Get or create HMS client.
   */
  private def getClient(): HiveMetaStoreClient = {
    client.getOrElse {
      synchronized {
        client.getOrElse {
          val newClient = new HiveMetaStoreClient(hiveConf)
          client = Some(newClient)
          newClient
        }
      }
    }
  }

  private def createHiveConf(config: CatalogConfig): HiveConf = {
    val conf = new HiveConf()
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, config.metastoreUri)
    // Skip metastore DB schema version check (client-side only, so okay to skip)
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION, false)
    conf
  }

  /**
   * Register Vine table in Hive Metastore.
   *
   * @param tableName Full table name (database.table)
   * @param schema Spark schema
   * @param location Table location path
   * 
   * @return Success or failure
   */
  def registerTable(
    tableName: String,
    schema: StructType,
    location: String
  ): Try[Unit] = Try {
    val Array(dbName, tblName) = parseTableName(tableName)

    if (tableExists(dbName, tblName)) {
      println(s"Table $tableName already exists in Hive Metastore, skipping registration")
      return Success(())
    }

    val hiveTable = MetadataConverter.createHiveTable(dbName, tblName, schema, location)

    getClient().createTable(hiveTable)
    println(s"Successfully registered Vine table $tableName in Hive Metastore")
  }

  /**
   * Register partition for table.
   *
   * @param tableName Full table name (database.table)
   * @param partitionValue Partition value (e.g., "2024-12-26")
   * @param location Partition location path
   * 
   * @return Success or failure
   */
  def registerPartition(
    tableName: String,
    partitionValue: String,
    location: String
  ): Try[Unit] = Try {
    val Array(dbName, tblName) = parseTableName(tableName)

    if (partitionExists(dbName, tblName, partitionValue)) {
      // Skip if partition already exists
      return Success(())
    }

    val table = getClient().getTable(dbName, tblName)
    val sd = table.getSd

    val partition = new Partition()
    partition.setDbName(dbName)
    partition.setTableName(tblName)
    partition.setValues(List(partitionValue).asJava)

    val partSd = sd.deepCopy()
    partSd.setLocation(location)
    partition.setSd(partSd)

    getClient().add_partition(partition)
    println(s"Successfully registered partition $partitionValue for table $tableName")
  }

  /**
   * Discover and register all date-based partitions from filesystem.
   *
   * @param tableName Full table name
   * @param basePath Base path to scan for partitions
   * 
   * @return Number of partitions registered
   */
  def discoverAndRegisterPartitions(
    tableName: String,
    basePath: String
  ): Int = {
    val dateDirs = findDateDirectories(basePath)
    var registered = 0

    dateDirs.foreach { dateDir =>
      val partitionValue = dateDir.getFileName.toString
      val partitionLocation = dateDir.toString

      registerPartition(tableName, partitionValue, partitionLocation) match {
        case Success(_) => registered += 1
        case Failure(e) => println(s"Failed to register partition $partitionValue: ${e.getMessage}")
      }
    }

    registered
  }

  /**
   * Find date-based partition directories (YYYY-MM-DD pattern).
   */
  private def findDateDirectories(basePath: String): Seq[java.nio.file.Path] = {
    val base = Paths.get(basePath)
    if (!Files.exists(base) || !Files.isDirectory(base)) {
      return Seq.empty
    }

    val datePattern = "\\d{4}-\\d{2}-\\d{2}".r

    Files.list(base)
      .iterator()
      .asScala
      .filter(p => Files.isDirectory(p) && datePattern.matches(p.getFileName.toString))
      .toSeq
      .sorted
  }

  /**
   * Check if table exists.
   */
  def tableExists(dbName: String, tableName: String): Boolean = {
    Try(getClient().getTable(dbName, tableName)).isSuccess
  }

  /**
   * Check if partition exists.
   */
  def partitionExists(dbName: String, tableName: String, partitionValue: String): Boolean = {
    Try {
      getClient().getPartition(dbName, tableName, List(partitionValue).asJava)
    }.isSuccess
  }

  /**
   * Drop table from Hive Metastore.
   */
  def dropTable(tableName: String, deleteData: Boolean = false): Try[Unit] = Try {
    val Array(dbName, tblName) = parseTableName(tableName)
    getClient().dropTable(dbName, tblName, deleteData, true)
    println(s"Successfully dropped table $tableName from Hive Metastore")
  }

  /**
   * List all partitions for table.
   */
  def listPartitions(tableName: String): Seq[String] = {
    val Array(dbName, tblName) = parseTableName(tableName)
    Try {
      getClient().listPartitionNames(dbName, tblName, Short.MaxValue)
        .asScala
        .toSeq
    }.getOrElse(Seq.empty)
  }

  /**
   * Parse table name into database and table.
   * Supports both "database.table" and "table" (uses default database).
   */
  private def parseTableName(fullName: String): Array[String] = {
    val parts = fullName.split("\\.")
    if (parts.length == 2) {
      parts
    } else {
      Array(config.database, fullName)
    }
  }

  /**
   * List all databases in Hive Metastore.
   */
  def getAllDatabases(): Array[String] = {
    getClient().getAllDatabases.asScala.toArray
  }

  /**
   * Close the HMS client.
   */
  def close(): Unit = {
    client.foreach(_.close())
    client = None
  }
}

object HiveMetastoreClient {
  /**
   * Create new HMS client from configuration.
   */
  def apply(config: CatalogConfig): HiveMetastoreClient = {
    new HiveMetastoreClient(config)
  }
}

package io.kination.vine.catalog

import io.kination.vine.{VineDataSource, VineModule}
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

/**
 * Catalog implementation for Hive Metastore integration.
 * This allows Spark to discover and query Vine tables registered in Hive Metastore.
 *
 * Usage in spark-defaults.conf:
 * {{{
 * spark.sql.catalog.vine = io.kination.vine.catalog.Catalog
 * spark.sql.catalog.vine.hive.metastore.uris = thrift://localhost:9083
 * spark.sql.catalog.vine.auto-register-partitions = true
 * }}}
 *
 * Then in Spark SQL:
 * {{{
 * spark.sql("SELECT * FROM vine.default.events WHERE date = '2024-12-26'")
 * }}}
 */
class Catalog extends TableCatalog with SupportsNamespaces {
  private var catalogName: String = _
  private var config: Option[CatalogConfig] = None
  private var hmsClient: Option[HiveMetastoreClient] = None

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    this.config = CatalogConfig.fromOptions(options)

    // Initialize HMS client if metastore URI is configured
    this.config.foreach { cfg =>
      this.hmsClient = Some(HiveMetastoreClient(cfg))
      println(s"Initialized Vine Catalog '$name' with Hive Metastore: ${cfg.metastoreUri}")
    }

    if (this.config.isEmpty) {
      println(s"Warning: Vine Catalog '$name' initialized without Hive Metastore configuration")
    }
  }

  override def name(): String = catalogName

  /**
   * List available namespaces (databases).
   */
  override def listNamespaces(): Array[Array[String]] = {
    hmsClient match {
      case Some(client) =>
        client.getAllDatabases().map(db => Array(db))
      case None =>
        Array(Array("default"))
    }
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    if (namespace.length == 0) {
      listNamespaces()
    } else {
      Array.empty
    }
  }

  /**
   * Load namespace properties.
   */
  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    if (namespace.length != 1) {
      throw new NoSuchNamespaceException(namespace)
    }

    val props = new util.HashMap[String, String]()
    props.put("namespace", namespace(0))
    props
  }

  /**
   * Check if namespace exists.
   */
  override def namespaceExists(namespace: Array[String]): Boolean = {
    if (namespace.length != 1) return false
    hmsClient match {
      case Some(client) =>
        client.getAllDatabases().contains(namespace(0))
      case None =>
        namespace(0) == "default"
    }
  }

  /**
   * Create namespace (not supported - use Hive directly).
   */
  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    throw new NamespaceAlreadyExistsException(namespace)
  }

  /**
   * Alter namespace (not supported).
   */
  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    throw new UnsupportedOperationException("alterNamespace is not supported")
  }

  /**
   * Drop namespace (not supported - use Hive directly).
   */
  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    throw new UnsupportedOperationException("dropNamespace is not supported")
  }

  /**
   * List tables in a namespace.
   */
  override def listTables(namespace: Array[String]): Array[Identifier] = {
    // In a full implementation, this would query HMS
    // For now, return empty
    Array.empty
  }

  /**
   * Load a table.
   */
  override def loadTable(ident: Identifier): Table = {
    val tableName = ident.toString

    hmsClient match {
      case Some(client) =>
        val Array(dbName, tblName) = parseIdentifier(ident)

        if (!client.tableExists(dbName, tblName)) {
          throw new NoSuchTableException(ident)
        }

        // Load table metadata from HMS and create Vine table
        // For now, delegate to VineDataSource
        val dataSource = new VineDataSource()
        // Note: In a full implementation, we'd get the location from HMS
        // and pass it to the data source
        throw new NoSuchTableException(s"Table loading from HMS not yet fully implemented: $tableName")

      case None =>
        throw new UnsupportedOperationException("Hive Metastore not configured")
    }
  }

  /**
   * Create a table and register it in Hive Metastore.
   */
  override def createTable(
    ident: Identifier,
    schema: StructType,
    partitions: Array[Transform],
    properties: util.Map[String, String]
  ): Table = {
    val Array(dbName, tblName) = parseIdentifier(ident)
    val location = properties.get("location")

    if (location == null) {
      throw new IllegalArgumentException("Table location must be specified in properties")
    }

    hmsClient match {
      case Some(client) =>
        // Check if table already exists
        if (client.tableExists(dbName, tblName)) {
          throw new TableAlreadyExistsException(ident)
        }

        // Register table in HMS
        client.registerTable(ident.toString, schema, location) match {
          case Success(_) =>
            // Discover and register existing partitions
            if (config.exists(_.autoRegisterPartitions)) {
              val registered = client.discoverAndRegisterPartitions(ident.toString, location)
              println(s"Auto-registered $registered partitions for table ${ident.toString}")
            }

            // Return a Vine table instance
            // Note: This is a simplified implementation
            throw new UnsupportedOperationException("createTable not yet fully implemented")

          case Failure(e) =>
            throw new RuntimeException(s"Failed to register table ${ident.toString} in Hive Metastore", e)
        }

      case None =>
        throw new UnsupportedOperationException("Hive Metastore not configured")
    }
  }

  /**
   * Alter table (not supported yet).
   */
  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("alterTable is not yet supported")
  }

  /**
   * Drop a table from Hive Metastore.
   */
  override def dropTable(ident: Identifier): Boolean = {
    hmsClient match {
      case Some(client) =>
        client.dropTable(ident.toString, deleteData = false) match {
          case Success(_) => true
          case Failure(_) => false
        }

      case None =>
        throw new UnsupportedOperationException("Hive Metastore not configured")
    }
  }

  /**
   * Rename table (not supported yet).
   */
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("renameTable is not yet supported")
  }

  /**
   * Parse Identifier into database and table name.
   */
  private def parseIdentifier(ident: Identifier): Array[String] = {
    val namespace = ident.namespace()
    val tableName = ident.name()

    if (namespace.length == 0) {
      Array(config.map(_.database).getOrElse("default"), tableName)
    } else if (namespace.length == 1) {
      Array(namespace(0), tableName)
    } else {
      throw new IllegalArgumentException(s"Invalid identifier: $ident")
    }
  }

  /**
   * Get HMS client for external use.
   */
  def getHmsClient(): Option[HiveMetastoreClient] = hmsClient

  /**
   * Close the catalog and release resources.
   */
  def close(): Unit = {
    hmsClient.foreach(_.close())
  }
}

object Catalog {
  /**
   * Register a Vine table in Hive Metastore from Spark.
   *
   * Example usage:
   * {{{
   * val catalog = spark.sessionState.catalog.asInstanceOf[Catalog]
   * Catalog.registerTable(
   *   catalog,
   *   "default.events",
   *   spark.read.format("vine").load("/data/events").schema,
   *   "/data/events"
   * )
   * }}}
   */
  def registerTable(
    catalog: Catalog,
    tableName: String,
    schema: StructType,
    location: String
  ): Unit = {
    catalog.getHmsClient() match {
      case Some(client) =>
        client.registerTable(tableName, schema, location) match {
          case Success(_) =>
            val partitions = client.discoverAndRegisterPartitions(tableName, location)
            println(s"Successfully registered table $tableName with $partitions partitions")

          case Failure(e) =>
            throw new RuntimeException(s"Failed to register table $tableName", e)
        }

      case None =>
        throw new UnsupportedOperationException("Hive Metastore not configured in catalog")
    }
  }
}

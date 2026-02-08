package io.kination.vine.catalog

import org.apache.hadoop.hive.metastore.api.{FieldSchema, SerDeInfo, StorageDescriptor, Table => HiveTable}
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

/**
 * Converts between Vine metadata and Hive Metastore table structures.
 */
object MetadataConverter {

  /**
   * Convert Spark StructType to Hive FieldSchema list.
   */
  def sparkSchemaToHiveFields(schema: StructType): java.util.List[FieldSchema] = {
    schema.fields.map { field =>
      new FieldSchema(
        field.name,
        sparkTypeToHiveType(field.dataType),
        s"Vine field: ${field.name}"
      )
    }.toList.asJava
  }

  /**
   * Convert Spark DataType to Hive type string.
   */
  def sparkTypeToHiveType(dataType: DataType): String = dataType match {
    case ByteType => serdeConstants.TINYINT_TYPE_NAME
    case ShortType => serdeConstants.SMALLINT_TYPE_NAME
    case IntegerType => serdeConstants.INT_TYPE_NAME
    case LongType => serdeConstants.BIGINT_TYPE_NAME
    case FloatType => serdeConstants.FLOAT_TYPE_NAME
    case DoubleType => serdeConstants.DOUBLE_TYPE_NAME
    case StringType => serdeConstants.STRING_TYPE_NAME
    case BooleanType => serdeConstants.BOOLEAN_TYPE_NAME
    case BinaryType => serdeConstants.BINARY_TYPE_NAME
    case DateType => serdeConstants.DATE_TYPE_NAME
    case TimestampType => serdeConstants.TIMESTAMP_TYPE_NAME
    case _: DecimalType => serdeConstants.DECIMAL_TYPE_NAME
    case _ => serdeConstants.STRING_TYPE_NAME // fallback
  }

  /**
   * Create Hive partition fields (currently just "date" for date-based partitioning).
   */
  def createPartitionFields(): java.util.List[FieldSchema] = {
    List(
      new FieldSchema("date", serdeConstants.STRING_TYPE_NAME, "Date partition (YYYY-MM-DD)")
    ).asJava
  }

  /**
   * Create StorageDescriptor for Vine table.
   * Uses Vine-specific format identifiers so other engines don't
   * mistakenly try to read .vtx files as Parquet.
   * 
   * Vine connectors (vine-spark, vine-trino) bypass SerDe and read via JNI.
   * 
   */
  def createStorageDescriptor(
    schema: StructType,
    location: String
  ): StorageDescriptor = {
    val sd = new StorageDescriptor()
    sd.setCols(sparkSchemaToHiveFields(schema))
    sd.setLocation(location)
    sd.setInputFormat("io.kination.vine.serde.VortexInputFormat")
    sd.setOutputFormat("io.kination.vine.serde.VortexOutputFormat")

    // Set SerDe info
    val serDeInfo = new SerDeInfo()
    serDeInfo.setName("vine")
    serDeInfo.setSerializationLib("io.kination.vine.serde.VortexSerDe")
    sd.setSerdeInfo(serDeInfo)

    // Set storage properties
    sd.setCompressed(false)
    sd.setNumBuckets(-1)
    sd.setStoredAsSubDirectories(false)

    sd
  }

  /**
   * Create Hive Table metadata for Vine table.
   */
  def createHiveTable(
    dbName: String,
    tableName: String,
    schema: StructType,
    location: String
  ): HiveTable = {
    val table = new HiveTable()
    table.setDbName(dbName)
    table.setTableName(tableName)
    table.setTableType("EXTERNAL_TABLE")
    table.setOwner(System.getProperty("user.name"))

    table.setSd(createStorageDescriptor(schema, location))
    table.setPartitionKeys(createPartitionFields())

    // Set table parameters
    val parameters = new java.util.HashMap[String, String]()
    parameters.put("EXTERNAL", "TRUE")
    parameters.put("vine.version", "0.2.0")
    parameters.put("transient_lastDdlTime", (System.currentTimeMillis() / 1000).toString)
    table.setParameters(parameters)

    table
  }
}

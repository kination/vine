package io.kination.vine

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Utility functions for converting between Spark and Vine types.
 *
 */
object VineTypeUtils {

  /**
   * Map Spark DataType to Vine/Vortex type string.
   *
   * Used when creating vine_meta.json schema files.
   *
   * Type mappings:
   * - Integer types: byte, short, integer, long
   * - Floating point: float, double
   * - Other: boolean, string, binary
   * - Date/Time: date, timestamp
   * - Decimal (arbitrary precision)
   *
   * @param dataType Spark DataType
   * @return Vine type string
   */
  def sparkTypeToVineType(dataType: DataType): String = dataType match {
    case ByteType => "byte"
    case ShortType => "short"
    case IntegerType => "integer"
    case LongType => "long"
    case FloatType => "float"
    case DoubleType => "double"
    case BooleanType => "boolean"
    case StringType => "string"
    case BinaryType => "binary"
    case DateType => "date"
    case TimestampType => "timestamp"
    case _: DecimalType => "decimal"
    case _ => "string"  // Fallback for unsupported types
  }

  /**
   * Map Vine/Vortex type string to Spark DataType.
   *
   * Used when reading vine_meta.json to construct Spark schema.
   * Supports both Vine type names and SQL-like aliases.
   *
   * Type mappings (with aliases):
   * - Integer types: byte/tinyint, short/smallint, integer/int, long/bigint
   * - Floating point: float, double
   * - Other: boolean/bool, string, binary
   * - Date/Time: date, timestamp
   * - Decimal (default precision 38,18)
   *
   * @param vineType Vine type string from metadata
   * @return Spark DataType
   */
  def vineTypeToSparkType(vineType: String): DataType = vineType.toLowerCase match {
    case "byte" | "tinyint" => ByteType
    case "short" | "smallint" => ShortType
    case "integer" | "int" => IntegerType
    case "long" | "bigint" => LongType
    case "float" => FloatType
    case "double" => DoubleType
    case "boolean" | "bool" => BooleanType
    case "string" => StringType
    case "binary" => BinaryType
    case "date" => DateType
    case "timestamp" => TimestampType
    case "decimal" => DecimalType(38, 18)  // Default precision
    case _ => StringType  // Fallback
  }

  /**
   * Format a Spark Row to CSV string for JNI.
   *
   * Handles all Vine/Vortex types with appropriate conversions:
   * - DateType: converts days-since-epoch to YYYY-MM-DD format
   * - BinaryType: Base64 encodes binary data
   * - Nulls: represented as empty strings
   *
   * @param row Spark Row to format
   * @param schema Schema of the row
   * @return CSV-formatted string
   */
  def formatRow(row: Row, schema: StructType): String = {
    schema.fields.zipWithIndex.map { case (field, idx) =>
      if (row.isNullAt(idx)) {
        ""
      } else {
        formatValue(row, idx, field.dataType)
      }
    }.mkString(",")
  }

  /**
   * Format a Spark InternalRow to CSV string for JNI.
   *
   * Similar to formatRow but works with Spark's internal representation.
   * Used in DataSource V2 write path for better performance.
   *
   * @param record InternalRow to format
   * @param schema Schema of the row
   * @return CSV-formatted string
   */
  def formatInternalRow(record: InternalRow, schema: StructType): String = {
    schema.fields.zipWithIndex.map { case (field, idx) =>
      if (record.isNullAt(idx)) {
        ""
      } else {
        formatInternalValue(record, idx, field.dataType)
      }
    }.mkString(",")
  }

  /**
   * Parse string value to Spark internal type.
   *
   * Used in read path to convert CSV data (from JNI) to Spark types.
   * Handles all Vine/Vortex types with appropriate parsing:
   * - DateType: parses YYYY-MM-DD to days-since-epoch
   * - TimestampType: handles both epoch millis and ISO format
   * - BinaryType: Base64 decodes
   * - BooleanType: accepts multiple representations (true/false, 1/0, yes/no)
   *
   * @param value String value to parse
   * @param dataType Target Spark DataType
   * @return Parsed value in Spark's internal representation
   */
  def parseValue(value: String, dataType: DataType): Any = dataType match {
    case StringType => UTF8String.fromString(value)
    case IntegerType => value.toInt
    case LongType => value.toLong
    case DoubleType => value.toDouble
    case FloatType => value.toFloat
    case BooleanType => value.toLowerCase match {
      case "true" | "1" | "yes" => true
      case _ => false
    }
    case ShortType => value.toShort
    case ByteType => value.toByte
    case DateType =>
      // Parse YYYY-MM-DD to days since epoch
      java.time.LocalDate.parse(value).toEpochDay.toInt
    case TimestampType =>
      // Parse timestamp (epoch millis or ISO format)
      try {
        value.toLong  // Epoch milliseconds
      } catch {
        case _: NumberFormatException =>
          // Try ISO format
          java.time.Instant.parse(value).toEpochMilli
      }
    case BinaryType =>
      // Base64 decode
      java.util.Base64.getDecoder.decode(value)
    case dt: DecimalType =>
      Decimal(new java.math.BigDecimal(value), dt.precision, dt.scale)
    case _ => UTF8String.fromString(value)  // Fallback
  }

  /**
   * Format a value from a Row for the given data type.
   */
  private def formatValue(row: Row, idx: Int, dataType: DataType): String = dataType match {
    case StringType => row.getString(idx)
    case IntegerType => row.getInt(idx).toString
    case LongType => row.getLong(idx).toString
    case DoubleType => row.getDouble(idx).toString
    case BooleanType => row.getBoolean(idx).toString
    case FloatType => row.getFloat(idx).toString
    case ShortType => row.getShort(idx).toString
    case ByteType => row.getByte(idx).toString
    case TimestampType => row.getLong(idx).toString
    case DateType =>
      // Convert Spark DateType (days since epoch) to YYYY-MM-DD format
      val days = row.getInt(idx)
      java.time.LocalDate.ofEpochDay(days).toString
    case BinaryType =>
      // Base64 encode binary data
      java.util.Base64.getEncoder.encodeToString(row.getAs[Array[Byte]](idx))
    case _: DecimalType =>
      row.getDecimal(idx).toString
    case _ => row.get(idx).toString  // Fallback
  }

  /**
   * Format a value from an InternalRow for the given data type.
   */
  private def formatInternalValue(record: InternalRow, idx: Int, dataType: DataType): String = dataType match {
    case StringType => record.getString(idx)
    case IntegerType => record.getInt(idx).toString
    case LongType => record.getLong(idx).toString
    case DoubleType => record.getDouble(idx).toString
    case BooleanType => record.getBoolean(idx).toString
    case FloatType => record.getFloat(idx).toString
    case ShortType => record.getShort(idx).toString
    case ByteType => record.getByte(idx).toString
    case TimestampType => record.getLong(idx).toString
    case DateType =>
      // Convert days since epoch to YYYY-MM-DD format
      val days = record.getInt(idx)
      java.time.LocalDate.ofEpochDay(days).toString
    case BinaryType =>
      // Base64 encode binary data
      val bytes = record.getBinary(idx)
      java.util.Base64.getEncoder.encodeToString(bytes)
    case _: DecimalType =>
      val dt = dataType.asInstanceOf[DecimalType]
      record.getDecimal(idx, dt.precision, dt.scale).toString
    case _ => record.get(idx, dataType).toString  // Fallback
  }
}

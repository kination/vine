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

}

package io.kination.vine

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

/**
 * Reader for Vine tables.
 */
object VineReader {

  /**
   * Read Vine table as DataFrame, using Arrow IPC format.
   * Schema is inferred from "vine_meta.json".
   *
   * @param spark SparkSession
   * @param path Directory path to Vine table
   * @return DataFrame containing the data
   */
  def read(spark: SparkSession, path: String): DataFrame = {
    // Read schema from vine_meta.json
    val metaPath = s"$path/vine_meta.json"
    val schemaOpt = readSchemaFromMeta(metaPath)

    schemaOpt match {
      case Some(schema) => read(spark, path, schema)
      case None =>
        throw new IllegalArgumentException(
          s"Schema file not found at $metaPath. " +
          "Vine tables require vine_meta.json to get schema definition."
        )
    }
  }

  /**
   * Read Vine table with explicit schema using Arrow IPC format.
   *
   * @param spark SparkSession
   * @param path Directory path to Vine table
   * @param schema Expected schema
   * @return DataFrame containing the data
   */
  def read(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    val arrowBytes = VineModule.readDataArrow(path)

    if (arrowBytes == null || arrowBytes.isEmpty) {
      return spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }

    val rows = VineArrowBridge.arrowIpcToRows(arrowBytes, schema)
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  /**
   * Read Vine table as raw Arrow IPC bytes.
   *
   * @param path Directory path to Vine table
   * @return Arrow IPC stream bytes
   */
  def readRaw(path: String): Array[Byte] = {
    VineModule.readDataArrow(path)
  }

  /**
   * Read schema from vine_meta.json.
   */
  private def readSchemaFromMeta(metaPath: String): Option[StructType] = {
    try {
      val file = new java.io.File(metaPath)
      if (!file.exists()) return None

      implicit val formats: DefaultFormats.type = DefaultFormats
      val jsonStr = Source.fromFile(file).mkString
      val json = parse(jsonStr)

      val fields = (json \ "fields").extract[List[Map[String, Any]]]
      val sparkFields = fields.map { field =>
        val name = field("name").asInstanceOf[String]
        val dataType = field("data_type").asInstanceOf[String]
        val isRequired = field.get("is_required").exists {
          case b: Boolean => b
          case _ => false
        }

        StructField(name, vineTypeToSparkType(dataType), nullable = !isRequired)
      }

      Some(StructType(sparkFields))
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Map Vine/Vortex type string to Spark DataType.
   */
  private def vineTypeToSparkType(vineType: String): DataType = {
    VineTypeUtils.vineTypeToSparkType(vineType)
  }
}

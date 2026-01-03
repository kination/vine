package io.kination.vine

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source

/**
 * Reader for Vine tables
 * Provides methods to read Vine table into Spark DataFrame.
 */
object VineReader {

  /**
   * Read Vine table as DataFrame.
   * Schema is inferred from vine_meta.json if exists.
   *
   * @param spark SparkSession
   * @param path Directory path to Vine table
   * @return DataFrame containing the data
   */
  def read(spark: SparkSession, path: String): DataFrame = {
    // Try to read schema from vine_meta.json
    val metaPath = s"$path/vine_meta.json"
    val schemaOpt = readSchemaFromMeta(metaPath)

    schemaOpt match {
      case Some(schema) => read(spark, path, schema)
      case None =>
        // Fallback to inference
        val csvData = VineModule.readDataFromVine(path)
        if (csvData == null || csvData.trim.isEmpty) {
          spark.emptyDataFrame
        } else {
          import spark.implicits._
          val lines = csvData.split("\n").toSeq
          spark.read
            .option("inferSchema", "true")
            .option("header", "false")
            .csv(lines.toDS())
        }
    }
  }

  /**
   * Read Vine table with explicit schema.
   *
   * @param spark SparkSession
   * @param path Directory path to Vine table
   * @param schema Expected schema
   * @return DataFrame containing the data
   */
  def read(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    val csvData = VineModule.readDataFromVine(path)

    if (csvData == null || csvData.trim.isEmpty) {
      return spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }

    import spark.implicits._
    val lines = csvData.split("\n").toSeq
    spark.read
      .schema(schema)
      .option("header", "false")
      .csv(lines.toDS())
  }

  /**
   * Read Vine table as raw CSV string.
   *
   * @param path Directory path to Vine table
   * @return CSV-formatted data (one row per line)
   */
  def readRaw(path: String): String = {
    VineModule.readDataFromVine(path)
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

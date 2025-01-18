package io.kination.vine;

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.json4s._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import scala.io.Source
import java.util
import scala.reflect.io.File


class VineDataSource extends TableProvider {

  // Set this as "true", to get schema information based on DataFrame
  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // TODO: Make infer logic
    new StructType()
    // val path = options.get("path")
    // val sampleData = readSampleData(path)
    // inferSchemaFromData(sampleData)
  }

  /**
    * - Read schema information through "vine_meta.json" file inside root dir
    * - TODO: Add support for other data types
    * - TODO: Figure out why error message "Cannot write nullable values to non-null column" happens though data is non-null
    */
  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    val schemaFile = properties.get("path")
    val metaPath = s"$schemaFile/vine_meta.json"
    
    val metadataSchema = if (File(metaPath).exists) {
      println("Read schema from metadata")
      readSchemaFromMetadata(metaPath)
    } else {
      println("Read schema from DF")
      schema
    }

    println(f"Schema -> $metadataSchema")

    new VineTable(metadataSchema)
  }

  private def readSchemaFromMetadata(path: String): StructType = {
    val schemaJson = Source.fromFile(path).getLines().mkString
    implicit val formats: DefaultFormats.type = DefaultFormats
    val parsedJson = parse(schemaJson)
    val fields = (parsedJson \ "fields").extract[List[Map[String, Any]]]
    val structFields = fields.map { field =>
      val name = field("name").toString
      val dataType = field("data_type").toString match {
        case "integer" => IntegerType
        case "string" => StringType
        // TODO: add more...
        case other    => throw new IllegalArgumentException(s"Unsupported data type: $other")
      }
      val isRequired = field("is_required").asInstanceOf[Boolean]
      StructField(name, dataType, nullable = !isRequired)
    }

    StructType(structFields)
  }

}

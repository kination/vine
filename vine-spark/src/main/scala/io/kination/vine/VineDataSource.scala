package io.kination.vine;

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.io.Source
import java.util


class VineDataSource extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // TODO: Make infer logic
    new StructType()
  }

  /**
    * - Read schema information through "vine_meta.json" file inside root dir
    * - TODO: Add support for other data types
    * - TODO: Figure out why error message "Cannot write nullable values to non-null column" happens though data is non-null
    */
  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    val schemaFile = properties.get("path")
    val metaPath = s"$schemaFile/vine_meta.json"
    val metadataSchema = readSchemaFromMetadata(metaPath)
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
        case "i32"    => IntegerType
        case "String" => StringType
        // TODO: add more...
        case other    => throw new IllegalArgumentException(s"Unsupported data type: $other")
      }
      val isRequired = field("is_required").asInstanceOf[Boolean]
      StructField(name, dataType, nullable = !isRequired)
    }

    StructType(structFields)
  }

}

package io.kination.vine

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types._

import org.json4s._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import scala.reflect.io.File

/**
 * WriteBuilder for DataSource V2.
 * Handles schema management and create batch/streaming writers.
 */
class VineDataSourceWriteBuilder(schema: StructType, info: LogicalWriteInfo) extends WriteBuilder {

  private val path: String = info.options().get("path")

  override def buildForBatch(): BatchWrite = {
    val updatedSchema = updateSchema(schema, info)
    new VineDataSourceWriter(updatedSchema, info, path)
  }

  override def buildForStreaming(): StreamingWrite = super.buildForStreaming()

  private def updateSchema(tableSchema: StructType, info: LogicalWriteInfo): StructType = {
    val metaPath = s"$path/vine_meta.json"

    if (File(metaPath).exists) {
      tableSchema
    } else {
      createNewMetadataFile(info)
      info.schema()
    }
  }

  /**
   * Create vine_meta.json with 'Vortex-compatible' type mappings.
   * Maps Spark types to Vine/Vortex types
   */
  private def createNewMetadataFile(info: LogicalWriteInfo): Unit = {
    val metaPath = s"$path/vine_meta.json"

    // Ensure directory exists
    val dir = new java.io.File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }

    implicit val formats: DefaultFormats.type = DefaultFormats
    val fields = info.schema().fields.zipWithIndex.map { case (field, index) =>
      Map(
        "id" -> (index + 1),
        "name" -> field.name,
        "data_type" -> sparkTypeToVineType(field.dataType),
        "is_required" -> !field.nullable
      )
    }

    val schemaJson = compact(render(
      Extraction.decompose(Map(
        "table_name" -> path,
        "fields" -> fields
      ))
    ))

    new java.io.PrintWriter(metaPath) { write(schemaJson); close() }
  }

  /**
   * Map Spark DataType to Vine/Vortex type string.
   */
  private def sparkTypeToVineType(dataType: DataType): String = {
    VineTypeUtils.sparkTypeToVineType(dataType)
  }
}

/**
 * BatchWrite implementation
 * This creates 'VineDataWriterFactory' for each partition
 */
class VineDataSourceWriter(
    schema: StructType,
    info: LogicalWriteInfo,
    path: String
) extends BatchWrite {

  override def createBatchWriterFactory(physicalInfo: PhysicalWriteInfo): DataWriterFactory = {
    new VineDataWriterFactory(schema, physicalInfo, path)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // Log commit statistics if needed
    val totalRows = messages.collect {
      case msg: VineWriterCommitMessage => msg.rowsWritten
    }.sum
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // Nothing to clean up - partial writes are acceptable in append mode
  }
}

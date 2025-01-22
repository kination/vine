package io.kination.vine

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types.StructType

import org.json4s._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import scala.reflect.io.File

/**
  * 
  * TODO:
      - Compare schema inside param-schema and info.schema
      - Decide where should we merge/update schema
        - By updating logical schema here, we can focus on data handling on DataWriter
        - But because PhysicalWriteInfo not exists on this stage, we cannot handle logic base on partitioning column
        - So there can be duplicates in future.
  * 
  * 
  */
class VineDataSourceWriteBuilder(schema: StructType, info: LogicalWriteInfo) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = {
    val updatedSchema = updateSchema(schema, info)
    new VineDataSourceWriter(updatedSchema, info)
  }

  override def buildForStreaming(): StreamingWrite = super.buildForStreaming()

  // TODO: update schema by comparing 2 data
  private def updateSchema(tableSchema: StructType, info: LogicalWriteInfo): StructType = {
    val schemaFile = info.options().get("path")
    val metaPath = s"$schemaFile/vine_meta.json"
    
    if (File(metaPath).exists) {
      tableSchema
    } else {
      createNewMetadataFile(info)
      info.schema()
    }
  }

  private def createNewMetadataFile(info: LogicalWriteInfo) {
    val path = info.options().get("path")
    val metaPath = s"$path/vine_meta.json"
    
    implicit val formats: DefaultFormats.type = DefaultFormats
    val fields = info.schema().fields.zipWithIndex.map { case (field, index) =>
      Map(
        "id" -> (index + 1),
        "name" -> field.name,
        "data_type" -> field.dataType.typeName,
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
}

class VineDataSourceWriter(schema: StructType, info: LogicalWriteInfo) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new VineDataWriterFactory(schema, info)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}

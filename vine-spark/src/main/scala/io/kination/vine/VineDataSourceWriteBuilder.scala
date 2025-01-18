package io.kination.vine

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types.StructType

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
    val updatedSchema = updateSchema(schema, info.schema())
    new VineDataSourceWriter(updatedSchema, info)
  }

  override def buildForStreaming(): StreamingWrite = super.buildForStreaming()

  // TODO: update schema by comparing 2 data
  private def updateSchema(tableSchema: StructType, logicalSchema: StructType): StructType = {
    /* 
    StructType(tableSchema.fields.map { field =>
      writeSchema.find(_.name == field.name).getOrElse(field)
    })
     */
    tableSchema
  }
}

class VineDataSourceWriter(schema: StructType, info: LogicalWriteInfo) extends BatchWrite {

  println(f"logical -> $info")

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    println(f"physical -> $info")
    new VineDataWriterFactory(schema, info)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}

package io.kination.vine

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import scala.collection.mutable.ListBuffer

class VineDataWriterFactory(schema: StructType, info: PhysicalWriteInfo) extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new VineDataWriter(schema, info)
  }
}

class VineDataWriter(schema: StructType, info: PhysicalWriteInfo) extends DataWriter[InternalRow] {
  // TODO:
  //  - This is only supporting for 2 col only. Make data based on schema
  //  - Put actual path from configs
  //  - Think of setting up buffer
  //  - Think of how-to make commit based on multiple partition
  private val buffer = ListBuffer[String]()
  private val bufferSize = 10

  override def write(record: InternalRow): Unit = {
    if (schema.fields.size != record.numFields) {
      // TODO: number of schema fields and actual record fields are not matching.
      // Make proper update
    }

    val data = schema.fields.zipWithIndex.map { case (field, idx) =>
      field.dataType match {
        case org.apache.spark.sql.types.StringType => record.getString(idx)
        case org.apache.spark.sql.types.IntegerType => record.getInt(idx).toString
        case org.apache.spark.sql.types.LongType => record.getLong(idx).toString
        case org.apache.spark.sql.types.DoubleType => record.getDouble(idx).toString
        case org.apache.spark.sql.types.BooleanType => record.getBoolean(idx).toString
        case org.apache.spark.sql.types.TimestampType => record.getLong(idx).toString
        // Add more types as needed
        case _ => record.getString(idx) // fallback to string for unsupported types
      }
    }.mkString(",")
    

    buffer += data
    // println(buffer)
    if (buffer.size >= bufferSize) {
      flushBuffer()
    }
  }

  override def commit(): WriterCommitMessage = {
    if (buffer.nonEmpty) {
      flushBuffer()
    }
    null
  }

  override def abort(): Unit = {}

  override def close(): Unit = {}

  private def flushBuffer(): Unit = {
    val mergeBuffer = buffer.mkString("\n")
    VineModule.writeData("vine-test/result", mergeBuffer)
    buffer.clear()
  }
}

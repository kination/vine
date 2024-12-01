package io.kination.vine
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  Write,
  WriteBuilder,
  WriterCommitMessage,
  DataWriter,
  DataWriterFactory,
  LogicalWriteInfo,
  PhysicalWriteInfo
}

class MyDataSourceWriteBuilder(info: LogicalWriteInfo) extends WriteBuilder {

  override def build(): Write = {
    new BatchWrite {
      override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
        new SimpleDataWriterFactory
      }

      override def commit(messages: Array[WriterCommitMessage]): Unit = {

      }

      override def abort(messages: Array[WriterCommitMessage]): Unit = {

      }
    }
  }
}

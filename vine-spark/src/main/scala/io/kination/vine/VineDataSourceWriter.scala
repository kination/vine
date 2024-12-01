package io.kination.vine

import org.apache.spark.sql.connector.write._


class VineDataSourceWriter(info: LogicalWriteInfo) extends WriteBuilder with BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new SimpleDataWriterFactory
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}
package io.kination.vine

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite


class VineDataSourceWriteBuilder(info: LogicalWriteInfo) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = {
    new VineDataSourceWriter(info)
  }

  override def buildForStreaming(): StreamingWrite = super.buildForStreaming()
}

class VineDataSourceWriter(info: LogicalWriteInfo) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new VineDataWriterFactory
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}

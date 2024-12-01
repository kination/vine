import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}

class SimpleDataWriterFactory extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new SimpleDataWriter
  }
}

class SimpleDataWriter extends DataWriter[InternalRow] {

  override def write(record: InternalRow): Unit = {
    // TODO: put actual data
    val data = record.getString(0) + record.getString(1)

  }

  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = ???

  override def close(): Unit = ???
}

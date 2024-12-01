import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.unsafe.types.UTF8String

class SimplePartitionReaderFactory extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new SimplePartitionReader
  }
}

class SimplePartitionReader extends PartitionReader[InternalRow] {
  // TODO: put actual data
  private val data = List("1","2","3","4","5").iterator

  override def next(): Boolean = data.hasNext

  override def get(): InternalRow = {
    val row = data.next().split(",")
    // TODO: setup actual row data
    InternalRow(UTF8String.fromString(row(0)), UTF8String.fromString(row(1)))
  }

  override def close(): Unit = {}
}

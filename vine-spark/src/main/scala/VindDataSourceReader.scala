import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class VineDataSourceReader(options: CaseInsensitiveStringMap) extends ScanBuilder with Batch {

  override def build(): Scan = ???

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new SimpleInputPartition)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new SimplePartitionReaderFactory
  }
}

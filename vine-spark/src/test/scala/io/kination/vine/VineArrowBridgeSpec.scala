package io.kination.vine

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Date, Timestamp}

/**
 * Unit tests for VineArrowBridge.
 *
 * Tests Arrow IPC conversion between Spark Row/InternalRow and Arrow format.
 */
class VineArrowBridgeSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    VineArrowBridge.close()
    super.afterAll()
  }

  "VineArrowBridge.sparkSchemaToArrowSchema" should "convert all Spark types correctly" in {
    val sparkSchema = StructType(Seq(
      StructField("byte_col", ByteType, nullable = false),
      StructField("short_col", ShortType, nullable = false),
      StructField("int_col", IntegerType, nullable = false),
      StructField("long_col", LongType, nullable = false),
      StructField("float_col", FloatType, nullable = false),
      StructField("double_col", DoubleType, nullable = false),
      StructField("bool_col", BooleanType, nullable = false),
      StructField("string_col", StringType, nullable = true),
      StructField("binary_col", BinaryType, nullable = true),
      StructField("date_col", DateType, nullable = true),
      StructField("timestamp_col", TimestampType, nullable = true),
      StructField("decimal_col", DecimalType(10, 2), nullable = true)
    ))

    val arrowSchema = VineArrowBridge.sparkSchemaToArrowSchema(sparkSchema)

    arrowSchema.getFields.size() should be(12)
    arrowSchema.findField("byte_col").isNullable should be(false)
    arrowSchema.findField("string_col").isNullable should be(true)
  }

  it should "handle nullable fields correctly" in {
    val sparkSchema = StructType(Seq(
      StructField("required_field", IntegerType, nullable = false),
      StructField("optional_field", StringType, nullable = true)
    ))

    val arrowSchema = VineArrowBridge.sparkSchemaToArrowSchema(sparkSchema)

    arrowSchema.findField("required_field").isNullable should be(false)
    arrowSchema.findField("optional_field").isNullable should be(true)
  }

  "VineArrowBridge.rowsToArrowIpc" should "convert simple integer rows" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", IntegerType, nullable = false)
    ))

    val rows = Seq(
      Row(1, 100),
      Row(2, 200),
      Row(3, 300)
    )

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)

    arrowBytes should not be null
    arrowBytes.length should be > 0
  }

  it should "handle null values correctly" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))

    val rows = Seq(
      Row(1, "Alice"),
      Row(2, null),
      Row(3, "Charlie")
    )

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)
    val readRows = VineArrowBridge.arrowIpcToRows(arrowBytes, schema)

    readRows.length should be(3)
    readRows(0).getString(1) should be("Alice")
    readRows(1).isNullAt(1) should be(true)
    readRows(2).getString(1) should be("Charlie")
  }

  it should "handle all primitive types" in {
    val schema = StructType(Seq(
      StructField("byte_col", ByteType, nullable = false),
      StructField("short_col", ShortType, nullable = false),
      StructField("int_col", IntegerType, nullable = false),
      StructField("long_col", LongType, nullable = false),
      StructField("float_col", FloatType, nullable = false),
      StructField("double_col", DoubleType, nullable = false),
      StructField("bool_col", BooleanType, nullable = false)
    ))

    val rows = Seq(
      Row(1.toByte, 10.toShort, 100, 1000L, 1.5f, 2.5, true),
      Row(2.toByte, 20.toShort, 200, 2000L, 2.5f, 3.5, false)
    )

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)
    val readRows = VineArrowBridge.arrowIpcToRows(arrowBytes, schema)

    readRows.length should be(2)
    readRows(0).getByte(0) should be(1.toByte)
    readRows(0).getShort(1) should be(10.toShort)
    readRows(0).getInt(2) should be(100)
    readRows(0).getLong(3) should be(1000L)
    readRows(0).getFloat(4) should be(1.5f +- 0.01f)
    readRows(0).getDouble(5) should be(2.5 +- 0.01)
    readRows(0).getBoolean(6) should be(true)
  }

  it should "handle string and binary types" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("data", BinaryType, nullable = true)
    ))

    val binaryData = Array[Byte](1, 2, 3, 4, 5)
    val rows = Seq(
      Row(1, "Alice", binaryData),
      Row(2, "Bob", null)
    )

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)
    val readRows = VineArrowBridge.arrowIpcToRows(arrowBytes, schema)

    readRows.length should be(2)
    readRows(0).getString(1) should be("Alice")
    readRows(0).getAs[Array[Byte]](2) should be(binaryData)
    readRows(1).isNullAt(2) should be(true)
  }

  it should "handle empty row sequence" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    val rows = Seq.empty[Row]
    val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)

    arrowBytes should not be null
    arrowBytes.length should be > 0
  }

  it should "handle UTF-8 strings correctly" in {
    val schema = StructType(Seq(
      StructField("text", StringType, nullable = true)
    ))

    val rows = Seq(
      Row("Hello 世界"),
      Row("Привет мир"),
      Row("مرحبا بالعالم")
    )

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)
    val readRows = VineArrowBridge.arrowIpcToRows(arrowBytes, schema)

    readRows.length should be(3)
    readRows(0).getString(0) should be("Hello 世界")
    readRows(1).getString(0) should be("Привет мир")
    readRows(2).getString(0) should be("مرحبا بالعالم")
  }

  "VineArrowBridge.internalRowsToArrowIpc" should "convert InternalRow correctly" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))

    val internalRows = Seq(
      InternalRow(1, UTF8String.fromString("Alice")),
      InternalRow(2, UTF8String.fromString("Bob"))
    )

    val arrowBytes = VineArrowBridge.internalRowsToArrowIpc(internalRows, schema)

    arrowBytes should not be null
    arrowBytes.length should be > 0
  }

  it should "handle null values in InternalRow" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))

    val internalRows = Seq(
      InternalRow(1, UTF8String.fromString("Alice")),
      InternalRow.apply(2, null)
    )

    val arrowBytes = VineArrowBridge.internalRowsToArrowIpc(internalRows, schema)

    arrowBytes should not be null
    arrowBytes.length should be > 0
  }

  "VineArrowBridge.arrowIpcToRows" should "handle empty bytes" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    val rows = VineArrowBridge.arrowIpcToRows(Array.empty[Byte], schema)

    rows should be(Seq.empty)
  }

  it should "handle null input" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    val rows = VineArrowBridge.arrowIpcToRows(null, schema)

    rows should be(Seq.empty)
  }

  "VineArrowBridge roundtrip" should "preserve all data types" in {
    val schema = StructType(Seq(
      StructField("byte_col", ByteType, nullable = false),
      StructField("short_col", ShortType, nullable = false),
      StructField("int_col", IntegerType, nullable = false),
      StructField("long_col", LongType, nullable = false),
      StructField("float_col", FloatType, nullable = false),
      StructField("double_col", DoubleType, nullable = false),
      StructField("bool_col", BooleanType, nullable = false),
      StructField("string_col", StringType, nullable = true)
    ))

    val originalRows = Seq(
      Row(1.toByte, 10.toShort, 100, 1000L, 1.5f, 2.5, true, "Alice"),
      Row(2.toByte, 20.toShort, 200, 2000L, 2.5f, 3.5, false, "Bob"),
      Row(3.toByte, 30.toShort, 300, 3000L, 3.5f, 4.5, true, null)
    )

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(originalRows, schema)
    val readRows = VineArrowBridge.arrowIpcToRows(arrowBytes, schema)

    readRows.length should be(originalRows.length)

    for (i <- originalRows.indices) {
      readRows(i).getByte(0) should be(originalRows(i).getByte(0))
      readRows(i).getShort(1) should be(originalRows(i).getShort(1))
      readRows(i).getInt(2) should be(originalRows(i).getInt(2))
      readRows(i).getLong(3) should be(originalRows(i).getLong(3))
      readRows(i).getFloat(4) should be(originalRows(i).getFloat(4) +- 0.01f)
      readRows(i).getDouble(5) should be(originalRows(i).getDouble(5) +- 0.01)
      readRows(i).getBoolean(6) should be(originalRows(i).getBoolean(6))

      if (originalRows(i).isNullAt(7)) {
        readRows(i).isNullAt(7) should be(true)
      } else {
        readRows(i).getString(7) should be(originalRows(i).getString(7))
      }
    }
  }

  it should "preserve large datasets" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", DoubleType, nullable = false)
    ))

    val originalRows = (1 to 10000).map { i =>
      Row(i, i * 1.5)
    }

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(originalRows, schema)
    val readRows = VineArrowBridge.arrowIpcToRows(arrowBytes, schema)

    readRows.length should be(10000)
    readRows.head.getInt(0) should be(1)
    readRows.last.getInt(0) should be(10000)
    readRows(4999).getDouble(1) should be(5000 * 1.5 +- 0.01)
  }

  it should "handle binary data correctly" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("data", BinaryType, nullable = true)
    ))

    val binaryData1 = Array[Byte](1, 2, 3, 4, 5)
    val binaryData2 = Array.fill[Byte](1000)(42)

    val originalRows = Seq(
      Row(1, binaryData1),
      Row(2, binaryData2),
      Row(3, null)
    )

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(originalRows, schema)
    val readRows = VineArrowBridge.arrowIpcToRows(arrowBytes, schema)

    readRows.length should be(3)
    readRows(0).getAs[Array[Byte]](1) should be(binaryData1)
    readRows(1).getAs[Array[Byte]](1) should be(binaryData2)
    readRows(2).isNullAt(1) should be(true)
  }

  "VineArrowBridge edge cases" should "handle single row" in {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    val rows = Seq(Row(42))

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)
    val readRows = VineArrowBridge.arrowIpcToRows(arrowBytes, schema)

    readRows.length should be(1)
    readRows.head.getInt(0) should be(42)
  }

  it should "handle wide schema (many columns)" in {
    val fields = (1 to 100).map { i =>
      StructField(s"col_$i", IntegerType, nullable = true)
    }
    val schema = StructType(fields)

    val values = (1 to 100).map(_.asInstanceOf[Any])
    val rows = Seq(Row.fromSeq(values))

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)
    val readRows = VineArrowBridge.arrowIpcToRows(arrowBytes, schema)

    readRows.length should be(1)
    readRows.head.getInt(0) should be(1)
    readRows.head.getInt(99) should be(100)
  }

  it should "handle all null row" in {
    val schema = StructType(Seq(
      StructField("col1", StringType, nullable = true),
      StructField("col2", IntegerType, nullable = true),
      StructField("col3", DoubleType, nullable = true)
    ))

    val rows = Seq(Row(null, null, null))

    val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)
    val readRows = VineArrowBridge.arrowIpcToRows(arrowBytes, schema)

    readRows.length should be(1)
    readRows.head.isNullAt(0) should be(true)
    readRows.head.isNullAt(1) should be(true)
    readRows.head.isNullAt(2) should be(true)
  }
}

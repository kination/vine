package io.kination.vine

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.{Files, Paths}

/**
 * Unit tests for VineModule (JNI interface).
 *
 * Tests native library loading and Arrow IPC JNI functions.
 */
class VineModuleSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private def deleteRecursively(file: File): Unit = {
    if (file.exists()) {
      if (file.isDirectory) {
        file.listFiles().foreach(deleteRecursively)
      }
      file.delete()
    }
  }

  private def createMetadata(outputPath: String, tableName: String, fields: Seq[(String, String, Boolean)]): Unit = {
    val fieldsJson = fields.zipWithIndex.map { case ((name, dataType, isRequired), idx) =>
      s"""{
         |      "id": ${idx + 1},
         |      "name": "$name",
         |      "data_type": "$dataType",
         |      "is_required": $isRequired
         |    }""".stripMargin
    }.mkString(",\n")

    val metadata =
      s"""{
         |  "table_name": "$tableName",
         |  "fields": [
         |$fieldsJson
         |  ]
         |}""".stripMargin

    Files.write(Paths.get(outputPath, "vine_meta.json"), metadata.getBytes)
  }

  "VineModule" should "load native library" in {
    // Check whether native library loaded well
    // (VineModule static initializer loads the library)
    noException should be thrownBy {
      classOf[VineModule].getName
    }
  }

  "VineModule.batchWriteArrow" should "write simple Arrow IPC data" in {
    val outputPath = Files.createTempDirectory("vine-jni-test-write-").toString

    try {
      createMetadata(outputPath, "test_table",
        Seq(("id", "integer", true), ("value", "integer", true)))

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

      // Call JNI function
      VineModule.batchWriteArrow(outputPath, arrowBytes)

      // Verify files created
      val dateDirs = new File(outputPath).listFiles().filter(_.isDirectory)
      dateDirs should not be empty

    } finally {
      deleteRecursively(new File(outputPath))
    }
  }

  it should "handle large batches" in {
    val outputPath = Files.createTempDirectory("vine-jni-test-large-").toString

    try {
      createMetadata(outputPath, "test_table",
        Seq(("id", "integer", true), ("value", "double", true)))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("value", DoubleType, nullable = false)
      ))

      val rows = (1 to 1000).map(i => Row(i, i * 1.5))

      val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)

      VineModule.batchWriteArrow(outputPath, arrowBytes)

      // Verify files created
      val dateDirs = new File(outputPath).listFiles().filter(_.isDirectory)
      dateDirs should not be empty

    } finally {
      deleteRecursively(new File(outputPath))
    }
  }

  it should "handle null values" in {
    val outputPath = Files.createTempDirectory("vine-jni-test-nulls-").toString

    try {
      createMetadata(outputPath, "test_table",
        Seq(("id", "integer", true), ("name", "string", false)))

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

      VineModule.batchWriteArrow(outputPath, arrowBytes)

      // Verify files created
      val dateDirs = new File(outputPath).listFiles().filter(_.isDirectory)
      dateDirs should not be empty

    } finally {
      deleteRecursively(new File(outputPath))
    }
  }

  "VineModule.readDataArrow" should "read back written data" in {
    val outputPath = Files.createTempDirectory("vine-jni-test-read-").toString

    try {
      createMetadata(outputPath, "test_table",
        Seq(("id", "integer", true), ("name", "string", true)))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false)
      ))

      val originalRows = Seq(
        Row(1, "Alice"),
        Row(2, "Bob"),
        Row(3, "Charlie")
      )

      // Write data
      val writeBytes = VineArrowBridge.rowsToArrowIpc(originalRows, schema)
      VineModule.batchWriteArrow(outputPath, writeBytes)

      // Read data back
      val readBytes = VineModule.readDataArrow(outputPath)
      val readRows = VineArrowBridge.arrowIpcToRows(readBytes, schema)

      readRows.length should be(3)
      readRows(0).getInt(0) should be(1)
      readRows(0).getString(1) should be("Alice")

    } finally {
      deleteRecursively(new File(outputPath))
    }
  }

  "VineModule streaming writer" should "create and use streaming writer" in {
    val outputPath = Files.createTempDirectory("vine-jni-test-stream-").toString

    try {
      createMetadata(outputPath, "test_table",
        Seq(("id", "integer", true), ("value", "integer", true)))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("value", IntegerType, nullable = false)
      ))

      // Create streaming writer
      val writerId = VineModule.createStreamingWriter(outputPath)

      writerId should be >= 0L

      // Write batches
      val batch1 = VineArrowBridge.rowsToArrowIpc(Seq(Row(1, 100)), schema)
      VineModule.streamingAppendBatchArrow(writerId, batch1)

      val batch2 = VineArrowBridge.rowsToArrowIpc(Seq(Row(2, 200)), schema)
      VineModule.streamingAppendBatchArrow(writerId, batch2)

      // Flush and close
      VineModule.streamingFlush(writerId)
      VineModule.streamingClose(writerId)

      // Verify files created
      val dateDirs = new File(outputPath).listFiles().filter(_.isDirectory)
      dateDirs should not be empty

    } finally {
      deleteRecursively(new File(outputPath))
    }
  }

  it should "handle multiple streaming writers" in {
    val outputPath1 = Files.createTempDirectory("vine-jni-test-stream1-").toString
    val outputPath2 = Files.createTempDirectory("vine-jni-test-stream2-").toString

    try {
      createMetadata(outputPath1, "test_table1",
        Seq(("id", "integer", true)))
      createMetadata(outputPath2, "test_table2",
        Seq(("id", "integer", true)))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false)
      ))

      // Create two writers
      val writer1 = VineModule.createStreamingWriter(outputPath1)
      val writer2 = VineModule.createStreamingWriter(outputPath2)

      writer1 should not be writer2

      // Write to both
      val batch1 = VineArrowBridge.rowsToArrowIpc(Seq(Row(1)), schema)
      VineModule.streamingAppendBatchArrow(writer1, batch1)

      val batch2 = VineArrowBridge.rowsToArrowIpc(Seq(Row(2)), schema)
      VineModule.streamingAppendBatchArrow(writer2, batch2)

      // Close both
      VineModule.streamingClose(writer1)
      VineModule.streamingClose(writer2)

      // Verify both created files
      new File(outputPath1).listFiles().filter(_.isDirectory) should not be empty
      new File(outputPath2).listFiles().filter(_.isDirectory) should not be empty

    } finally {
      deleteRecursively(new File(outputPath1))
      deleteRecursively(new File(outputPath2))
    }
  }


  // TODO: Re-enable this test after implementing proper error handling in Rust
  // Currently, Rust code uses expect() which causes panic instead of returning JNI exception
  // See: vine-core/src/lib.rs:192-193
  "VineModule error handling" should "handle invalid path gracefully" ignore {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false)
    ))

    val rows = Seq(Row(1))
    val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)

    // Write to invalid path should throw exception (not panic)
    // TODO: Implement proper error handling in JNI layer
    an[Exception] should be thrownBy {
      VineModule.batchWriteArrow("/invalid/path/that/does/not/exist", arrowBytes)
    }
  }

  it should "handle empty Arrow bytes" in {
    val outputPath = Files.createTempDirectory("vine-jni-test-empty-").toString

    try {
      createMetadata(outputPath, "test_table",
        Seq(("id", "integer", true)))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false)
      ))

      val emptyRows = Seq.empty[Row]
      val arrowBytes = VineArrowBridge.rowsToArrowIpc(emptyRows, schema)

      // Should not throw exception
      noException should be thrownBy {
        VineModule.batchWriteArrow(outputPath, arrowBytes)
      }

    } finally {
      deleteRecursively(new File(outputPath))
    }
  }

  "VineModule data types" should "handle all supported types via JNI" in {
    val outputPath = Files.createTempDirectory("vine-jni-test-types-").toString

    try {
      createMetadata(outputPath, "test_table", Seq(
        ("byte_col", "byte", true),
        ("short_col", "short", true),
        ("int_col", "integer", true),
        ("long_col", "long", true),
        ("float_col", "float", true),
        ("double_col", "double", true),
        ("bool_col", "boolean", true),
        ("string_col", "string", true)
      ))

      val schema = StructType(Seq(
        StructField("byte_col", ByteType, nullable = false),
        StructField("short_col", ShortType, nullable = false),
        StructField("int_col", IntegerType, nullable = false),
        StructField("long_col", LongType, nullable = false),
        StructField("float_col", FloatType, nullable = false),
        StructField("double_col", DoubleType, nullable = false),
        StructField("bool_col", BooleanType, nullable = false),
        StructField("string_col", StringType, nullable = false)
      ))

      val rows = Seq(
        Row(1.toByte, 10.toShort, 100, 1000L, 1.5f, 2.5, true, "test")
      )

      val arrowBytes = VineArrowBridge.rowsToArrowIpc(rows, schema)
      VineModule.batchWriteArrow(outputPath, arrowBytes)

      val readBytes = VineModule.readDataArrow(outputPath)
      val readRows = VineArrowBridge.arrowIpcToRows(readBytes, schema)

      readRows.length should be(1)
      readRows(0).getByte(0) should be(1.toByte)
      readRows(0).getShort(1) should be(10.toShort)
      readRows(0).getInt(2) should be(100)
      readRows(0).getLong(3) should be(1000L)
      readRows(0).getFloat(4) should be(1.5f +- 0.01f)
      readRows(0).getDouble(5) should be(2.5 +- 0.01)
      readRows(0).getBoolean(6) should be(true)
      readRows(0).getString(7) should be("test")

    } finally {
      deleteRecursively(new File(outputPath))
    }
  }
}

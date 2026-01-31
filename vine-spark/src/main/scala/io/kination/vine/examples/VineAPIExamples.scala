package io.kination.vine.examples

import io.kination.vine._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

/**
 * Examples demonstrating how to use the Vine Spark API with Vortex format.
 *
 * These examples show both batch and streaming patterns for reading and writing Vine tables.
 *
 * Vortex format features:
 * - Date-based partitioning (YYYY-MM-DD directories)
 * - Efficient columnar storage
 * - Schema embedded in file footer
 */
object VineAPIExamples {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Vine API Examples")
      .master("local[*]")
      .getOrCreate()

    // Example 1: Batch Write
    batchWriteExample(spark)

    // Example 2: Streaming Write
    streamingWriteExample(spark)

    // Example 3: Read
    readExample(spark)

    // Example 4: Structured Streaming Integration
    structuredStreamingExample(spark)

    spark.stop()
  }

  /**
   * Example 1: Batch Write
   *
   * Write entire DataFrame in one shot.
   * Good for bulk data ingestion, ETL jobs, backfills.
   */
  def batchWriteExample(spark: SparkSession): Unit = {
    println("\n=== Example 1: Batch Write ===")

    import spark.implicits._

    // Create sample data
    val data = Seq(
      (1, "Alice", 25.5),
      (2, "Bob", 30.0),
      (3, "Charlie", 35.5)
    )
    val df = data.toDF("id", "name", "score")

    // Write to Vine table (Vortex format)
    // - Creates vine_meta.json if not exists
    // - Creates date-partitioned .vtx files
    VineBatchWriter.write("vine-data/users", df)

    println("Batch write completed")
  }

  /**
   * Example 2: Streaming Write
   *
   * Write data incrementally in batches.
   * Good for streaming pipelines, micro-batching, continuous ingestion.
   */
  def streamingWriteExample(spark: SparkSession): Unit = {
    println("\n=== Example 2: Streaming Write ===")

    import spark.implicits._

    // Create streaming writer
    val writer = VineStreamingWriter("vine-data/events")

    try {
      // Simulate continuous data stream
      for (i <- 1 to 5) {
        // Get next batch
        val batch = Seq(
          (i * 100 + 1, s"Event ${i}.1", System.currentTimeMillis()),
          (i * 100 + 2, s"Event ${i}.2", System.currentTimeMillis()),
          (i * 100 + 3, s"Event ${i}.3", System.currentTimeMillis())
        )
        val batchDF = batch.toDF("id", "name", "timestamp")

        // Append batch to stream
        writer.appendBatch(batchDF)
        println(s"Written batch $i")

        // Flush every 2 batches to control file sizes
        if (i % 2 == 0) {
          writer.flush()
          println(s"Flushed at batch $i")
        }

        Thread.sleep(100) // Simulate streaming delay
      }
    } finally {
      // Always close the writer when done
      writer.close()
      println("Streaming writer closed")
    }
  }

  /**
   * Example 3: Read
   *
   * Read Vine table as DataFrame.
   * Schema is automatically read from vine_meta.json.
   */
  def readExample(spark: SparkSession): Unit = {
    println("\n=== Example 3: Read ===")

    // Read table (schema from vine_meta.json)
    val df = VineReader.read(spark, "vine-data/users")

    println("Data read:")
    df.show()

    // Read with explicit schema
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false)
    ))
    val dfWithSchema = VineReader.read(spark, "vine-data/users", schema)
    println("Data with schema:")
    dfWithSchema.show()

    // Read raw Arrow IPC bytes (for debugging)
    val rawData = VineReader.readRaw("vine-data/users")
    println(s"Raw Arrow IPC data (${rawData.length} bytes)")
  }

  /**
   * Example 4: Structured Streaming Integration
   *
   * Use VineStreamingWriter with Spark Structured Streaming.
   */
  def structuredStreamingExample(spark: SparkSession): Unit = {
    println("\n=== Example 4: Structured Streaming Integration ===")

    // Create a streaming source (rate source for demo)
    val streamingDF = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "10")
      .load()
      .selectExpr("value as id", "CAST(timestamp AS LONG) as timestamp")

    // Create Vine streaming writer
    val vineWriter = VineStreamingWriter("vine-data/streaming-events")

    try {
      // Write stream to Vine using foreachBatch
      val query = streamingDF
        .writeStream
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          println(s"Processing batch $batchId...")

          // Append batch to Vine
          vineWriter.appendBatch(batchDF)

          // Flush every 10 batches
          if (batchId % 10 == 0) {
            vineWriter.flush()
            println(s"Flushed at batch $batchId")
          }
        }
        .start()

      // Run for a short time (demo)
      query.awaitTermination(5000)
      query.stop()

      println("Structured streaming completed")
    } finally {
      vineWriter.close()
    }
  }

  /**
   * Example 5: All Supported Types
   *
   * Demonstrate writing and reading all Vine/Vortex supported types.
   */
  def allTypesExample(spark: SparkSession): Unit = {
    println("\n=== Example 5: All Supported Types ===")

    import spark.implicits._

    // Create data with all supported types
    val schema = StructType(Seq(
      StructField("byte_col", ByteType, nullable = false),
      StructField("short_col", ShortType, nullable = false),
      StructField("int_col", IntegerType, nullable = false),
      StructField("long_col", LongType, nullable = false),
      StructField("float_col", FloatType, nullable = false),
      StructField("double_col", DoubleType, nullable = false),
      StructField("bool_col", BooleanType, nullable = false),
      StructField("string_col", StringType, nullable = true),
      StructField("date_col", DateType, nullable = true),
      StructField("timestamp_col", TimestampType, nullable = true)
    ))

    val data = Seq(
      org.apache.spark.sql.Row(
        1.toByte, 100.toShort, 1000, 10000L, 1.5f, 2.5,
        true, "test", java.sql.Date.valueOf("2024-01-15"),
        java.sql.Timestamp.valueOf("2024-01-15 10:30:00")
      )
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    // Write
    VineBatchWriter.write("vine-data/all-types", df)
    println("Written all types")

    // Read back
    val readDf = VineReader.read(spark, "vine-data/all-types")
    println("Read all types:")
    readDf.show()
    readDf.printSchema()
  }

  /**
   * Example 6: Error Handling
   *
   * Demonstrate proper error handling with streaming writers.
   */
  def errorHandlingExample(spark: SparkSession): Unit = {
    println("\n=== Example 6: Error Handling ===")

    import spark.implicits._

    val writer = VineStreamingWriter("vine-data/safe-events")

    try {
      val batch1 = Seq((1, "Event 1")).toDF("id", "name")
      writer.appendBatch(batch1)
      println("Batch 1 written")

      // Simulate error scenario
      try {
        val batch2 = Seq((2, "Event 2")).toDF("id", "name")
        writer.appendBatch(batch2)
        println("Batch 2 written")
      } catch {
        case e: Exception =>
          println(s"Error in batch 2: ${e.getMessage}")
          // Optionally flush to ensure previous data is committed
          writer.flush()
      }

    } finally {
      // Always close in finally block
      writer.close()
      println("Writer closed safely")
    }
  }
}

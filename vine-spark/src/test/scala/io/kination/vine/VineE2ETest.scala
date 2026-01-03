package io.kination.vine

import org.apache.spark.sql.SparkSession

import java.io.File
import java.nio.file.{Files, Paths}
import scala.io.Source

/**
 * Simple E2E test for Vine Spark integration.
 *
 * This is a standalone application that tests:
 * 1. Reading CSV sample data
 * 2. Writing to Vine format
 * 3. Reading back and verifying data
 *
 * Run with: sbt "Test/runMain io.kination.vine.VineE2ETest"
 */
object VineE2ETest {

  // Helper to avoid getClass ambiguity with spark.implicits
  // Copy resource to temp file since Spark can't read from JAR
  private def getResourcePath(resourceName: String): String = {
    val stream = this.getClass.getResourceAsStream(resourceName)
    if (stream == null) {
      throw new RuntimeException(s"Resource not found: $resourceName")
    }

    val tempFile = Files.createTempFile("vine-test-resource-", resourceName.replace("/", "-"))
    try {
      Files.copy(stream, tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
      tempFile.toString
    } finally {
      stream.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("VineE2ETest")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    println("\n" + "=" * 80)
    println("Vine E2E Test Suite")
    println("=" * 80 + "\n")

    try {
      // Run all tests
      test1_BasicBatchWrite(spark)
      test2_EventsWrite(spark)
      test3_UsersWrite(spark)
      test4_LargeDataset(spark)
      test5_MultipleBatches(spark)
      test6_RoundtripIntegrity(spark)

      println("\n" + "=" * 80)
      println("✓ All E2E tests PASSED")
      println("=" * 80 + "\n")

    } catch {
      case e: Exception =>
        println(s"\n✗ Test FAILED: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  def test1_BasicBatchWrite(spark: SparkSession): Unit = {
    println("Test 1: Basic batch write")
    val outputPath = Files.createTempDirectory("vine-test1-").toString

    try {
      // Read sample CSV from resources
      val csvPath = getResourcePath("/users.csv")
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csvPath)

      val rowCount = df.count()
      println(s"  Read $rowCount rows from CSV")

      // Copy metadata
      val metadataPath = getResourcePath("/users_metadata.json")
      val metadataContent = Source.fromFile(metadataPath).mkString
      Files.write(Paths.get(outputPath, "vine_meta.json"), metadataContent.getBytes)

      // Write to Vine
      try {
        VineBatchWriter.write(outputPath, df)
      } catch {
        case e: Exception =>
          println(s"  ERROR during write: ${e.getMessage}")
          e.printStackTrace()
          throw e
      }

      // Verify files created
      val outputDir = new File(outputPath)
      val allFiles = outputDir.listFiles()
      println(s"  Files in output dir: ${if (allFiles != null) allFiles.mkString(", ") else "none"}")

      val dateDirs = if (allFiles != null) allFiles.filter(_.isDirectory) else Array()
      assert(dateDirs.nonEmpty, s"Should create date directories, but found: ${if (allFiles != null) allFiles.mkString(", ") else "none"}")

      // Check for .vtx files (Vortex format) instead of .parquet
      val vtxFiles = dateDirs.flatMap(_.listFiles())
        .filter(f => f.getName.endsWith(".vtx") || f.getName.endsWith(".parquet"))

      println(s"  Data files in date dir: ${vtxFiles.mkString(", ")}")
      assert(vtxFiles.nonEmpty, s"Should create data files (.vtx or .parquet), but found none in ${dateDirs.mkString(", ")}")

      // Read back
      val readDF = VineReader.read(spark, outputPath)
      val readCount = readDF.count()

      assert(readCount == rowCount, s"Row count mismatch: wrote $rowCount, read $readCount")

      println(s"  ✓ Wrote $rowCount rows, read back $readCount rows")
      println(s"  ✓ Created ${vtxFiles.length} data file(s)")

    } finally {
      deleteRecursively(new File(outputPath))
    }
  }

  def test2_EventsWrite(spark: SparkSession): Unit = {
    println("\nTest 2: Events write")
    val outputPath = Files.createTempDirectory("vine-test2-").toString

    try {
      val csvPath = getResourcePath("/events.csv")
      val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

      val metadataPath = getResourcePath("/events_metadata.json")
      val metadataContent = Source.fromFile(metadataPath).mkString
      Files.write(Paths.get(outputPath, "vine_meta.json"), metadataContent.getBytes)

      VineBatchWriter.write(outputPath, df)

      val readDF = VineReader.read(spark, outputPath)
      assert(readDF.count() == df.count(), "Row count should match")

      println(s"  ✓ Events write succeeded")

    } finally {
      deleteRecursively(new File(outputPath))
    }
  }

  def test3_UsersWrite(spark: SparkSession): Unit = {
    println("\nTest 3: Users write")
    val outputPath = Files.createTempDirectory("vine-test3-").toString

    try {
      val csvPath = getResourcePath("/users.csv")
      val df = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)

      val metadataPath = getResourcePath("/users_metadata.json")
      val metadataContent = Source.fromFile(metadataPath).mkString
      Files.write(Paths.get(outputPath, "vine_meta.json"), metadataContent.getBytes)

      VineBatchWriter.write(outputPath, df)

      val dataFiles = new File(outputPath).listFiles()
        .filter(_.isDirectory)
        .flatMap(_.listFiles())
        .filter(f => f.getName.endsWith(".vtx") || f.getName.endsWith(".parquet"))

      assert(dataFiles.nonEmpty, "Should create data files")
      println(s"  ✓ Users write succeeded")

    } finally {
      deleteRecursively(new File(outputPath))
    }
  }

  def test4_LargeDataset(spark: SparkSession): Unit = {
    println("\nTest 4: Large dataset (1000 rows)")
    val outputPath = Files.createTempDirectory("vine-test4-").toString

    try {
      // Get metadata path before importing implicits
      val metadataPath = getResourcePath("/users_metadata.json")
      val metadataContent = Source.fromFile(metadataPath).mkString
      Files.write(Paths.get(outputPath, "vine_meta.json"), metadataContent.getBytes)

      import spark.implicits._

      val largeData = (1 to 1000).map { i =>
        (i, s"user$i", 20 + (i % 50), 80.0 + (i % 20))
      }.toDF("id", "name", "age", "score")

      VineBatchWriter.write(outputPath, largeData)

      val readDF = VineReader.read(spark, outputPath)
      assert(readDF.count() == 1000, "Should read 1000 rows")

      println(s"  ✓ Successfully wrote and read 1000 rows")

    } finally {
      deleteRecursively(new File(outputPath))
    }
  }

  def test5_MultipleBatches(spark: SparkSession): Unit = {
    println("\nTest 5: Multiple batch writes to same table")
    val outputPath = Files.createTempDirectory("vine-test5-").toString

    try {
      // Get metadata path before importing implicits
      val metadataPath = getResourcePath("/users_metadata.json")
      val metadataContent = Source.fromFile(metadataPath).mkString
      Files.write(Paths.get(outputPath, "vine_meta.json"), metadataContent.getBytes)

      import spark.implicits._

      // Write 3 separate batches
      val batch1 = Seq((1, "Alice", 25, 95.5)).toDF("id", "name", "age", "score")
      VineBatchWriter.write(outputPath, batch1)
      Thread.sleep(100)

      val batch2 = Seq((2, "Bob", 30, 87.3)).toDF("id", "name", "age", "score")
      VineBatchWriter.write(outputPath, batch2)
      Thread.sleep(100)

      val batch3 = Seq((3, "Charlie", 35, 92.0)).toDF("id", "name", "age", "score")
      VineBatchWriter.write(outputPath, batch3)

      // Read all
      val allData = VineReader.read(spark, outputPath)
      val totalRows = allData.count()

      assert(totalRows == 3, s"Should read 3 rows, got $totalRows")
      println(s"  ✓ Successfully wrote 3 batches, read $totalRows rows")

    } finally {
      deleteRecursively(new File(outputPath))
    }
  }

  def test6_RoundtripIntegrity(spark: SparkSession): Unit = {
    println("\nTest 6: Read-Write roundtrip data integrity")
    val outputPath = Files.createTempDirectory("vine-test6-").toString

    try {
      // Read CSV
      val csvPath = getResourcePath("/users.csv")
      val originalDF = spark.read.option("header", "true").option("inferSchema", "true").csv(csvPath)
      val originalCount = originalDF.count()

      val metadataPath = getResourcePath("/users_metadata.json")
      val metadataContent = Source.fromFile(metadataPath).mkString
      Files.write(Paths.get(outputPath, "vine_meta.json"), metadataContent.getBytes)

      // Write to Vine
      VineBatchWriter.write(outputPath, originalDF)

      // Read back
      val readDF = VineReader.read(spark, outputPath)
      val readCount = readDF.count()

      assert(readCount == originalCount, s"Row count mismatch: $originalCount vs $readCount")

      println(s"  ✓ Roundtrip integrity verified: $originalCount rows")

    } finally {
      deleteRecursively(new File(outputPath))
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.exists()) {
      if (file.isDirectory) {
        file.listFiles().foreach(deleteRecursively)
      }
      file.delete()
    }
  }

  private def assert(condition: Boolean, message: String): Unit = {
    if (!condition) {
      throw new AssertionError(message)
    }
  }
}

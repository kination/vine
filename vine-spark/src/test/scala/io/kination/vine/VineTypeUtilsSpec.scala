package io.kination.vine

import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for VineTypeUtils.
 *
 * Tests type conversion between Spark DataType and Vine type strings.
 */
class VineTypeUtilsSpec extends AnyFlatSpec with Matchers {

  "VineTypeUtils.sparkTypeToVineType" should "convert integer types correctly" in {
    VineTypeUtils.sparkTypeToVineType(ByteType) should be("byte")
    VineTypeUtils.sparkTypeToVineType(ShortType) should be("short")
    VineTypeUtils.sparkTypeToVineType(IntegerType) should be("integer")
    VineTypeUtils.sparkTypeToVineType(LongType) should be("long")
  }

  it should "convert floating point types correctly" in {
    VineTypeUtils.sparkTypeToVineType(FloatType) should be("float")
    VineTypeUtils.sparkTypeToVineType(DoubleType) should be("double")
  }

  it should "convert boolean type correctly" in {
    VineTypeUtils.sparkTypeToVineType(BooleanType) should be("boolean")
  }

  it should "convert string and binary types correctly" in {
    VineTypeUtils.sparkTypeToVineType(StringType) should be("string")
    VineTypeUtils.sparkTypeToVineType(BinaryType) should be("binary")
  }

  it should "convert date and timestamp types correctly" in {
    VineTypeUtils.sparkTypeToVineType(DateType) should be("date")
    VineTypeUtils.sparkTypeToVineType(TimestampType) should be("timestamp")
  }

  it should "convert decimal type correctly" in {
    VineTypeUtils.sparkTypeToVineType(DecimalType(10, 2)) should be("decimal")
    VineTypeUtils.sparkTypeToVineType(DecimalType(38, 18)) should be("decimal")
  }

  it should "fallback to string for unsupported types" in {
    VineTypeUtils.sparkTypeToVineType(ArrayType(IntegerType)) should be("string")
    VineTypeUtils.sparkTypeToVineType(MapType(StringType, IntegerType)) should be("string")
    VineTypeUtils.sparkTypeToVineType(StructType(Seq(StructField("x", IntegerType)))) should be("string")
  }

  "VineTypeUtils.vineTypeToSparkType" should "convert integer types correctly" in {
    VineTypeUtils.vineTypeToSparkType("byte") should be(ByteType)
    VineTypeUtils.vineTypeToSparkType("tinyint") should be(ByteType)
    VineTypeUtils.vineTypeToSparkType("short") should be(ShortType)
    VineTypeUtils.vineTypeToSparkType("smallint") should be(ShortType)
    VineTypeUtils.vineTypeToSparkType("integer") should be(IntegerType)
    VineTypeUtils.vineTypeToSparkType("int") should be(IntegerType)
    VineTypeUtils.vineTypeToSparkType("long") should be(LongType)
    VineTypeUtils.vineTypeToSparkType("bigint") should be(LongType)
  }

  it should "convert floating point types correctly" in {
    VineTypeUtils.vineTypeToSparkType("float") should be(FloatType)
    VineTypeUtils.vineTypeToSparkType("double") should be(DoubleType)
  }

  it should "convert boolean type correctly" in {
    VineTypeUtils.vineTypeToSparkType("boolean") should be(BooleanType)
    VineTypeUtils.vineTypeToSparkType("bool") should be(BooleanType)
  }

  it should "convert string and binary types correctly" in {
    VineTypeUtils.vineTypeToSparkType("string") should be(StringType)
    VineTypeUtils.vineTypeToSparkType("binary") should be(BinaryType)
  }

  it should "convert date and timestamp types correctly" in {
    VineTypeUtils.vineTypeToSparkType("date") should be(DateType)
    VineTypeUtils.vineTypeToSparkType("timestamp") should be(TimestampType)
  }

  it should "convert decimal type correctly with default precision" in {
    VineTypeUtils.vineTypeToSparkType("decimal") should be(DecimalType(38, 18))
  }

  it should "be case insensitive" in {
    VineTypeUtils.vineTypeToSparkType("INTEGER") should be(IntegerType)
    VineTypeUtils.vineTypeToSparkType("String") should be(StringType)
    VineTypeUtils.vineTypeToSparkType("BOOLEAN") should be(BooleanType)
    VineTypeUtils.vineTypeToSparkType("TinyInt") should be(ByteType)
  }

  it should "fallback to string for unsupported types" in {
    VineTypeUtils.vineTypeToSparkType("unknown") should be(StringType)
    VineTypeUtils.vineTypeToSparkType("array") should be(StringType)
    VineTypeUtils.vineTypeToSparkType("map") should be(StringType)
  }

  "VineTypeUtils roundtrip" should "preserve all basic types" in {
    val sparkTypes = Seq(
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      BooleanType,
      StringType,
      BinaryType,
      DateType,
      TimestampType
    )

    sparkTypes.foreach { sparkType =>
      val vineType = VineTypeUtils.sparkTypeToVineType(sparkType)
      val backToSpark = VineTypeUtils.vineTypeToSparkType(vineType)
      backToSpark should be(sparkType)
    }
  }

  it should "preserve decimal type (with default precision)" in {
    val sparkType = DecimalType(10, 2)
    val vineType = VineTypeUtils.sparkTypeToVineType(sparkType)
    val backToSpark = VineTypeUtils.vineTypeToSparkType(vineType)

    // Vine doesn't store precision, so it returns default (38, 18)
    backToSpark should be(DecimalType(38, 18))
  }

  "VineTypeUtils SQL aliases" should "work for integer types" in {
    // byte
    VineTypeUtils.vineTypeToSparkType("byte") should be(ByteType)
    VineTypeUtils.vineTypeToSparkType("tinyint") should be(ByteType)

    // short
    VineTypeUtils.vineTypeToSparkType("short") should be(ShortType)
    VineTypeUtils.vineTypeToSparkType("smallint") should be(ShortType)

    // integer
    VineTypeUtils.vineTypeToSparkType("integer") should be(IntegerType)
    VineTypeUtils.vineTypeToSparkType("int") should be(IntegerType)

    // long
    VineTypeUtils.vineTypeToSparkType("long") should be(LongType)
    VineTypeUtils.vineTypeToSparkType("bigint") should be(LongType)
  }

  it should "work for boolean type" in {
    VineTypeUtils.vineTypeToSparkType("boolean") should be(BooleanType)
    VineTypeUtils.vineTypeToSparkType("bool") should be(BooleanType)
  }

  "VineTypeUtils edge cases" should "handle empty string" in {
    VineTypeUtils.vineTypeToSparkType("") should be(StringType)
  }

  it should "handle whitespace in type names" in {
    // toLowerCase doesn't trim whitespace, so this will fallback to StringType
    VineTypeUtils.vineTypeToSparkType("  integer  ") should be(StringType)
    // Without whitespace should work
    VineTypeUtils.vineTypeToSparkType("integer") should be(IntegerType)
  }

  it should "handle mixed case with aliases" in {
    VineTypeUtils.vineTypeToSparkType("TinyInt") should be(ByteType)
    VineTypeUtils.vineTypeToSparkType("SmallInt") should be(ShortType)
    VineTypeUtils.vineTypeToSparkType("BigInt") should be(LongType)
  }

  "VineTypeUtils complex schemas" should "convert full schema correctly" in {
    val sparkSchema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("score", DoubleType, nullable = true),
      StructField("active", BooleanType, nullable = false)
    ))

    val vineTypes = sparkSchema.fields.map(f => VineTypeUtils.sparkTypeToVineType(f.dataType))

    vineTypes should be(Seq("integer", "string", "integer", "double", "boolean"))
  }

  it should "reconstruct schema from vine types" in {
    val vineTypes = Seq("integer", "string", "double", "boolean")
    val sparkTypes = vineTypes.map(VineTypeUtils.vineTypeToSparkType)

    sparkTypes should be(Seq(IntegerType, StringType, DoubleType, BooleanType))
  }

  "VineTypeUtils type coverage" should "support all documented Vine types" in {
    val vineTypes = Seq(
      "byte", "tinyint",
      "short", "smallint",
      "integer", "int",
      "long", "bigint",
      "float",
      "double",
      "boolean", "bool",
      "string",
      "binary",
      "date",
      "timestamp",
      "decimal"
    )

    // All should convert without errors
    vineTypes.foreach { vineType =>
      noException should be thrownBy VineTypeUtils.vineTypeToSparkType(vineType)
    }
  }

  it should "support all Spark primitive types" in {
    val sparkTypes = Seq(
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      BooleanType,
      StringType,
      BinaryType,
      DateType,
      TimestampType,
      DecimalType(10, 2)
    )

    // All should convert without errors
    sparkTypes.foreach { sparkType =>
      noException should be thrownBy VineTypeUtils.sparkTypeToVineType(sparkType)
    }
  }
}

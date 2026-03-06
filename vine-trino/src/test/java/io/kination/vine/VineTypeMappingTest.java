package io.kination.vine;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VineTypeMappingTest {

    @Test
    void testIntegerTypes() {
        assertEquals(TinyintType.TINYINT, VineTypeMapping.toTrinoType("byte"));
        assertEquals(TinyintType.TINYINT, VineTypeMapping.toTrinoType("tinyint"));
        assertEquals(SmallintType.SMALLINT, VineTypeMapping.toTrinoType("short"));
        assertEquals(SmallintType.SMALLINT, VineTypeMapping.toTrinoType("smallint"));
        assertEquals(IntegerType.INTEGER, VineTypeMapping.toTrinoType("integer"));
        assertEquals(IntegerType.INTEGER, VineTypeMapping.toTrinoType("int"));
        assertEquals(BigintType.BIGINT, VineTypeMapping.toTrinoType("long"));
        assertEquals(BigintType.BIGINT, VineTypeMapping.toTrinoType("bigint"));
    }

    @Test
    void testFloatingPointTypes() {
        assertEquals(RealType.REAL, VineTypeMapping.toTrinoType("float"));
        assertEquals(DoubleType.DOUBLE, VineTypeMapping.toTrinoType("double"));
    }

    @Test
    void testBooleanType() {
        assertEquals(BooleanType.BOOLEAN, VineTypeMapping.toTrinoType("boolean"));
        assertEquals(BooleanType.BOOLEAN, VineTypeMapping.toTrinoType("bool"));
    }

    @Test
    void testStringAndBinaryTypes() {
        assertEquals(VarcharType.VARCHAR, VineTypeMapping.toTrinoType("string"));
        assertEquals(VarbinaryType.VARBINARY, VineTypeMapping.toTrinoType("binary"));
    }

    @Test
    void testDateTimeTypes() {
        assertEquals(DateType.DATE, VineTypeMapping.toTrinoType("date"));
        assertEquals(TimestampType.TIMESTAMP_MILLIS, VineTypeMapping.toTrinoType("timestamp"));
    }

    @Test
    void testDecimalFallback() {
        assertEquals(VarcharType.VARCHAR, VineTypeMapping.toTrinoType("decimal"));
    }

    @Test
    void testUnknownTypeFallback() {
        assertEquals(VarcharType.VARCHAR, VineTypeMapping.toTrinoType("unknown_type"));
    }

    @Test
    void testCaseInsensitive() {
        assertEquals(IntegerType.INTEGER, VineTypeMapping.toTrinoType("INTEGER"));
        assertEquals(BooleanType.BOOLEAN, VineTypeMapping.toTrinoType("Boolean"));
        assertEquals(VarcharType.VARCHAR, VineTypeMapping.toTrinoType("STRING"));
    }
}

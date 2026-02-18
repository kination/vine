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
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

/**
 * Maps Vine data types (from vine_meta.json) to Trino types.
 */
public class VineTypeMapping {

    public static Type toTrinoType(String vineType) {
        switch (vineType.toLowerCase()) {
            case "byte":
            case "tinyint":
                return TinyintType.TINYINT;
            case "short":
            case "smallint":
                return SmallintType.SMALLINT;
            case "integer":
            case "int":
                return IntegerType.INTEGER;
            case "long":
            case "bigint":
                return BigintType.BIGINT;
            case "float":
                return RealType.REAL;
            case "double":
                return DoubleType.DOUBLE;
            case "boolean":
            case "bool":
                return BooleanType.BOOLEAN;
            case "string":
                return VarcharType.VARCHAR;
            case "binary":
                return VarbinaryType.VARBINARY;
            case "date":
                return DateType.DATE;
            case "timestamp":
                return TimestampType.TIMESTAMP_MILLIS;
            case "decimal":
                return VarcharType.VARCHAR;
            default:
                return VarcharType.VARCHAR;
        }
    }
}

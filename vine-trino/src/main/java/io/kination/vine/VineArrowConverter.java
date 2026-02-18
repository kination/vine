package io.kination.vine;

import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Converts Arrow IPC bytes (from vine-core JNI) to row-oriented data for Trino's RecordCursor.
 */
public class VineArrowConverter {

    private static final BufferAllocator ALLOCATOR = new RootAllocator();

    /**
     * Decode Arrow IPC bytes into 'row oriented' Object array.
     *
     * @param arrowData Arrow IPC bytes from VineModule.readDataArrow()
     * @param columns   Requested columns with ordinal positions and types
     * @return Object[row][col] with Trino-compatible typed values
     */
    public static Object[][] arrowToRows(byte[] arrowData, List<VineColumnHandle> columns) throws IOException {
        if (arrowData == null || arrowData.length == 0) {
            return new Object[0][];
        }

        BufferAllocator childAllocator = ALLOCATOR.newChildAllocator("arrow-to-rows", 0, Long.MAX_VALUE);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(arrowData);
             ArrowStreamReader reader = new ArrowStreamReader(bais, childAllocator)) {

            List<Object[]> rows = new ArrayList<>();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();

            while (reader.loadNextBatch()) {
                int rowCount = root.getRowCount();
                List<FieldVector> allVectors = root.getFieldVectors();

                IntStream.range(0, rowCount)
                        .mapToObj(i -> {
                            Object[] row = new Object[columns.size()];
                            for (int c = 0; c < columns.size(); c++) {
                                VineColumnHandle col = columns.get(c);
                                int ordinal = col.getOrdinalPosition();
                                if (ordinal < allVectors.size()) {
                                    row[c] = extractValue(allVectors.get(ordinal), i, col.getType());
                                }
                            }
                            return row;
                        })
                        .forEach(rows::add);
            }

            return rows.toArray(new Object[0][]);
        } finally {
            childAllocator.close();
        }
    }

    /**
     * Extract value from Arrow vector, converting to 'Trino compatible' Java types.
     *
     * Trino types:
     * - TINYINT, SMALLINT, INTEGER, BIGINT, DATE -> long (via getLong)
     * - REAL -> long (Float.floatToRawIntBits, via getLong)
     * - DOUBLE -> double (via getDouble)
     * - BOOLEAN -> boolean (via getBoolean)
     * - VARCHAR -> String (converted to Slice in cursor)
     * - VARBINARY -> byte[] (converted to Slice in cursor)
     * - TIMESTAMP -> long (millis, via getLong)
     */
    private static Object extractValue(FieldVector vector, int index, Type trinoType) {
        if (vector.isNull(index)) {
            return null;
        }

        switch (vector.getMinorType()) {
            case TINYINT:
                return (long) ((TinyIntVector) vector).get(index);
            case SMALLINT:
                return (long) ((SmallIntVector) vector).get(index);
            case INT:
                return (long) ((IntVector) vector).get(index);
            case BIGINT:
                return ((BigIntVector) vector).get(index);
            case FLOAT4:
                return (long) Float.floatToRawIntBits(((Float4Vector) vector).get(index));
            case FLOAT8:
                return ((Float8Vector) vector).get(index);
            case BIT:
                return ((BitVector) vector).get(index) == 1;
            case VARCHAR:
                return new String(((VarCharVector) vector).get(index));
            case VARBINARY:
                return ((VarBinaryVector) vector).get(index);
            case DATEDAY:
                return (long) ((DateDayVector) vector).get(index);
            case TIMESTAMPMILLI:
                return ((TimeStampMilliVector) vector).get(index);
            default:
                Object obj = vector.getObject(index);
                return obj != null ? obj.toString() : null;
        }
    }
}

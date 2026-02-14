# Vine Trino Connector

A read-only [Trino](https://trino.io/) connector for querying Vine tables stored in [Vortex](https://github.com/spiraldb/vortex) columnar format (`.vtx`) via standard SQL.

## Requirements

- Java 11+
- Trino 439
- vine-core native library (requires Rust build)

## Build

```bash
# Build vine-core native library (Rust)
cd vine-core && cargo build --release && cd ..

# Build vine-trino
cd vine-trino
./gradlew clean build       # compile + test
./gradlew shadowJar         # produce deployable fat JAR
```

Build artifacts:
- `build/libs/vine-trino-0.1.0-all.jar` — shadow JAR (bundles Arrow, Jackson with relocated packages)
- `build/libs/vine-trino-0.1.0.jar` — thin JAR

## Deployment

### 1. Install the Plugin

```bash
mkdir -p <trino-home>/plugin/vine/
cp build/libs/vine-trino-0.1.0-all.jar <trino-home>/plugin/vine/

# Copy the native library for your platform:
# macOS
cp ../vine-core/target/release/libvine_core.dylib <trino-home>/plugin/vine/
# Linux
cp ../vine-core/target/release/libvine_core.so <trino-home>/plugin/vine/
```

### 2. Configure the Catalog

Create `<trino-home>/etc/catalog/vine.properties`:

```properties
connector.name=vine
vine.data-dir=/path/to/vine/tables
```

### 3. Data Directory Layout

The path specified by `vine.data-dir` must follow this structure. Each subdirectory with a `vine_meta.json` file is treated as a table.

```
/path/to/vine/tables/
├── events/
│   ├── vine_meta.json
│   ├── 2024-12-26/
│   │   ├── data_143025_123456000.vtx
│   │   └── data_150130_789012000.vtx
│   └── 2024-12-27/
│       └── data_091500_345678000.vtx
└── users/
    ├── vine_meta.json
    └── 2024-12-26/
        └── data_100000_000000000.vtx
```

### 4. Query

```sql
-- List tables
SHOW TABLES FROM vine.default;

-- Inspect schema
DESCRIBE vine.default.events;

-- Query data
SELECT * FROM vine.default.events;

SELECT user_id, COUNT(*) AS event_count
FROM vine.default.events
GROUP BY user_id;
```

## Architecture

### Data Flow

```
Trino SQL Query
    │
    ▼
VinePlugin              ← discovered via ServiceLoader
    │
    ▼
VineConnectorFactory    ← reads vine.data-dir config
    │
    ▼
VineConnector
    ├─ VineConnectorMetadata   vine_meta.json → schema / table / column info
    ├─ VineSplitManager        one split per table
    └─ VineRecordSetProvider
            │
            ▼
        VineModule.readDataArrow(path)   [JNI → Rust vine-core]
            │
            ▼
        Arrow IPC bytes
            │
            ▼
        VineArrowConverter               Arrow IPC → Object[][]
            │
            ▼
        VineRecordCursor                 row-by-row delivery to Trino
```

### Module Structure

```
io.kination.vine/
├── VinePlugin.java                Trino Plugin entry point
├── VineConnectorFactory.java      Creates Connector from catalog properties
├── VineConnector.java             Read-only connector (metadata + splits + record sets)
├── VineTransactionHandle.java     Singleton transaction handle
│
├── VineConnectorMetadata.java     Schema discovery (listSchemas, listTables, getColumnHandles)
├── VineMetadata.java              vine_meta.json POJO
├── VineMetadataReader.java        vine_meta.json parser (Jackson)
├── VineTypeMapping.java           Vine type → Trino type mapping
│
├── VineTableHandle.java           Table reference (schema, name, path)
├── VineColumnHandle.java          Column reference (name, type, ordinal)
│
├── VineSplitManager.java          Split generation (1 per table)
├── VineSplit.java                 Split payload (table path)
│
├── VineRecordSetProvider.java     JNI invocation → RecordSet creation
├── VineRecordSet.java             Holds Arrow data, produces cursor
├── VineRecordCursor.java          Row-by-row cursor consumed by Trino
├── VineArrowConverter.java        Arrow IPC → Object[][] conversion
│
└── VineModule.java                JNI bridge (readDataArrow)
```

### Type Mapping

| Vine Type (`vine_meta.json`) | Alias | Trino Type | Arrow Vector | Cursor Method |
|---|---|---|---|---|
| `integer` | `int` | `INTEGER` | IntVector | `getLong()` |
| `long` | `bigint` | `BIGINT` | BigIntVector | `getLong()` |
| `short` | `smallint` | `SMALLINT` | SmallIntVector | `getLong()` |
| `byte` | `tinyint` | `TINYINT` | TinyIntVector | `getLong()` |
| `float` | — | `REAL` | Float4Vector | `getLong()` (float bits) |
| `double` | — | `DOUBLE` | Float8Vector | `getDouble()` |
| `boolean` | `bool` | `BOOLEAN` | BitVector | `getBoolean()` |
| `string` | — | `VARCHAR` | VarCharVector | `getSlice()` |
| `binary` | — | `VARBINARY` | VarBinaryVector | `getSlice()` |
| `date` | — | `DATE` | DateDayVector | `getLong()` |
| `timestamp` | — | `TIMESTAMP(3)` | TimeStampMilliVector | `getLong()` |
| `decimal` | — | `VARCHAR` | VarCharVector | `getSlice()` |

### vine_meta.json Schema

```json
{
  "table_name": "events",
  "fields": [
    {"id": 1, "name": "user_id", "data_type": "integer", "is_required": true},
    {"id": 2, "name": "event_type", "data_type": "string", "is_required": false},
    {"id": 3, "name": "timestamp", "data_type": "long", "is_required": true}
  ]
}
```

## Limitations

- **Read-only** — only `SELECT` queries are supported; `INSERT`, `UPDATE`, and `DELETE` are not implemented.
- **Single split per table** — the entire table is read in one pass, which may cause high memory usage for large datasets.
- **Single schema** — all tables reside under the `default` schema.
- **No partition pruning** — all date partitions are read regardless of query predicates.

## Roadmap

- Per-partition splits for parallel reads
- Partition pruning based on `WHERE` clause predicates
- Predicate pushdown
- Column pruning (project only required columns)
- Hive Metastore (HMS) integration

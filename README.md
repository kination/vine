# Vine - Datalake Format base on Rust (WIP)

> **Status**: Work in Progress

This project aimes 'datalake table format' optimized for `streaming data writes`, built on Rust. 

## Quick Start

### Build

```bash
./build.sh
```

This builds:
- `vine-core`: Rust library for Vine
- `vine-spark`: Spark DataSource V2 connector

### Usage with Spark

```scala
// Write streaming data
spark.readStream
  .format("vine")
  .load("input-path")
  .writeStream
  .format("vine")
  .option("path", "/data/my-table")
  .start()

// Read with Spark SQL
val df = spark.read.format("vine").load("/data/my-table")
df.show()
```

## Architecture

```
┌─────────────────────────────────────┐
│   Query Engines (Spark, Trino)      │
└──────────────┬──────────────────────┘
               │ DataSource API
┌──────────────▼──────────────────────┐
│  Connectors (vine-spark/vine-trino) │
└──────────────┬──────────────────────┘
               │ JNI
┌──────────────▼──────────────────────┐
│  Rust Core (vine-core)              │
│  - Fast 'vortex' writes             │
│  - Date-based partitioning          │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│  Storage (vortex files)             │
│  2024-12-26/data_143025.vtx         │
│  2024-12-27/data_091500.vtx         │
└─────────────────────────────────────┘
```

## Components

| Component | Language | Status | Purpose |
|-----------|----------|--------|---------|
| **vine-core** | Rust | WIP | Write-optimized datalake table format |
| **vine-spark** | Scala | WIP | Spark DataSource V2 connector |
| **vine-trino** | Java | Planned | Trino connector (not started) |

## Storage Format

- **Files**: [Vortex](https://github.com/vortex-data/vortex)
- **Partitioning**: Date-based directories (`YYYY-MM-DD/data_HHMMSS.vtx`)
- **Metadata**: JSON schema file (`vine_meta.json`)
- **Types**: integer, string, boolean, double

## Documentation

- [Presentation](https://docs.google.com/presentation/d/1-gT93SZ0XrQrAGgmbkHzQlQj_U38BkIdYl2T_pj_aC4/edit?slide=id.p)

## Development

### Build Components Individually

**Rust Core**
```bash
cd vine-core
cargo build --release
cargo test
```

**Spark Connector**
```bash
cd vine-spark
sbt clean assembly
```

### Requirements

- Rust 1.70+
- Scala 2.13, sbt 1.x
- Java 11

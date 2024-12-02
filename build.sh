#/bin/sh
cd vine-core
cargo build --release

cd ../vine-spark
sbt clean assembly
cd ..

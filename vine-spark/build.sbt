
// The simplest possible sbt build file is just one line:

scalaVersion := "2.13.12"
// That is, to create a valid sbt build, all you've got to do is define the
// version of Scala you'd like your project to use.

// ============================================================================

// Lines like the above defining `scalaVersion` are called "settings". Settings
// are key/value pairs. In the case of `scalaVersion`, the key is "scalaVersion"
// and the value is "2.13.12"

// It's possible to define many kinds of settings, such as:

name := "vine-spark"
organization := "io.kination.vine"
version := "0.2.0"

// Java options for Spark compatibility
Test / fork := true
Test / javaOptions ++= Seq(
  s"-Djava.library.path=${baseDirectory.value}/../vine-core/target/release"
)


// Note, it's not required for you to define these three settings. These are
// mostly only necessary if you intend to publish your library's binaries on a
// place like Sonatype.


// Want to use a published library in your project?
// You can define other libraries as dependencies in your build like this:

// Spark version for Arrow compatibility
val sparkVersion = "3.4.0"
val arrowVersion = "14.0.2"
val jacksonVersion = "2.14.3" // Downgrade to fix compatibility with Scala module 2.14.2
val hiveVersion = "2.3.9" // Hive Metastore version compatible with Spark 3.4

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.apache.parquet" % "parquet-avro" % "1.12.0",
    "org.scalatest" %% "scalatest" % "3.2.17" % Test,
    // Apache Arrow for high-performance JNI data transfer
    "org.apache.arrow" % "arrow-vector" % arrowVersion,
    "org.apache.arrow" % "arrow-memory-netty" % arrowVersion,
    // Hive Metastore for catalog integration
    "org.apache.hive" % "hive-metastore" % hiveVersion % Provided excludeAll(
      ExclusionRule(organization = "org.pentaho"),
      ExclusionRule(organization = "org.apache.logging.log4j"),
      ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"),
      ExclusionRule(organization = "log4j", name = "log4j")
    ),
    "org.apache.hive" % "hive-exec" % hiveVersion % Provided excludeAll(
      ExclusionRule(organization = "org.pentaho"),
      ExclusionRule(organization = "org.apache.logging.log4j"),
      ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"),
      ExclusionRule(organization = "log4j", name = "log4j")
    ),
    "org.apache.thrift" % "libthrift" % "0.12.0" % Provided
)

// Force Jackson version downgrade for Spark compatibility
// Arrow 14.0.2 brings Jackson 2.15.x, but Spark 3.4 needs 2.14.x
dependencyOverrides ++= Seq(
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @_*) => MergeStrategy.discard
    case x => MergeStrategy.first
}
mainClass in assembly := Some("io.kination.vine.VineDataSource")

// Here, `libraryDependencies` is a set of dependencies, and by using `+=`,
// we're adding the scala-parser-combinators dependency to the set of dependencies
// that sbt will go and fetch when it starts up.
// Now, in any Scala file, you can import classes, objects, etc., from
// scala-parser-combinators with a regular import.

// TIP: To find the "dependency" that you need to add to the
// `libraryDependencies` set, which in the above example looks like this:

// "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0"

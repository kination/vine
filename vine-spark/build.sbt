
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

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "3.4.0" % Provided,
    "org.apache.parquet" % "parquet-avro" % "1.12.0",
    "org.scalatest" %% "scalatest" % "3.2.17" % Test
//    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.0"
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

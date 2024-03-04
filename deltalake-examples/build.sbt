ThisBuild / version := "0.0.1-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

val SparkVersion = "3.5.0"

val DeltaVersion = "3.1.0"

val GlueVersion = "4.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "deltalake-examples",
    idePackagePrefix := Some("demo.spark.delta-lake"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % SparkVersion,
      "io.delta" %% "delta-spark" % DeltaVersion,
    )
  )
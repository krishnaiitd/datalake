name := "SparkDemo"
description := "This codebase is only for Hello World type code base like " +
  "csv to df, jdbc to df, and register temp table view and do some SQL learning"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2"
)


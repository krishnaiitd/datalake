ThisBuild / name := "Hudi-Ingestion"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

val SparkVersion = "3.3.3"

val HudiVersion = "0.14.1"

val GlueVersion = "4.0.0"

lazy val root = (project in file("."))
  .settings(
//    resolvers += "aws-glue-etl-artifacts" at "https://aws-glue-etl-artifacts.s3.amazonaws.com/release/",
    libraryDependencies ++= Seq(
//      Apache Hudi dependency
      "org.apache.hudi" %% "hudi-spark3.3-bundle" % HudiVersion,
      "org.apache.spark" %% "spark-sql" % SparkVersion,
      //      AWS Hudi Dependency
      //      "org.apache.hudi" % "hudi-aws-bundle" % HudiVersion % Provided,
      //      "com.amazonaws" % "AWSGlueETL" % GlueVersion % Provided,
      //      Spark Dependency
      //      "org.apache.spark" %% "spark-avro" % SparkVersion,
      //      "org.apache.spark" %% "spark-core" % SparkVersion,
//      Test Dependency
//      "org.scalatest" %% "scalatest" % "3.2.15" % "test"
    )
  )
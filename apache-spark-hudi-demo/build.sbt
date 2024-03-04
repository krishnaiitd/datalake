ThisBuild / version := "0.0.1-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

val SparkVersion = "3.3.3"

val HudiVersion = "0.14.1"

val GlueVersion = "4.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "apache-spark-hudi-demo",
    idePackagePrefix := Some("demo.spark.hudi"),
    libraryDependencies ++= Seq(
     "org.apache.spark" %% "spark-sql" % SparkVersion,
      "org.apache.hudi" %% "hudi-spark3.3-bundle" % HudiVersion,
    )
  )

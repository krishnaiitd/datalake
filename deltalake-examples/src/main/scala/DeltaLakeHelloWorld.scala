import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object DeltaLakeHelloWorld {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val deltaConf = Map(
      "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

    val spark : SparkSession = deltaConf.foldLeft(
      SparkSession.builder()) {
      (builder, cfg) =>
        builder.config(cfg._1, cfg._2)
      }.master("local[*]")
      .appName("MultipleTableTest")
      .getOrCreate()

    val df = spark.range(10)
    df.show()
    df.write
      .format("delta")
//      .partitionBy("id")
      .mode(SaveMode.Overwrite)
      .save("/tmp/delta-table")
    df.show(10)

  }
}
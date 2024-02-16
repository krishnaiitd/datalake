import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SimpleSparkApp {

  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder
      .appName("SimpleSparkApp")
      .master("local[*]") // You can specify the master URL based on your Spark cluster setup
      .getOrCreate()

//    Set the log level to avoid excessive logging on your console while executing it
    spark.sparkContext.setLogLevel(Level.WARN.toString)

    // Read a CSV file into a DataFrame
    val inputFilePath = "data/example1.csv"
    val df: DataFrame = spark.read.option("header", "true").csv(inputFilePath)

    // Perform a simple transformation (e.g., add a new column)
    val resultDF: DataFrame = df.withColumn("new_column", lit("Hello, Spark!"))

    println("From CSV Read to show directly: raw data")
    // Show the schema
    resultDF.printSchema()
    // Show the result, only 5 records and don't truncate the output column values
    resultDF.show(numRows = 5, truncate = false)

    // Write back to a CSV file
    val outputFilePath = "data/output/csv"
    resultDF.write.mode(saveMode = SaveMode.Overwrite).option("header", "true").csv(outputFilePath)

    // Let's suppose you want to store the data in parquet file
    val parquetBaseFilePath = "data/output/parquet"
    resultDF.write
      .mode(saveMode = SaveMode.Overwrite)
      .parquet(parquetBaseFilePath)

    // Do check the parquet files structure at cd data/output/parquet, and then tree -a:

    // Do more like register a temp table

//    Create or Replace a table
    resultDF.createOrReplaceTempView("person")
    println("From Temp table, do any kind of analysis")
    spark.sql("select * from person").show(truncate = false)

    // Some aggregation query:

    // Just to explore things in Spark.SQL
//    1. What are the databases default exist
    println("show databases")
    spark.sql("show databases").show(truncate = false)
    spark.sql("show tables").show(truncate = false)


    // Finally drop the Temp table, its optional
    println("drop the temp table and check the list of tables")
    spark.sql("drop table person")
    spark.sql("show tables").show(truncate = false)

    // Stop the Spark session
    spark.stop()
  }
}


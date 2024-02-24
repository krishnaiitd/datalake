package spark.java.demo;

import org.apache.log4j.Level;
import org.apache.spark.sql.*;

public class SparkHelloWorld {
    public static void main(String[] args) {
        // Create a Spark session
        SparkSession spark = SparkSession.builder()
                .appName("SimpleSparkApp")
                .master("local[*]") // You can specify the master URL based on your Spark cluster setup
                .getOrCreate();

        //    Set the log level to avoid excessive logging on your console while executing it
        spark.sparkContext().setLogLevel(Level.WARN.toString());

        // Read a CSV file into a DataFrame
        String inputFilePath = "data/example1.csv";
        Dataset<Row> df = spark
                .read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(inputFilePath);
//        val resultDF: DataFrame = df.withColumn("new_column", lit("Hello, Spark!"))

//        System.out.println("Hello");
//        System.out.println(df.schema());
//        df.printSchema();
        System.out.println("Original data");
        df.show(20, false);

        Dataset<Row> resultDf = df.withColumn("new_column", functions.lit("Hello"));
//        resultDf.schema();
//        resultDf.printSchema();
        System.out.println("After transformation");
        resultDf.show(20, false);

        // Store this in Parquet file and read back
        String parquetPath = "./data/output/parquets/example";
        resultDf
                .write()
                .mode(SaveMode.Overwrite)
                .format("parquet")
                .parquet(parquetPath);

        // Let's read it
        Dataset<Row> parquetDF = spark.read().parquet(parquetPath);
        parquetDF.createOrReplaceTempView("people");
        System.out.println("show databases");
        spark.sql("show databases").show(10);
        System.out.println("show tables");
        spark.sql("show tables").show(10);
        System.out.println("desc people");
        spark.sql("desc people").show(10);
        System.out.println("Run SELECT statements");
        spark.sql("select * from people").show(10);
        
        spark.stop();
    }
}

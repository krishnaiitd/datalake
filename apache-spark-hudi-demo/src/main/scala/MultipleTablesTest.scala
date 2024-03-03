package demo.spark.hudi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object MultipleTablesTest {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = Map(
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
      "spark.sql.extensions" -> "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
      "spark.kryo.registrator" -> "org.apache.spark.HoodieSparkKryoRegistrar"
    )

    val spark: SparkSession = sparkConf.foldLeft(SparkSession.builder()) { (builder, cfg) =>
      builder.config(cfg._1, cfg._2)
    }
      .master("local[*]")
      .appName("MultipleTableTest")
      .getOrCreate()

    spark.sparkContext.setLogLevel(Level.WARN.toString)

    import org.apache.spark.sql.functions._
    import spark.sqlContext.implicits._
    val data = List(
      Row(1, "Prasad", 32),
      Row(2, "Kumar", 3),
      Row(3, "Singh", 76),
      Row(4, "Sinha", 13)
    )

    val schema = StructType(
      Array(
        StructField("id", IntegerType, false),
        StructField("last_name", StringType , true),
        StructField("age", IntegerType, true)
      )
    )
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd, schema)
    df.printSchema()
    df.show()



    // Store into two table with same suffix of folder directory only,
    // prefix of the storage changed based on the table name,
    // e.g: table1 -> Apache Hudi base path s3://<bucket_name>/bronze/table1
    // e.g: table2 -> Apache Hudi base path s3://<bucket_name>/bronze/table2
    // These be able to storage in AWS S3 or in local machine with local path

    val localBasePath = "file:///Users/krishna/Downloads/hudidb"
    val basePathTable1 = localBasePath + "/person_table1"
    val hudi_options_tabl1 = Map(
      "hoodie.table.name" -> "person_table1",
      "hoodie.datasource.write.recordkey.field" -> "id",
      "hoodie.datasource.write.table.name" -> "person_table1",
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.datasource.write.precombine.field" -> "id",
      "hoodie.upsert.shuffle.parallelism" -> "2",
      "hoodie.insert.shuffle.parallelism" -> "2",
      "hoodie.datasource.write.storage.type" -> "COPY_ON_WRITE",
    )
    // Save into localBasePath + "/person_table1"
    df.write
      .format("hudi")
      .options(hudi_options_tabl1)
      .mode(SaveMode.Overwrite)
      .save(basePathTable1)
    println("Writing to table 1 finished")
//    Store same dataset into different table as below
    val basePathTable2 = localBasePath + "/person_table2"
    val hudi_options_tabl2 = Map(
      "hoodie.table.name" -> "person_table2",
      "hoodie.datasource.write.recordkey.field" -> "id",
      "hoodie.datasource.write.table.name" -> "person_table2",
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.datasource.write.precombine.field" -> "id",
      "hoodie.upsert.shuffle.parallelism" -> "2",
      "hoodie.insert.shuffle.parallelism" -> "2",
      "hoodie.datasource.write.storage.type" -> "MERGE_ON_READ",
    )
    df.write
      .format("hudi")
      .options(hudi_options_tabl2)
      .mode(SaveMode.Append)
      .save(basePathTable2)
    println("Writing to table 2 finished")

  }
}
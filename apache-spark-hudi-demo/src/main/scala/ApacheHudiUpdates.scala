package demo.spark.hudi

import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME, QUERY_TYPE, QUERY_TYPE_INCREMENTAL_OPT_VAL, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import java.util.{Calendar, TimeZone}

object ApacheHudiUpdates {

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
    val data = List(
      Row(1, "Row 1 age 10", 10),
      Row(2, "Row 2 age 11", 11),
      Row(3, "Row 3 age 12", 12),
      Row(4, "Row 4 age 13", 13),
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



    // Store into a table called apache_hudi_udate_test
    // basePath = /Users/krishna/Downloads/hudidb
    val localBasePath = "file:///Users/krishna/Downloads/hudidb"
    val basePathTable = localBasePath + "/apache_hudi_udate_test"

    val hudi_options = Map(
      "hoodie.table.name" -> "apache_hudi_udate_test",
      TABLE_NAME.key() -> "apache_hudi_udate_test",
      RECORDKEY_FIELD.key() -> "id",
      PRECOMBINE_FIELD.key() -> "id",
      OPERATION.key() -> UPSERT_OPERATION_OPT_VAL,
      TABLE_TYPE.key() -> MOR_TABLE_TYPE_OPT_VAL,
      "hoodie.upsert.shuffle.parallelism" -> "2",
      "hoodie.insert.shuffle.parallelism" -> "2",
    )
    // Save into localBasePath + "/person_table1"
    println("Writing to table started at: "  +  Calendar.getInstance(TimeZone.getTimeZone("JST")).toInstant.toString)
    df.write
      .format("hudi")
      .options(hudi_options)
      .mode(SaveMode.Overwrite)
      .save(basePathTable)
    println("Writing to table finished at: "  +  Calendar.getInstance(TimeZone.getTimeZone("JST")).toInstant.toString)

    println("Snapshot Queries started at: " +  Calendar.getInstance(TimeZone.getTimeZone("JST")).toInstant.toString)

    val snapshotReadDF = spark
      .read
      .format("hudi")
      .option(QUERY_TYPE.key(), QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePathTable)

    snapshotReadDF.printSchema()
    snapshotReadDF.show(truncate = false)
    println("Snapshot Queries finished at: " +  Calendar.getInstance(TimeZone.getTimeZone("JST")).toInstant.toString)


    println("Incremental query started at: " +  Calendar.getInstance(TimeZone.getTimeZone("JST")).toInstant.toString)
    val incrementalQueryDF = spark
      .read
      .format("hudi")
      .option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(BEGIN_INSTANTTIME.key(), "0") // <beginInstantTime> => we could changed to something else
      .load(basePathTable)

    incrementalQueryDF.printSchema()
    incrementalQueryDF.createOrReplaceTempView("table111")
    spark.sql("select * from table111").show(truncate = false)
    println("Incremental query finished at: " + Calendar.getInstance(TimeZone.getTimeZone("JST")).toInstant.toString)


//    // Update the age by 2 for each even age group and upsert it into the same table
//
//    val updatedDF = snapshotReadDF.filter(col("age") % 2 === 0).withColumn("age", col("age") + 2)
//    updatedDF
//      .write.format("hudi")
//      .options(hudi_options)
//      .mode(SaveMode.Append)
//      .save(basePathTable)
//
//    println("After update Snapshot Queries started at: " + spark.time().toString)
//    val updatedSnapshotRead = spark
//      .read
//      .format("hudi")
//      .option(QUERY_TYPE.key(), QUERY_TYPE_SNAPSHOT_OPT_VAL)
//      .load(basePathTable)
//
//    updatedSnapshotRead.printSchema()
//    updatedSnapshotRead.show(truncate = false)
//    println("After update Snapshot Queries finished at: " + spark.time().toString)


  }
}
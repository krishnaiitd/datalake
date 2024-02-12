package local

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, date_format, to_timestamp}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}

object Csv2Hudi {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    // @todo: Need to pass this in the sparkSession config, not one by one
    val sparkConf = Map(
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
      "spark.sql.extensions" -> "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
      "spark.kryo.registrator" -> "org.apache.spark.HoodieSparkKryoRegistrar"
    )

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("CSV2HudiLocal")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    spark.sparkContext.setLogLevel(Level.WARN.toString)

    val sourceCsvFileBasePath = "data/employees.csv"

    val initDf = spark.read
      .format("csv")
      .option("header", "true")
      .load(sourceCsvFileBasePath)

//    To check the sample data
    initDf.printSchema()
    initDf.show(numRows = 5, truncate = false)

    // Store to local hudi table in MoR type table and partition it as daily based on hire_date column

//    Will the choose for partition column
//    |-- HIRE_DATE: string (nullable = true)
    val partitionColumn = "HIRE_DATE"

    val PartitionLabel:String = "_partition"

    // Convert the exist column to timestamp which will be used for partitionColumn
    val df = initDf.withColumn(partitionColumn, to_timestamp(col(partitionColumn), "yyyy/MM/dd"))

    // assign the partition column value to new Column with defined partition format
    val partitionFormat = "yyyy-MM-dd"
    val pdf = df.withColumn(PartitionLabel, date_format(col(partitionColumn), partitionFormat))

    pdf.printSchema()
    pdf.show(numRows = 5, truncate = false)

    // Transform data type of some of the column
    val tdf = pdf
      .withColumn("EMPLOYEE_ID", col("EMPLOYEE_ID").cast(LongType))
      .withColumn("DEPARTMENT_ID", col("DEPARTMENT_ID").cast(IntegerType))
      .withColumn("MANAGER_ID", col("MANAGER_ID").cast(IntegerType))
      .withColumn("SALARY", col("SALARY").cast(DoubleType))

    // make toLowerCase for all the columns
    val fdf = tdf.columns.foldLeft(tdf) {
      (tmpDf, colName) => tmpDf.withColumnRenamed(colName, colName.toLowerCase)
    }
    fdf.printSchema()
    fdf.show(numRows = 5, truncate = false)

    val tableName =  "mor_employee"
    val dbName = "bronze"
    val hudiOptions = Map(
      "hoodie.table.name" -> tableName,
      "hoodie.datasource.write.storage.type" -> "MERGE_ON_READ",
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.datasource.write.recordkey.field" -> "employee_id",
      "hoodie.datasource.write.precombine.field" -> "employee_id",
      "hoodie.datasource.write.partitionpath.field" -> PartitionLabel,
      "hoodie.datasource.write.hive_style_partitioning" -> "true"
    )

    val hiveOptions =  Map(
      "hoodie.datasource.hive_sync.enable" -> "true",
      "hoodie.datasource.hive_sync.database" -> dbName,
      "hoodie.datasource.hive_sync.table" -> tableName,
      "hoodie.datasource.hive_sync.partition_fields" -> PartitionLabel,
      "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.MultiPartKeysValueExtractor",
      "hoodie.datasource.hive_sync.use_jdbc" -> "false",
      "hoodie.datasource.hive_sync.mode" -> "hms",
    )

    // Merge hiveOptions only if you have hive enabled
    val options =  hudiOptions ++ hiveOptions
    println(options.toString())

    val localLoadPath = "file:///Users/krishna/Downloads/hudidb/bronze/mor_daily_employee"
    fdf.write.format("hudi")
      .options(hudiOptions)
      .mode(SaveMode.Overwrite)
      .save(localLoadPath)

    val hudiDF = spark.read.format("hudi").load(localLoadPath)
    hudiDF.printSchema()
    hudiDF.show(numRows = 2, truncate = false)

  }

}

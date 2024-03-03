package local

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object Main {
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
    val data = Seq(("1", "Prasad", 32), ("2", "Singh", 3), ("3", "Kumar", 76), ("4", "Prasad", 13))
    val schema = Seq("id", "last_name", "age")

    val rdd = spark.sparkContext.parallelize(data)

    val df = rdd.toDF(schema:_*)

    val partitionLabel = "_partition"
//    add partitin column "_partition", only two partition as of now, i.e % 2 on age
    val df2 = df.withColumn(partitionLabel, when(col("age") % 2 === 0, "even").otherwise("old"))
    print("Getting the data from partitioned data frame")
    df2.printSchema()
    df2.show()


    val tableName = "partition_demo_hudi_cow"
    val localBasePath = "file:///Users/krishna/Downloads/hudidb"
//    val basePathCoW = "s3://<BUCKET_NAME>/bronze/medium-post-examples/apache-hudi/partition_demo_hudi_cow"
    val basePathCoW = localBasePath + "/medium-post-examples/apache-hudi/cow_partition_demo_hudi"
    val hudi_options = Map(
      "hoodie.table.name" -> tableName,
      "hoodie.datasource.write.recordkey.field" -> "id",
      "hoodie.datasource.write.partitionpath.field" -> partitionLabel,
      "hoodie.datasource.write.table.name" -> tableName,
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.datasource.write.precombine.field" -> "id",
      "hoodie.upsert.shuffle.parallelism" -> "2",
      "hoodie.insert.shuffle.parallelism" -> "2",
      "hoodie.datasource.write.storage.type" -> "COPY_ON_WRITE",
    )

    df2.write
      .format("hudi")
      .options(hudi_options)
      .mode("append")
      .save(basePathCoW)

    println("Written to " + basePathCoW)
   
//    val basePathMoR = "s3://<BUCKET_NAME>/bronze/medium-post-examples/apache-hudi/partition_demo_hudi_mor"
    val basePathMoR = localBasePath + "/medium-post-examples/apache-hudi/mor_partition_demo_hudi"

    val morHudiOption = hudi_options.updated("hoodie.datasource.write.storage.type", "MERGE_ON_READ")

    println("BasePathMoR: " + basePathMoR)
    println("morHudiOption: " + morHudiOption.toString())

    df2.write
      .format("hudi")
      .options(morHudiOption)
      .mode("append")
      .save(basePathMoR)

    // Reading Apache Hudi table
    println("Reading Apache Hudi table")
    val columnNames = Seq("_hoodie_partition_path", "_hoodie_file_name", "age", partitionLabel)

//    CoW
    println("CoW")
    val cowTable = spark.read.format("hudi")
      .load(basePathCoW)
//    cowTable.select(col("_hoodie_partition_path"), col(partitionLabel)).show(truncate = false)
    cowTable.select(columnNames.map(c => col(c)) :_*).show(truncate = false)
//    MoR
    println("MoR")
    val morTable = spark.read.format("hudi").load(basePathMoR)
    morTable.select(columnNames.map(c => col(c)) :_*).show(truncate = false)

//    @todo: Trying reading different type table read concepts like incremental, snapshot, time travel etc.

  }
}
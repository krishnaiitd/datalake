import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("TestingSparkOnlyLocally")
      .getOrCreate()

    import org    .apache.spark.sql.functions._
    import spark.sqlContext.implicits._
    val data = Seq(("1", "Prasad", 32), ("2", "Singh", 3), ("3", "Kumar", 76), ("4", "Prasad", 13))
    val schema = Seq("id", "last_name", "age")

    val rdd = spark.sparkContext.parallelize(data)

    val df = rdd.toDF(schema:_*)

    val partitionLabel = "_partition"
//    add partitin column "_partition", only two partition as of now, i.e % 2 on age
    val df2 = df.withColumn("_partition", when(col("age") % 2 === 0, "even").otherwise("old"))
    df2.printSchema()
    df2.show()


    val tableName = "partition_demo_hudi_cow"
    val basePathCoW = "s3://unified-datalake/bronze/medium-post-examples/apache-hudi/partition_demo_hudi_cow"
    val hudi_options = Map(
      "hoodie.table.name" -> tableName,
      "hoodie.datasource.write.recordkey.field" -> "uuid",
      "hoodie.datasource.write.partitionpath.field" -> partitionLabel,
      "hoodie.datasource.write.table.name" -> tableName,
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.datasource.write.precombine.field" -> "ts",
      "hoodie.upsert.shuffle.parallelism" -> "2",
      "hoodie.insert.shuffle.parallelism" -> "2",
      "hoodie.datasource.write.storage.type" -> "COPY_ON_WRITE",
    )

    df2.write
      .format("hudi")
      .options(hudi_options)
      .mode("overwrite")
      .save(basePathCoW)

    print("Written to " + basePath)
   
    val basePathMoR = val basePath = "s3://unified-datalake/bronze/medium-post-examples/apache-hudi/partition_demo_hudi_mor"
    df2.write
      .format("hudi")
      .options(hudi_options)
      .mode("overwrite")
      .save(basePathMoR)
  }
}
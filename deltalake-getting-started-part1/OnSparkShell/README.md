# Getting Start with Delta lake

Install Apache Spark Version 3.5

## Spark 3.5

### Start the spark-shell as below

    spark-shell \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

### On the spark shell:

Below steps are followed from: https://docs.delta.io/3.1.0/quick-start.html#language-scala

<!-- Import Delta lake libraries -->
    import io.delta.tables._
    import org.apache.spark.sql.functions._

<!-- Create Random Data frame -->
    val data = Seq(("1", "One", "category1"), ("2", "Two", "category2"), ("3", "Three", "category1"), ("4", "Four", "category3"), ("5", "Five", "category2"))
    val schema = Seq("id", "value", "category")
    val rdd = spark.sparkContext.parallelize(data)
    val df = rdd.toDF(schema:_*)

    <!-- Let's store in Delta lake storage format -->
    val basePath = "file:///Users/krishna/Downloads/deltalakedb/no-partition/example1"
    df.write.format("delta").mode("overwrite").save(basePath)
    
    <!-- Store in partition as category -->
    val basePathPartition = "file:///Users/krishna/Downloads/deltalakedb/no-partition/partition_example1"
    df.write.format("delta").partitionBy("category").mode("overwrite").save(basePathPartition)


<!-- Store with partition column derived from exist column -->
<!-- Like you want to create only 100 partition based on the id -->
// Cast column "id" to long
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
val partitionLabel = "_partition"
val cdf = df.withColumn("id", col("id").cast(LongType))
val fdf = cdf.withColumn(partitionLabel, col("id") % 3)

<!-- Store into delta lake format -->
val basePathCustomPartition = "file:///Users/krishna/Downloads/deltalakedb/no-partition/cusotom_partition_example1"
fdf.write.format("delta").partitionBy(partitionLabel).mode("overwrite").save(basePathCustomPartition)

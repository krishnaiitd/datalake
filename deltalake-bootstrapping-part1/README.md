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

    import io.delta.tables._
    import org.apache.spark.sql.functions._

    val data1 = spark.range(5, 10)
    data1.show
        +---+
        | id|
        +---+
        |  5|
        |  6|
        |  7|
        |  8|
        |  9|
        +---+

    data1.write.format("delta").mode("overwrite").save("/tmp/delta-table1")

Let's see the above write on the directory: /tmp/delta-table1

    ➜  delta-table1 tree
    .
    ├── _delta_log
    │   └── 00000000000000000000.json
    ├── part-00000-a7cbb685-8a54-4fb4-95a9-8688fffa546c-c000.snappy.parquet
    ├── part-00001-cad6b5e0-b1f3-4987-a2be-a9562edfa51b-c000.snappy.parquet
    ├── part-00003-2fc43774-1e6f-4c16-912a-48228cbf17e7-c000.snappy.parquet
    ├── part-00004-f4645dab-edb7-4f6e-9b5a-fce463718e5b-c000.snappy.parquet
    ├── part-00006-1e516537-4fa3-46d3-be5f-5287334eefb7-c000.snappy.parquet
    └── part-00007-701fd179-6d54-4fdf-b951-1b1873a92822-c000.snappy.parquet

![And the structure as below in the screenshot](/data/deltalake-bootstrapping-part1/bootstrapping-deltalake-part1.png)

## Try reading above overwritten data as delta read from spark

    val df = spark.read.format("delta").load("/tmp/delta-table1")

    df.show

        df: org.apache.spark.sql.DataFrame = [id: bigint]

        scala> df.printSchema
        root
        |-- id: long (nullable = true)


        scala> df.show
        +---+
        | id|
        +---+
        |  5|
        |  6|
        |  8|
        |  7|
        |  9|
        +---+
 <!-- Updates or merge on exiting table-->

    val deltaTable = DeltaTable.forPath("/tmp/delta-table1")

    val newData = spark.range(0, 20).toDF

    deltaTable.as("oldData")
    .merge(
        newData.as("newData"),
        "oldData.id = newData.id")
    .whenMatched
    .update(Map("id" -> col("newData.id")))
    .whenNotMatched
    .insert(Map("id" -> col("newData.id")))
    .execute()

    val df = spark.read.format("delta").load("/tmp/delta-table1")
        df: org.apache.spark.sql.DataFrame = [id: bigint]

        scala> df.show
        +---+
        | id|
        +---+
        |  0|
        |  1|
        |  2|
        |  3|
        |  4|
        |  5|
        |  6|
        |  7|
        |  8|
        |  9|
        | 10|
        | 11|
        | 12|
        | 13|
        | 14|
        | 15|
        | 16|
        | 17|
        | 18|
        | 19|
        +---+


Read the previous version

    val df = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table1")

        df: org.apache.spark.sql.DataFrame = [id: bigint]

        scala> df.show
        +---+
        | id|
        +---+
        |  5|
        |  6|
        |  8|
        |  7|
        |  9|
        +---+

    val streamingDf = spark.readStream.format("rate").load()


    val stream = streamingDf.select($"value" as "id").writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoint").start("/tmp/delta-table1")



#### Will try later
    val columns = Seq("id","name", "created_at")
    val data = Seq(("1", "Krishna", "2024/01/01"), ("2", "Prasad", "2024/02/20"), ("3", "Veena", "2024/03/25"), ("4", "Krishvi", "2024/02/12"))
    val rdd = spark.sparkContext.parallelize(data)
    var df = rdd.toDF(columns:_*)
    df.show
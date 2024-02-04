# Apache Hudi Partition Demo on the local machine

Download and install spark-3.3.1

and follow the below Quick Start guide
> https://hudi.apache.org/docs/0.13.1/quick-start-guide/

## Spark 3.3
./bin/spark-shell \
  --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'


<!-- val basePath = "file:///Users/krishna/Downloads/hudidb" -->

// In the spark-shell
  
    import org.apache.hudi.QuickstartUtils._
    import scala.collection.JavaConversions._
    import org.apache.spark.sql.SaveMode._
    import org.apache.hudi.DataSourceReadOptions._
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._
    import org.apache.hudi.common.model.HoodieRecord
    
    val tableName = "hudi_trips_cow"
    val basePath = "file:///Users/krishna/Downloads/hudidb/hudi_trips_cow"
    val dataGen = new DataGenerator
    
    val inserts = convertToStringList(dataGen.generateInserts(10))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format("hudi").
    options(getQuickstartWriteConfigs).
    option(PRECOMBINE_FIELD_OPT_KEY, "ts").
    option(RECORDKEY_FIELD_OPT_KEY, "uuid").
    option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
    option(TABLE_NAME, tableName).
    mode(Overwrite).
    save(basePath)

### Examine the partition path and values
    spark.read.format("hudi")
        .load(basePath)
        .select(col("_hoodie_partition_path"),col("partitionpath"))
        .show(truncate= false)

Output as:

    +------------------------------------+------------------------------------+
    |_hoodie_partition_path              |partitionpath                       |
    +------------------------------------+------------------------------------+
    |americas/united_states/san_francisco|americas/united_states/san_francisco|
    |americas/united_states/san_francisco|americas/united_states/san_francisco|
    |americas/united_states/san_francisco|americas/united_states/san_francisco|
    |americas/united_states/san_francisco|americas/united_states/san_francisco|
    |americas/united_states/san_francisco|americas/united_states/san_francisco|
    |americas/brazil/sao_paulo           |americas/brazil/sao_paulo           |
    |americas/brazil/sao_paulo           |americas/brazil/sao_paulo           |
    |americas/brazil/sao_paulo           |americas/brazil/sao_paulo           |
    |asia/india/chennai                  |asia/india/chennai                  |
    |asia/india/chennai                  |asia/india/chennai                  |
    +------------------------------------+------------------------------------+

## Now on Demo with small dataset and partitioning based on Date column and partition can be done as Daily, weekly, monthly:

    val columns = Seq("id","name", "created_at")
    val data = Seq(("1", "Krishna", "2024/01/01"), ("2", "Prasad", "2024/02/20"), ("3", "Veena", "2024/03/25"), ("4", "Krishvi", "2024/02/12"))
    val rdd = spark.sparkContext.parallelize(data)
    var df = rdd.toDF(columns:_*)
    df.show

    scala> df.show
      +---+-------+----------+
      | id|   name|created_at|
      +---+-------+----------+
      |  1|Krishna|2024/01/01|
      |  2| Prasad|2024/02/20|
      |  3|  Veena|2024/03/25|
      |  4|Krishvi|2024/02/12|
      +---+-------+----------+

Now add the partition column as daily from the created_at column 

    // Convert the existing column to timestamp which will be used for partition column
    df = df.withColumn("created_at", to_timestamp(col("created_at"), "yyyy/MM/dd"))
    scala> df.show
    +---+-------+-------------------+
    | id|   name|         created_at|
    +---+-------+-------------------+
    |  1|Krishna|2024-01-01 00:00:00|
    |  2| Prasad|2024-02-20 00:00:00|
    |  3|  Veena|2024-03-25 00:00:00|
    |  4|Krishvi|2024-02-12 00:00:00|
    +---+-------+-------------------+
    // Assign the partition column value to a new Column with the defined partition format
    val partitonColumn = "_partition"
    val partitionFormat = "yyyy-MM-dd"
    df = df.withColumn(partitonColumn, date_format(col("created_at"), partitionFormat))
      scala> df.show
      +---+-------+-------------------+----------+
      | id|   name|         created_at|_partition|
      +---+-------+-------------------+----------+
      |  1|Krishna|2024-01-01 00:00:00|2024-01-01|
      |  2| Prasad|2024-02-20 00:00:00|2024-02-20|
      |  3|  Veena|2024-03-25 00:00:00|2024-03-25|
      |  4|Krishvi|2024-02-12 00:00:00|2024-02-12|
      +---+-------+-------------------+----------+

Now we have derived a table that can be used as a partition setting in the Apache Hudi table, let's create a table namely daily_data_demo

    val tableName = "daily_data_demo"
    val dailyTableBasePath = "file:///Users/krishna/Downloads/hudidb/daily_data_demo"
    df.write.format("hudi").
    options(getQuickstartWriteConfigs).
    option(PRECOMBINE_FIELD_OPT_KEY, "id").
    option(RECORDKEY_FIELD_OPT_KEY, "id").
    option(PARTITIONPATH_FIELD_OPT_KEY, "_partition").
    option(TABLE_NAME, tableName).
    mode(Overwrite).
    save(dailyTableBasePath)

You can visualize the partition path by going into that directory and typing `tree` on your directory terminal

![Daily Partition Examples](/data/apache-hudi-partitionpath-demo/daily_data_demo.png)

Now considering changing the partition to monthly, it's super easy

    val monhtlyPartitionFormat = "yyyy-MM"
    df = df.withColumn(partitonColumn, date_format(col("created_at"), monhtlyPartitionFormat))
      scala> df.show
      +---+-------+-------------------+----------+
      | id|   name|         created_at|_partition|
      +---+-------+-------------------+----------+
      |  1|Krishna|2024-01-01 00:00:00|   2024-01|
      |  2| Prasad|2024-02-20 00:00:00|   2024-02|
      |  3|  Veena|2024-03-25 00:00:00|   2024-03|
      |  4|Krishvi|2024-02-12 00:00:00|   2024-02|
      +---+-------+-------------------+----------+
    val monhtlyTableName = "monthly_partiiton_data_demo"
    val monthlyTableBasePath = "file:///Users/krishna/Downloads/hudidb/monthly_partiiton_data_demo"
    df.write.format("hudi").
    options(getQuickstartWriteConfigs).
    option(PRECOMBINE_FIELD_OPT_KEY, "id").
    option(RECORDKEY_FIELD_OPT_KEY, "id").
    option(PARTITIONPATH_FIELD_OPT_KEY, "_partition").
    option(TABLE_NAME, monhtlyTableName).
    mode(Overwrite).
    save(monthlyTableBasePath)

![Data Storage Visualization](/data/apache-hudi-partitionpath-demo/monthly_partition_data_demo.png)

Now, let's see yearly partition demonstrations as:

    val yearlyPartitionFormat = "yyyy"
    df = df.withColumn(partitonColumn, date_format(col("created_at"), yearlyPartitionFormat))
      scala> df.show
      +---+-------+-------------------+----------+
      | id|   name|         created_at|_partition|
      +---+-------+-------------------+----------+
      |  1|Krishna|2024-01-01 00:00:00|      2024|
      |  2| Prasad|2024-02-20 00:00:00|      2024|
      |  3|  Veena|2024-03-25 00:00:00|      2024|
      |  4|Krishvi|2024-02-12 00:00:00|      2024|
      +---+-------+-------------------+----------+
    val yearlyTableBasePath = "file:///Users/krishna/Downloads/hudidb/yearly_partiiton_data_demo"
    val yearlyTableName = "yearly_partiiton_data_demo"
    df.write.format("hudi").
    options(getQuickstartWriteConfigs).
    option(PRECOMBINE_FIELD_OPT_KEY, "id").
    option(RECORDKEY_FIELD_OPT_KEY, "id").
    option(PARTITIONPATH_FIELD_OPT_KEY, "_partition").
    option(TABLE_NAME, yearlyTableName).
    mode(Overwrite).
    save(yearlyTableBasePath)

![Data Storage Visualization](/data/apache-hudi-partitionpath-demo/yearly_partiiton_data_demo.png)


The simple design follows on the AWS Glue pipeline:

HLD:
![HLD](/data/apache-hudi-partitionpath-demo/SimpleDesignOfThisDemo.png)

LLD:
![LLD](/data/apache-hudi-partitionpath-demo/ApacheHudiPartitionDemo.png)


# Scala: Start Apache Hudi with Apache Spark + Scala on local machine

## starting Scala Spark Shell 3 with Hudi
> spark-shell — packages org.apache.hudi:hudi-spark3-bundle_2.12:0.14.1,org.apache.spark:spark-avro_2.12:3.0.1 — conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'

    val data = Seq(("1", "sue", 32), ("2", "li", 3),("3", "bob", 75), ("4", "heo", 13))
    val schema = Seq("id" ,"first_name", "age")
    
    val rdd = spark.sparkContext.parallelize(data)
    
    import spark.implicits._
    
    val df = rdd.toDF(schema: _*)
    
    df.printSchema
    df.show
    
    val partitionLabel = "_partition"
    # add partitin column "_partition", only two partition as of now
    val df2 = df.withColumn(partitionLabel, col("age") % 2)
    
    val tableName = "test_hudi_cow"
    val basePath = "/tmp/examples/hudi/data/test-hudi-cow"
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
    
    df2.write.format("hudi").options(hudi_options).mode("overwrite").save(basePath)

### Below spark-shell has some issues
> spark-shell — packages org.apache.hudi:hudi-spark3-bundle_2.12:0.14.1,org.apache.spark:spark-avro_2.12:3.0.1 — conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'

    scala> df2.write.format("hudi").options(hudi_options).mode("overwrite").save(basePath)
    24/01/27 11:38:56 WARN DFSPropertiesConfiguration: Cannot find HUDI_CONF_DIR, please set it as the dir of hudi-defaults.conf
    24/01/27 11:38:56 WARN DFSPropertiesConfiguration: Properties file file:/etc/hudi/conf/hudi-defaults.conf not found. Ignoring to load props file
    24/01/27 11:38:56 WARN DataSourceOptionsHelper$: hoodie.datasource.write.storage.type is deprecated and will be removed in a later release; Please use hoodie.datasource.write.table.type
    24/01/27 11:38:56 WARN DataSourceOptionsHelper$: hoodie.datasource.write.storage.type is deprecated and will be removed in a later release; Please use hoodie.datasource.write.table.type
    org.apache.hudi.exception.HoodieException: hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer
    at org.apache.hudi.HoodieSparkSqlWriterInternal.writeInternal(HoodieSparkSqlWriter.scala:265)
    at org.apache.hudi.HoodieSparkSqlWriterInternal.write(HoodieSparkSqlWriter.scala:204)
    at org.apache.hudi.HoodieSparkSqlWriter$.write(HoodieSparkSqlWriter.scala:121)
    at org.apache.hudi.DefaultSource.createRelation(DefaultSource.scala:150)

#### Notes: Above command has issues of `— packages`
###Let's with below
>spark-shell --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.14.1,org.apache.spark:spark-avro_2.12:3.0.1 — conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'

    val data = Seq(("1", "sue", 32), ("2", "li", 3),("3", "bob", 75), ("4", "heo", 13))
    val schema = Seq("id" ,"first_name", "age")
    
    val rdd = spark.sparkContext.parallelize(data)
    
    import spark.implicits._
    
    val df = rdd.toDF(schema: _*)
    
    df.printSchema
    df.show
    
    
    val partitionLabel = "_partition"
    # add partitin column "_partition", only two partition as of now
    
    import org.apache.spark.sql.functions.{col, when, _}
    
    val df2 = df.withColumn("_partition", when( col("age") % 2 === 0, "even").otherwise("old") )
    
    val tableName = "test_hudi_cow"
    val basePath = "/tmp/examples/hudi/data/test-hudi-cow"
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
    
    df2.write.format("hudi").options(hudi_options).mode("overwrite").save(basePath)

    scala> df2.write.format("hudi").options(hudi_options).mode("overwrite").save(basePath)
    24/01/27 11:38:56 WARN DFSPropertiesConfiguration: Cannot find HUDI_CONF_DIR, please set it as the dir of hudi-defaults.conf
    24/01/27 11:38:56 WARN DFSPropertiesConfiguration: Properties file file:/etc/hudi/conf/hudi-defaults.conf not found. Ignoring to load props file
    24/01/27 11:38:56 WARN DataSourceOptionsHelper$: hoodie.datasource.write.storage.type is deprecated and will be removed in a later release; Please use hoodie.datasource.write.table.type
    24/01/27 11:38:56 WARN DataSourceOptionsHelper$: hoodie.datasource.write.storage.type is deprecated and will be removed in a later release; Please use hoodie.datasource.write.table.type
    org.apache.hudi.exception.HoodieException: hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer
    at org.apache.hudi.HoodieSparkSqlWriterInternal.writeInternal(HoodieSparkSqlWriter.scala:265)
    at org.apache.hudi.HoodieSparkSqlWriterInternal.write(HoodieSparkSqlWriter.scala:204)
    at org.apache.hudi.HoodieSparkSqlWriter$.write(HoodieSparkSqlWriter.scala:121)
    at org.apache.hudi.DefaultSource.createRelation(DefaultSource.scala:150)


# For Spark versions: 3.0 - 3.1
export SPARK_VERSION=3.1
spark-shell --packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.1 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'

-----

> spark-shell --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.14.1,org.apache.spark:spark-avro_2.12:3.0.1 — conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'


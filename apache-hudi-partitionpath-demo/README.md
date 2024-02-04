# Apache Hudi Partition Demo on local machine

Download and install spark-3.3.1

and follow from the below Quick Start guide
>https://hudi.apache.org/docs/0.13.1/quick-start-guide/

## Spark 3.3
./bin/spark-shell \
  --packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1 \
  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
  --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
  --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
  --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'


<!-- val basePath = "file:///Users/krishna/Downloads/hudidb" -->


// spark-shell
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

### Exmine the partition path and values
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

## Now on AWS with parition based on Date column and parition can be done as Daily, weekly, monthly:

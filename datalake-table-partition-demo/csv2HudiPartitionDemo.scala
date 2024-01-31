import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.hive.HiveSyncConfig

import scala.collection.JavaConverters._


object csv2HudiPartitionDemo {

  /**
  * This code is only for demo, so I am writing everything in single scala object so 
  * that it has minimum code to understand quickly, and anyway this is only for demo 
  */
  def huidLoad(df:DataFrame, 
                partitionColumn:String, 
                partitionFormat:String, 
                destDbName:String,
                hudiLoadS3Path: String, 
                destTableName:String, 
                recordkey:String): Unit = {
       val PartitionLabel:String = "_partition"
        // Convert the existing column to timestamp which will be used for table partitioning
        // Used CSV file has yyyy/MM/dd date format column in partitionColumn as String so changes needed
        var tdf = df.withColumn(partitionColumn, to_timestamp(col(partitionColumn), "yyyy/MM/dd"))
        
        // Add new Column as partiton column
        tdf = tdf.withColumn(PartitionLabel, date_format(col(partitionColumn), partitionFormat))
        
        // Make toLowerCase for all the columns -> Optional
        tdf = tdf.columns.foldLeft(tdf) { 
            (tmpDf, colName) => tmpDf.withColumnRenamed(colName, colName.toLowerCase)
        }
        
        val options = Map(
          "hoodie.table.name" -> destTableName,
          "hoodie.datasource.write.storage.type" -> "COPY_ON_WRITE",
          "hoodie.datasource.write.operation" -> "upsert",
          "hoodie.datasource.write.recordkey.field" -> recordkey.toLowerCase,
          "hoodie.datasource.write.precombine.field" -> recordkey.toLowerCase,
          "hoodie.datasource.write.partitionpath.field" -> PartitionLabel,
          "hoodie.datasource.write.hive_style_partitioning" -> "true",
          "hoodie.datasource.hive_sync.partition_fields" -> PartitionLabel,
          "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.MultiPartKeysValueExtractor",
          "hoodie.datasource.hive_sync.enable" -> "true",
          "hoodie.datasource.hive_sync.database" -> destDbName,
          "hoodie.datasource.hive_sync.table" -> destTableName,
          "hoodie.datasource.hive_sync.use_jdbc" -> "false",
          "hoodie.datasource.hive_sync.mode" -> "hms",
        )
        
        tdf.write.format("hudi")
          .options(options)
          .mode(SaveMode.Overwrite)
          .save(hudiLoadS3Path)
        
  }

  def main(sysArgs: Array[String]) = {
      
    val spark: SparkContext = new SparkContext()
    
    val glueContext: GlueContext = new GlueContext(spark)
    
    val args = GlueArgParser.getResolvedOptions(
        sysArgs, 
        Seq(
            "JOB_NAME",
            "source.s3.path",
            "destination.s3.path",
            "destination.database.name",
            "destination.table.name",
            "record.key.column",
            "partition.column",
            "partition.format",
            ).toArray
        )
    
    val sparkSession: SparkSession = glueContext.getSparkSession
    
    sparkSession.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
    
    import sparkSession.implicits._
    
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
        
    val csvSourceS3Path = args("source.s3.path")
    val hudiLoadS3Path = args("destination.s3.path")
    val destDbName = args("destination.database.name")
    val destTableName = args("destination.table.name")
    val recordkey = args("record.key.column")
    val partitionColumn = args("partition.column") // Should be DATE, DATETIME, TIMESTAMP
    val partitionFormat = args("partition.format")

    try {
      var df = sparkSession.read
            .option("header","true")
            .format("csv")
            .load(csvSourceS3Path)
      
      // Write into Apache Hudi table with partitionFormat as "yyyy", "yyyy-MM" and "yyyy-MM-dd"
      huidLoad(df, partitionColumn, partitionFormat, destDbName, hudiLoadS3Path, destTableName, recordkey)

      Job.commit()
    } finally {
      spark.stop()
    }
  }
}


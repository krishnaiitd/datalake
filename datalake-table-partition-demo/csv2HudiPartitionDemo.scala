import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.hive.HiveSyncConfig

import scala.collection.JavaConverters._


object csv2HudiPartitionDemo {

  def storeAsPartition(df:org.apache.spark.sql.DataFrame, partitionColumn:String, partitionFormat:String): Unit = {
       
       val PartitionLabel:String = "_partition"
        // Convert the exist column to timestamp which will be used for partitionColumn
        df = df.withColumn(partitionColumn, to_timestamp(col(partitionColumn), "yyyy/MM/dd"))
        
        // assign the partiton column value to new Column with defined partition format
        df = df.withColumn(PartitionLabel, date_format(col(partitionColumn), partitionFormat))
        
        // make toLowerCase for all the columns
        df = df.columns.foldLeft(df) { 
            (tmpDf, colName) => tmpDf.withColumnRenamed(colName, colName.toLowerCase)
        }
        
        val options = Map(
          "hoodie.table.name" -> destinationTableName,
          "hoodie.datasource.write.storage.type" -> "COPY_ON_WRITE",
          "hoodie.datasource.write.operation" -> "upsert",
          "hoodie.datasource.write.recordkey.field" -> recordkey.toLowerCase,
          "hoodie.datasource.write.precombine.field" -> recordkey.toLowerCase,
          "hoodie.datasource.write.partitionpath.field" -> PartitionLabel,
          "hoodie.datasource.write.hive_style_partitioning" -> "true",
          "hoodie.datasource.hive_sync.enable" -> "true",
          "hoodie.datasource.hive_sync.database" -> destinationDatabaseName,
          "hoodie.datasource.hive_sync.table" -> destinationTableName,
          "hoodie.datasource.hive_sync.partition_fields" -> PartitionLabel,
          "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.MultiPartKeysValueExtractor",
          "hoodie.datasource.hive_sync.use_jdbc" -> "false",
          "hoodie.datasource.hive_sync.mode" -> "hms",
        )
        
        df.write.format("hudi")
          .options(options)
          .mode(SaveMode.Overwrite)
          .save(destinationS3Path)
        
  }



  def main(sysArgs: Array[String]) = {
      
    val spark: SparkContext = new SparkContext()
    
    val glueContext: GlueContext = new GlueContext(spark)
    
    val args = GlueArgParser.getResolvedOptions(
        sysArgs, 
        Seq(
            "JOB_NAME",
            "temp.s3.path",
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
    
    print(args)
    
    val sourceS3Path = args("source.s3.path")
    val destinationS3Path = args("destination.s3.path")
    val tempS3Path = args("temp.s3.path")
    val destinationTableName = args("destination.table.name")
    val destinationDatabaseName = args("destination.database.name")

    val recordkey = args("record.key.column")
    val partitionColumn = args("partition.column")
    val partitionFormat = args("partition.format")
    
    print("recordkey: " + recordkey)
    print("partitionColumn: " +  partitionColumn)
    print("partitionFormat: " + partitionFormat)
    
    try {
      var df = sparkSession.read
            .option("header","true")
            .format("csv")
            .load(sourceS3Path)
      
      storeAsPartition(df, partitionColumn, partitionFormat)
      
      print(df.printSchema)      
      print(df.show)
      print(df.count())
      
      Job.commit()
    } finally {
      spark.stop()
    }
  }
}


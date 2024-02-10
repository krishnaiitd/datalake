//import com.amazonaws.services.glue.GlueContext
//import com.amazonaws.services.glue.MappingSpec
//import com.amazonaws.services.glue.errors.CallSite
//import com.amazonaws.services.glue.util.GlueArgParser
//import com.amazonaws.services.glue.util.Job
//import com.amazonaws.services.glue.util.JsonOptions
//import org.apache.spark.SparkContext
//import scala.collection.JavaConverters._
//
//
//import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
//import org.apache.hudi.config.HoodieWriteConfig
//
////object GlueApp {
//object AwsGlueS3ToParquetETL {
//  def main(sysArgs: Array[String]) {
//    val spark: SparkContext = new SparkContext()
//    val glueContext: GlueContext = new GlueContext(spark)
//    // @params: [JOB_NAME]
//    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
//    Job.init(args("JOB_NAME"), glueContext, args.asJava)
//    // Script generated for node Amazon S3
//    val AmazonS3_node1705714279753 = glueContext.getSourceWithFormat(formatOptions=JsonOptions("""{"quoteChar": "\"", "withHeader": true, "separator": ","}"""), connectionType="s3", format="csv", options=JsonOptions("""{"paths": ["s3://krishnaprasad/data-engineering-design/employees.csv"], "recurse": true}"""), transformationContext="AmazonS3_node1705714279753").getDynamicFrame()
//
//    // Script generated for node Change Schema
//    val ChangeSchema_node1705715042829 = AmazonS3_node1705714279753.applyMapping(mappings=Seq(("employee_id", "string", "employee_id", "int"), ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("email", "string", "email", "string"), ("phone_number", "string", "phone_number", "string"), ("hire_date", "string", "hire_date", "string"), ("job_id", "string", "job_id", "string"), ("salary", "string", "salary", "double"), ("commission_pct", "string", "commission_pct", "string"), ("manager_id", "string", "manager_id", "long"), ("department_id", "string", "department_id", "long")), caseSensitive=false, transformationContext="ChangeSchema_node1705715042829")
//
//    // Script generated for node Amazon S3
//    val AmazonS3_node1705715300296 = glueContext.getSinkWithFormat(connectionType="s3", options=JsonOptions("""{"path": "s3://apache-hudi-datalake/bronze/", "partitionKeys": ["hire_date"], "enableUpdateCatalog": true, "updateBehavior": "UPDATE_IN_DATABASE"}"""), formatOptions=JsonOptions("""{"compression": "snappy"}"""), transformationContext="AmazonS3_node1705715300296", format="glueparquet")
//    AmazonS3_node1705715300296.setCatalogInfo(catalogDatabase="bronze", catalogTableName="employee")
//    AmazonS3_node1705715300296.writeDynamicFrame(ChangeSchema_node1705715042829)
//    Job.commit()
//  }
//}
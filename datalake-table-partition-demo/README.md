# How to run

You can Copy and Paste the script [csv2HudiPartitionDemo.scala](datalake-table-partition-demo/csv2HudiPartitionDemo.scala) into your AWS Glue editor and setup the Glue job with the below parameters in the AWS Glue Job Details section:

1. Type: Spark
2. Glue Version: Glue 4.0 - Supports spark 3.3, Scala 2, Python 3
3. Language: Scala
4. Worker type: G.1x (or as your requirements or data size)
5. Dependent JARs path: s3://<your_s3_bucket>/<prefix>/hudi-spark3.3-bundle_2.12-0.14.1.jar
6. Job Parameters
   1. `--class`  csv2HudiPartitionDemo
   2. `--conf` spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false
   3. `--destination.database.name`  bronze
   4. `--destination.s3.path` s3://<your_bucket>/bronze/medium-post-examples/apache-hudi/daily_partition_employe
   5. `--destination.table.name`  daily_partition_employe // or monthly/ yearly
   6. `--partition.column`  HIRE_DATE
   7. `--partition.format`  yyyy-MM-dd
   8. `--record.key.column`   EMPLOYEE_ID
   9. `--source.s3.path`  s3://<your_s3_data_bucket>/employees.csv

![AWS Glue Job parameters](/data/csv2HudiPartitionDemoParameters.png)

## Link to get the dependent jar which required to pass on the AWS Glue Dependent jars path

For Sbt dependency: https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.3-bundle_2.12/0.14.0

For directly downloading and uploading to AWS S3: https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.3-bundle_2.12/0.14.1/

Download this one: hudi-spark3.3-bundle_2.12-0.14.1.jar

Also, Adding the data I have used for this demonstration i.e inside [/data/employee.csv](https://github.com/krishnaiitd/datalake/blob/main/data/employees.csv)


## Experiments 
Try with different parameters of _partition format along with destination table name and S3 storage path and then create multiple table to Query on AWS Athena with various where clause on partition column, without partition column to visualized the data retrieval details in Query Stats of AWS Athena.

### Query with partititon column in where clause:
   -- Query with Partition Column 
   select * from "yearly_partition_employe" where _partition='2023'
[Query Stats](/dta/queryOnePartitionWithPartitionColumn.png)

### Query with partititon column in where clause:
   -- Query without Partition Column 
   select * from "yearly_partition_employe" where YEAR(FROM_UNIXTIME(hire_date/1000000)) = 2023

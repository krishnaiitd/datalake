# How to run

Copy and paste the script [aws_glue_script.scala](https://github.com/krishnaiitd/datalake/blob/main/bootstrapping-data-lake-with-minimum-code/aws_glue_script.scala) into AWS Glue editor and setup the below parameters in the AWS Glue Job Details section:

1. Type: Spark
2. Glue Version: Glue 4.0 - Supports spark 3.3, Scala 2, Python 3
3. Language: Scala
4. Worker type: G.1x (or as your requirements or data size)
5. Dependent JARs path: s3://<your_s3_bucket>/<prefix>/hudi-spark3.3-bundle_2.12-0.14.1.jar
6. Job Parameters
   1. `--class`  S3Csv2Hudi
   2. `--conf` spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false
   3. `--destination.database.name`  bronze
   4. `--destination.s3.path`  s3://<your_bucket>/bronze/hudi/employee
   5. `--destination.table.name`  hudi_employee
   6. `--partition.column`  HIRE_DATE
   7. `--partition.format`  yyyy
   8. `--record.key.column`   EMPLOYEE_ID
   9. `--source.s3.path`  s3://<your_s3_data_bucket>/employees.csv
   10. `--temp.s3.path`  s3://aws-glue-assets-******-us-east-1/temporary/

## Link to get the dependent jar which required to pass on the AWS Glue Dependent jars path

For Sbt dependency: https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark3.3-bundle_2.12/0.14.0

For directly downloading and uploading to AWS S3: https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.3-bundle_2.12/0.14.1/

Download this one: hudi-spark3.3-bundle_2.12-0.14.1.jar

Also, Adding the data I have used for this demonstration i.e inside [/data/employee.csv](https://github.com/krishnaiitd/datalake/blob/main/data/employees.csv)

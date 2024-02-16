To Compile

sbt compile


To run on terminal

spark-submit --master local --driver-memory 2g --executor-memory 2g --class SimpleSparkApp target/scala-2.12/mysparkproject_2.12-1.0.jar
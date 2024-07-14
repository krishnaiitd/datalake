#Requirements: 
java 1.8
scala 1.12.19
spark version >= 3.3.1

To Compile
mvn clean install


To run on terminal

# HelloWorld : Main
spark-submit --master local --driver-memory 2g --executor-memory 2g --class spark.java.demo.Main target/spark-java-demo-1.0-SNAPSHOT.jar

# For SparkHelloWorld
spark-submit --master local --driver-memory 2g --executor-memory 2g --class spark.java.demo.SparkHelloWorld target/spark-java-demo-1.0-SNAPSHOT.jar

# For ParquetPartitionDemo
spark-submit --master local --driver-memory 2g --executor-memory 2g --class spark.java.demo.ParquetPartitionDemo target/spark-java-demo-1.0-SNAPSHOT.jar
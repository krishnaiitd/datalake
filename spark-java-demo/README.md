# Dependencies:

## java version
    ➜  spark-demo git:(main) ✗ java -version                                 
    openjdk version "1.8.0_412"
    OpenJDK Runtime Environment Corretto-8.412.08.1 (build 1.8.0_412-b08)
    OpenJDK 64-Bit Server VM Corretto-8.412.08.1 (build 25.412-b08, mixed mode)

## Spark Version
    version >= 3.3.1


## To Compile
    mvn clean install


## To run on terminal

#### HelloWorld : Main
    spark-submit --master local --driver-memory 2g --executor-memory 2g --class spark.java.demo.Main target/spark-java-demo-1.0-SNAPSHOT.jar

#### For SparkHelloWorld
    spark-submit --master local --driver-memory 2g --executor-memory 2g --class spark.java.demo.SparkHelloWorld target/spark-java-demo-1.0-SNAPSHOT.jar

#### For ParquetPartitionDemo
    spark-submit --master local --driver-memory 2g --executor-memory 2g --class spark.java.demo.ParquetPartitionDemo target/spark-java-demo-1.0-SNAPSHOT.jar
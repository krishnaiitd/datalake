# Dependencies:

## java version

    ➜  spark-demo git:(main) ✗ java -version                                 
    openjdk version "1.8.0_412"
    OpenJDK Runtime Environment Corretto-8.412.08.1 (build 1.8.0_412-b08)
    OpenJDK 64-Bit Server VM Corretto-8.412.08.1 (build 25.412-b08, mixed mode)
## Scala Version

    ➜  spark-demo git:(main) ✗ scala -version
    Scala code runner version 2.12.19 -- Copyright 2002-2024, LAMP/EPFL and Lightbend, Inc.

## Spark Version
    version >= 3.3.1


## To Compile
    sbt compile

## To create Fat Jar
    sbt assembly 

## To run on terminal or submit as a Spark job

    spark-submit --master local --driver-memory 2g --executor-memory 2g --class SimpleSparkApp target/scala-2.12/SparkDemo-assembly-1.0.jar

#!/bin/sh

#echo "Install scala"
#sudo wget www.scala-lang.org/files/archive/scala-2.11.8.deb
#sudo dpkg -i scala-2.11.8.deb
#sudo apt-get update
#sudo apt-get install scala
#rm scala-2.11.8.deb

echo "Changing the PATH for Yahoo Benchmark..."
cd ../yahoo-streaming-benchmark/

echo "Building query..."
mvn clean package
cp target/spark-bench-1.0-SNAPSHOT.jar ../spark

echo "Running Benchmark for Spark..."
cd ../spark
java -cp spark-bench-1.0-SNAPSHOT.jar:/home/$USER/.m2/repository/org/apache/spark/spark-sql-kafka-0-10_2.11/2.3.0/spark-sql-kafka-0-10_2.11-2.3.0.jar uk.ac.ic.imperial.StreamBenchMain


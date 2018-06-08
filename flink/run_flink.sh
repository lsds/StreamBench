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
cp target/spark-bench-1.0-SNAPSHOT.jar ../flink


echo "Running Benchmark for Flink..."
cd ../flink
java -cp spark-bench-1.0-SNAPSHOT.jar uk.ac.ic.imperial.benchmark.flink.YahooBenchmark


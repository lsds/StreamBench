#!/bin/sh

echo "Cloning from SABER the branch for Yahoo Benchmark..."
git clone -b StreamBench https://github.com/lsds/Saber.git

echo "Installing Java 8..."
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
export JAVA_HOME=/usr/lib/jvm/java-8-oracle

echo "Building SABER..."
cd Saber/
export SABER_HOME=`pwd`
./build.sh

echo "Building C code..."
cd clib/
make cpu

cd ..

echo "Running Benchmark..."
./run.sh uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.YahooBenchmarkApp

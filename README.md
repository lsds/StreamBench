# StreamBench
StreamBench is a collection of scripts to measure the performance of popular streaming engines using Yahoo Streaming Benchmark.

## Overview
We compare the performance of an efficient stream processing engine designed for single servers, SABER, with that achieved by popular distributed stream processing systems, Apache Spark and Apache Flink. We also compare the results to that by StreamBox, another recently proposed single-server design that emphases out-of-order processing of data. Based on our results, we argue that a single multicore server can provide better throughput than a multi-node cluster for many streaming applications. This opens an opportunity to cut down system complexity and operational costs by replacing cluster-based stream processing systems with (potentially replicated) single server deployments.

This repository contains our scripts for running the Yahoo Streaming Benchmark in SABER, Spark Streaming, Apache Flink and StreamBox. For Spark and Flink, we follow the approach from previous blogposts (https://databricks.com/blog/2017/10/11/benchmarking-structured-streaming-on-databricks-runtime-against-state-of-the-art-streaming-systems.html and https://data-artisans.com/blog/curious-case-broken-benchmark-revisiting-apache-flink-vs-databricks-runtime). We provide a script for each of these engines to setup and run the benchmark on a single node. Our code can be expanded to run on a distributed deployment.

## Benchmark Outline
The Yahoo Streaming Benchmark was designed to emulate an advertisement streaming application. It has a streaming query with four operators: filter, project, join (with relational data) and aggregate (a windowed count).

## How to run the code
For every engine, the script provided installs, builds and runs the Yahoo Streaming Benchmark.

## Credits
StreamBench is brought to you by George Theodorakis, Panagiotis Garefalakis, Alexandros Koliousis, Holger Pirk, Peter Pietzuch


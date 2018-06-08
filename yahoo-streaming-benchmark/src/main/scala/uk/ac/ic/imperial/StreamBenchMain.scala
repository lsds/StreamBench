package uk.ac.ic.imperial

import uk.ac.ic.imperial.benchmark.spark.{SparkHelper, SparkYahooRunner}
import uk.ac.ic.imperial.benchmark.utils.{LocalKafka, LocalFlink}
import uk.ac.ic.imperial.benchmark.yahoo.YahooBenchmark
import uk.ac.ic.imperial.benchmark.flink.{FlinkYahooRunner}


/**
  * Streaming benchmark Main entry point
  *
  *   Goals of the benchmark:
  *     * Explore different streaming systems
  *     * Discover bottlenecks and potential solutions
  *     * Explore system scalability
  *
  */
object StreamBenchMain {
  // Benchmark Configurations - Ideal for Community Edititon
  val stopSparkOnKafkaNodes = false
  val stopSparkOnFlinkNodes = false
  val numKafkaNodes = 1
  val numFlinkNodes = 1
  var flinkTaskSlots = 1 // Number of CPUs available for a taskmanager.
  var runDurationMillis = 100000 // How long to keep each stream running
  var numTrials = 3
  val benchmarkResultsBase = "streaming/benchmarks"//"/mnt/LSDSUserShare/homes/grt17/streaming/benchmarks" // Where to store the results of the benchmark

  //////////////////////////////////
  // Event Generation
  //////////////////////////////////
  var recordsPerSecond = 2000000
  val rampUpTimeSeconds = 10 // Ramps up event generation to the specified rate for the given duration to allow the JVM to warm up
  var recordGenerationParallelism = 1 // Parallelism within Spark to generate data for the Kafka Streams benchmark

  val numCampaigns = 100 // The number of campaigns to generate events for. Configures the cardinality of the state that needs to be updated

  val kafkaEventsTopicPartitions = 1 // Number of partitions within Kafka for the `events` stream
  val kafkaOutputTopicPartitions = 1 // Number of partitions within Kafka for the `outout` stream. We write data out to Kafka instead of Redis

  //////////////////////////////////
  // Kafka Streams
  //////////////////////////////////
  // Total number of Kafka Streams applications that will be running.
  val kafkaStreamsNumExecutors = 1
  // Number of threads to use within each Kafka Streams application.
  val kafkaStreamsNumThreadsPerExecutor = 1

  //////////////////////////////////
  // Flink
  //////////////////////////////////
  // Parallelism to use in Flink. Needs to be <= numFlinkNodes * flinkTaskSlots
  var flinkParallelism = 1
  // We can have Flink emit updates for windows more frequently to reduce latency by sacrificing throughput. Setting this to 0 means that
  // we emit updates for windows according to the watermarks, i.e. we will emit the result of a window, once the watermark passes the end
  // of the window.
  val flinkTriggerIntervalMillis = 0
  // How often to log the throughput of Flink in #records. Setting this lower will give us finer grained results, but will sacrifice throughput
  val flinkThroughputLoggingFreq = 100000

  //////////////////////////////////
  // Spark
  //////////////////////////////////
  var sparkParallelism = 1

  /////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////////////////////////////////

  def main(args: Array[String]): Unit = {
    
    var i = 0
    var j = 0
		while (i < args.length ) {
		  j = i + 1
			if (j == args.length) {
				System.err.println("Wrong number of arguments");
				System.exit(1);
			}
			if (args(i).equals("--runDurationMillis")) {
				runDurationMillis = Integer.parseInt(args(j));
			} else
			if (args(i).equals("--numTrials")) { 
				numTrials = Integer.parseInt(args(j));
			} else
			if (args(i).equals("--recordGenerationParallelism")) { 
				recordGenerationParallelism = Integer.parseInt(args(j));
			} else
			if (args(i).equals("--sparkParallelism")) { 
				sparkParallelism = Integer.parseInt(args(j));
			} else
			if (args(i).equals("--recordsPerSecond")) { 
				recordsPerSecond = Integer.parseInt(args(j));
			} else {
				System.err.println(String.format("error: unknown flag %s %s", args(i), args(j)));
				System.exit(1);
			}
			i = j + 1;
		}

    val spark = SparkHelper.getAndConfigureSparkSession()
    val kafkaCluster = LocalKafka.setup(spark, stopSparkOnKafkaNodes = stopSparkOnKafkaNodes, numKafkaNodes = numKafkaNodes)

    val benchmark = new YahooBenchmark(
      spark,
      kafkaCluster,
      tuplesPerSecond = recordsPerSecond,
      recordGenParallelism = recordGenerationParallelism,
      rampUpTimeSeconds = rampUpTimeSeconds,
      kafkaEventsTopicPartitions = kafkaEventsTopicPartitions,
      kafkaOutputTopicPartitions = kafkaOutputTopicPartitions,
      numCampaigns = numCampaigns,
      readerWaitTimeMs = runDurationMillis)


    val sparkRunner = new SparkYahooRunner(
      spark,
      kafkaCluster = kafkaCluster,
      parallelism = sparkParallelism)

    benchmark.run(sparkRunner, s"$benchmarkResultsBase/spark_$sparkParallelism"+"_cores", numRuns = numTrials)
    
    
    /*val flinkCluster = LocalFlink.setup(
      spark,
      stopSparkOnFlinkNodes = stopSparkOnFlinkNodes,
      numFlinkNodes = numFlinkNodes,
      numTaskSlots = flinkTaskSlots)

    val flinkRunner = new FlinkYahooRunner(
      spark,
      flinkCluster = flinkCluster,
      kafkaCluster = kafkaCluster,
      parallelism = flinkParallelism,
      triggerIntervalMs = flinkTriggerIntervalMillis,
      logFreq = flinkThroughputLoggingFreq)

    benchmark.run(flinkRunner, s"$benchmarkResultsBase/flink", numRuns = numTrials)*/
    

    kafkaCluster.stopAll()
  }
}
package uk.ac.ic.imperial.benchmark.flink

import uk.ac.ic.imperial.benchmark.utils.{LocalKafka, LocalFlink}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, _}
import uk.ac.ic.imperial.benchmark.utils.LocalKafka
import uk.ac.ic.imperial.benchmark.yahoo.YahooBenchmarkRunner

/**
 * Runner for the Yahoo Benchmark using Apache Flink.
 * @param parallelism The parallelism to run the Yahoo Benchmark at. Needs to be smaller than or equal to
 *                    `flinkCluster.numTaskSlots * flinkCluster.numFlinkNodes`.
 * @param triggerIntervalMs How often to emit updates for the windowed counts
 * @param logFreq The logging frequency for throughput. Lower values will give more accurate throughput calculations
 *                but may sacrifice throughput in the process.
 */
class FlinkYahooRunner(
    override val spark: SparkSession,
    flinkCluster: LocalFlink,
    kafkaCluster: LocalKafka,
    parallelism: Int,
    triggerIntervalMs: Int,
    logFreq: Int = 10000000) extends YahooBenchmarkRunner {
  
  import scala.sys.process._
  import org.apache.spark.sql._
  import org.apache.spark.sql.streaming._
  import StreamingQueryListener._
  import uk.ac.ic.imperial.benchmark.yahoo._
  
  require(parallelism >= 1, "Parallelism can't be less than 1")
  require(triggerIntervalMs >= 0, "The trigger interval needs to be greater than or equal to 0")
  require(logFreq >= 1, "The logging frequency needs to be greater than 0")
  
  override def start(): Unit = {
    Thread.sleep(1000000000L)
  }
  
  override def generateData(
      campaigns: Array[CampaignAd],
      tuplesPerSecond: Long,
      recordGenParallelism: Int,
      rampUpTimeSeconds: Int): Unit = {
    flinkCluster.runJob("uk.ac.ic.imperial.benchmark.flink.YahooBenchmark",
      "--bootstrap.servers", kafkaCluster.kafkaNodesString,
      "--parallelism", parallelism.toString,
      "--tuplesPerSecond", tuplesPerSecond.toString,
      "--numCampaigns", campaigns.length.toString,
      "--rampUpTimeSeconds", rampUpTimeSeconds.toString,
      "--triggerIntervalMs", triggerIntervalMs.toString,
      "--logFreq", logFreq.toString,
      "--outputTopic", Variables.OUTPUT_TOPIC)
  }
  
  override def stop(): Unit = {
    val jobs = "flink/bin/flink list -r".!!.split("\n").find(_.contains("Flink Yahoo Benchmark"))
    if (jobs.isDefined) {
      s"flink/bin/flink cancel ${jobs.get.split(" : ")(1).trim}".!!
    }
  }
  
  override val params: Map[String, Any] = Map("parallelism" -> parallelism, "triggerIntervalMs" -> triggerIntervalMs)
  
  private val LOG_FORMAT = "ThroughputLogging:(\\d*),(\\d*)".r

  private case class PerNodeStats(start: Long, end: Long, count: Long)  
  
  /**
   * Throughput calculation in Flink works as follows:
   *   1. The `ThroughputLogger` periodically logs the number of records per task and the current time to the taskmanager stdout logs
   *   2. We iterate through the stdout logs to find the last run. The zeros mark the start the start of a new run. The number of tasks
   *      is equal to the number of zeros we observe for this run.
   *   3. We then take the last timestamp and highest number of records processed for that run, and multiply by the number of tasks to
   *      get the number of records processed.
   *   4. We merge these numbers across Flink taskmanagers to get the total number of records processed, earliest timestamp (job start),
   *      and latest timestamp (job end) to calculate the throughput.
   *
   * The throughput calculation is therefore an approximation and the error on the number of records processed
   * is strictly less than `logFreq * parallelism`.
   */
  override def getThroughput(): DataFrame = {
    import org.apache.spark.sql.types._
    import spark.sql
    import spark.sqlContext.implicits._
    import org.apache.spark.sql._
    import org.apache.spark.sql.streaming._
    import StreamingQueryListener._
    import uk.ac.ic.imperial.benchmark.yahoo._

    val perNode = flinkCluster.taskManagers.map { node =>
      val log = if (flinkCluster.isJobManagerOnDriver) {
        val fileName = "ls flink/log/".!!.split("\n").find(_.endsWith(".out")).get
        s"cat flink/log/$fileName".!!.split("\n")
      } else {
        flinkCluster.ssh(node, "cat flink/log/flink-george-taskmanager-*.out", logStdout = false).split("\n")
      }
      var highestSeen = 0L
      var latestTimestamp = 0L
      var earliestTimestamp = -1L
      var numTasks = 0
      log.foreach { case LOG_FORMAT(timestampStr, countStr) =>
        val timestamp = timestampStr.toLong
        val count = countStr.toLong
        if (count == 0) {
          if (highestSeen > 0 || earliestTimestamp == -1L) {
            // this is a new run
            highestSeen = 0
            earliestTimestamp = timestamp
            numTasks = 1
          } else if (highestSeen == 0) {
            numTasks += 1
          }
        }
        if (timestamp > latestTimestamp) {
          latestTimestamp = timestamp
        }
        if (count > highestSeen) {
          highestSeen = count
        }
      }
      PerNodeStats(earliestTimestamp, latestTimestamp, highestSeen * numTasks)
    }
    val aggregated = perNode.reduce { (stats1, stats2) =>
      PerNodeStats(Math.min(stats1.start, stats2.start), Math.max(stats1.end, stats2.end), stats1.count + stats2.count)
    }
    val duration = aggregated.end - aggregated.start
    Seq((
        new java.sql.Timestamp(aggregated.start),
        new java.sql.Timestamp(aggregated.end),
        aggregated.count,
        duration,
        aggregated.count * 1000.0 / duration)).toDS()
      .toDF("start", "end", "totalInput", "totalDurationMillis", "throughput")
  }
  
  /**
   * We calculate the latency as the difference between the Kafka ingestion timestamp for a given `time_window` and `campaign_id`
   * pair and the event timestamp of the latest record generated that belongs to that bucket.
   */
  override def getLatency(): DataFrame = {
    import org.apache.spark.sql.types._
    import spark.sql
    import spark.sqlContext.implicits._
    import org.apache.spark.sql._
    import org.apache.spark.sql.streaming._
    import StreamingQueryListener._
    import uk.ac.ic.imperial.benchmark.yahoo._
  
    val schema = YahooBenchmark.outputSchema.add("lastUpdate", LongType)
    val realTimeMs = udf((t: java.sql.Timestamp) => t.getTime)
    
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaCluster.kafkaNodesString)
      .option("subscribe", Variables.OUTPUT_TOPIC)
      .load()
      .withColumn("result", from_json($"value".cast("string"), schema))
      .select(
        $"timestamp" as 'resultOutput,
        $"result.*")
      .groupBy($"time_window", $"campaign_id")
      .agg(max($"resultOutput") as 'resultOutput, max('lastUpdate) as 'lastUpdate)
      .withColumn("diff", realTimeMs($"resultOutput") - 'lastUpdate)
      .selectExpr(
        "min(diff) as latency_min",
        "mean(diff) as latency_avg",
        "percentile_approx(diff, 0.95) as latency_95",
        "percentile_approx(diff, 0.99) as latency_99",
        "max(diff) as latency_max")
  }
}
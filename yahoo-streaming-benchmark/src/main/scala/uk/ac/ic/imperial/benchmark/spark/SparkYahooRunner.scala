package uk.ac.ic.imperial.benchmark.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, _}
import uk.ac.ic.imperial.benchmark.utils.LocalKafka
import uk.ac.ic.imperial.benchmark.yahoo.YahooBenchmarkRunner

/**
  * Runner for the Yahoo Benchmark using Apache Spark.
  */
class SparkYahooRunner(
                        override val spark: SparkSession,
                        kafkaCluster: LocalKafka,
                        parallelism: Int) extends YahooBenchmarkRunner {

  require(parallelism >= 1, "Parallelism can't be less than 1")

  import org.apache.spark.sql._
  import org.apache.spark.sql.streaming._
  import StreamingQueryListener._
  import uk.ac.ic.imperial.benchmark.yahoo._

  private var stream: StreamingQuery = _
  @volatile private var numRecs: Long = 0L
  @volatile private var startTime: Long = 0L
  @volatile private var endTime: Long = 0L

  class Listener extends StreamingQueryListener {
    override def onQueryStarted(event: QueryStartedEvent): Unit = {
      startTime = System.currentTimeMillis
    }

    override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
      endTime = System.currentTimeMillis
    }

    override def onQueryProgress(event: QueryProgressEvent): Unit = {
      numRecs += event.progress.numInputRows
    }
  }

  lazy val listener = new Listener

  override def start(): Unit = {
    // Original processing performed in `generateData` since we generate data in-memory
    spark.streams.addListener(listener)
    startTime = 0L
    endTime = 0L
    numRecs = 0L
    try {
      Thread.sleep(1000000000L)
    } catch {
      case e: InterruptedException => Thread.currentThread().interrupt() // set interrupt flag
    }
  }

  override def generateData(
                             campaigns: Array[CampaignAd],
                             tuplesPerSecond: Long,
                             recordGenParallelism: Int,
                             rampUpTimeSeconds: Int): Unit = {
    import spark.sql
    import spark.sqlContext.implicits._

    val sc = spark.sparkContext
    sc.setLocalProperty("spark.scheduler.pool", "yahoo-benchmark")

    val millisTime = udf((t: java.sql.Timestamp) => t.getTime)
    sql(s"set spark.sql.shuffle.partitions = 1")

    stream = YahooBenchmarkRunner.generateStream(spark, campaigns, tuplesPerSecond, parallelism, rampUpTimeSeconds)
      .where($"event_type" === "view")
      .select($"ad_id", $"event_time")
      .join(campaigns.toSeq.toDS().cache(), Seq("ad_id"))
      .groupBy(millisTime(window($"event_time", "10 seconds").getField("start")) as 'time_window, $"campaign_id")
      .agg(count("*") as 'count, max('event_time) as 'lastUpdate)
      .select(to_json(struct("*")) as 'value)
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaCluster.kafkaNodesString)
      .option("topic", Variables.OUTPUT_TOPIC)
      .option("checkpointLocation", s"/tmp/${java.util.UUID.randomUUID()}")
      .outputMode("update")
      .start()

    sc.setLocalProperty("spark.scheduler.pool", null)
  }

  override val params = Map("parallelism" -> parallelism)

  /**
    * The throughput calculation for Spark is straightforward thanks to the StreamingQueryListener. We take the number of records
    * processed from each `onQueryProgress` update, we take the start and end times from `onQueryStarted` which is a synchronous call,
    * and `onQueryTerminated`, which is an asynchronous call respectively. Therefore the throughput we calculate is actually a lower
    * bound, as the `onQueryTerminated` call can be made arbitrarily late. If the benchmark cluster is only running the benchmark however,
    * then this `arbitrarily lateness` should be very short (almost instantaneous).
    */
  override def getThroughput(): DataFrame = {
    import spark.implicits._

    val ping = System.currentTimeMillis()
    while (endTime == 0 && (ping - System.currentTimeMillis) < 10000) {
      Thread.sleep(1000)
    }
    spark.streams.removeListener(listener)
    assert(endTime != 0L, "Did not receive endTime from the listener in 10 seconds...")
    val start = new java.sql.Timestamp(startTime)
    val end = new java.sql.Timestamp(endTime)
    val duration = endTime - startTime

    Seq((start, end, numRecs, duration, numRecs * 1000.0 / duration)).toDS()
      .toDF("start", "end", "totalInput", "totalDurationMillis", "throughput")
  }

  /**
    * We calculate the latency as the difference between the Kafka ingestion timestamp for a given `time_window` and `campaign_id`
    * pair and the event timestamp of the latest record generated that belongs to that bucket.
    */
  override def getLatency(): DataFrame = {
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    import spark.implicits._

    val schema = YahooBenchmark.outputSchema.add("lastUpdate", TimestampType)
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
      .withColumn("diff", realTimeMs($"resultOutput") - realTimeMs($"lastUpdate"))
      .selectExpr(
        "min(diff) as latency_min",
        "mean(diff) as latency_avg",
        "percentile_approx(diff, 0.95) as latency_95",
        "percentile_approx(diff, 0.99) as latency_99",
        "max(diff) as latency_max")
  }
}

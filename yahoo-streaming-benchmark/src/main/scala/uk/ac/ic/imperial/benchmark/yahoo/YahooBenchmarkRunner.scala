package uk.ac.ic.imperial.benchmark.yahoo

import java.util.UUID

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.ac.ic.imperial.benchmark.utils.LocalKafka

trait YahooBenchmarkRunner {

  val spark: SparkSession

  import spark.implicits._

  /**
    * Start the reader for the system. This should be called before `generateData` so that reader initialization doesn't cause any
    * latency penalties.
    */
  def start(): Unit

  /**
    * Stop the runner.
    */
  def stop(): Unit = {
    spark.streams.active.foreach(_.stop())
  }

  /** Runner specific parameters that should be saved among benchmark parameters. */
  val params: Map[String, Any] = Map.empty

  /**
    * Generate data for the benchmark. The Spark and Flink runners generate the data themselves, therefore for Kafka,
    * we need to push data into it ourselves.
    */
  def generateData(
                    campaigns: Array[CampaignAd],
                    tuplesPerSecond: Long,
                    recordGenParallelism: Int,
                    rampUpTimeSeconds: Int): Unit = {
    campaigns.toSeq.toDS().select(to_json(struct("*")) as 'value, 'ad_id as 'key).write
      .format("kafka")
      .option("kafka.bootstrap.servers", LocalKafka.cluster.kafkaNodesString)
      .option("topic", Variables.CAMPAIGNS_TOPIC)
      .save()

    YahooBenchmarkRunner.generateStream(spark, campaigns, tuplesPerSecond, recordGenParallelism, rampUpTimeSeconds)
      .select(to_json(struct("*")) as 'value, 'ad_id as 'key)
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", LocalKafka.cluster.kafkaNodesString)
      .option("topic", Variables.EVENTS_TOPIC)
      .option("checkpointLocation", s"/tmp/${UUID.randomUUID()}")
      .start()
  }

  /** Calculate the throughput of this runner. */
  def getThroughput(): DataFrame

  /** Calculate the latency of this runner. */
  def getLatency(): DataFrame
}

object YahooBenchmarkRunner {
  /**
    * Default event generator that creates a streaming DataFrame of `Events` that can then be pushed to Kafka
    * or consumed directly by Spark.
    */
  def generateStream(
                      spark: SparkSession,
                      campaigns: Array[CampaignAd],
                      tuplesPerSecond: Long,
                      numPartitions: Int,
                      rampUpTimeSeconds: Int): DataFrame = {
    import spark.implicits._
    val adTypeLength = Variables.AD_TYPES.length
    val eventTypeLength = Variables.EVENT_TYPES.length
    val campaignLength = campaigns.length
    val getAdType = udf((i: Int) => Variables.AD_TYPES(i % adTypeLength))
    val getEventType = udf((i: Int) => Variables.EVENT_TYPES(i % eventTypeLength))
    val getCampaign = udf((i: Int) => campaigns(i % campaignLength).ad_id)

    val uuid = UUID.randomUUID().toString

    spark.readStream
      .format("rate")
      .option("rowsPerSecond", tuplesPerSecond.toString)
      .option("numPartitions", numPartitions.toString)
      .option("rampUpTime", rampUpTimeSeconds.toString + "s")
      .load()
      .select(
        lit(uuid) as 'user_id,
        lit(uuid) as 'page_id,
        getCampaign('value % 100000) as 'ad_id,
        getAdType('value % 100000) as 'ad_type,
        getEventType('value % 100000) as 'event_type,
        current_timestamp() as 'event_time,
        lit("255.255.255.255") as 'ip_address)
  }
}


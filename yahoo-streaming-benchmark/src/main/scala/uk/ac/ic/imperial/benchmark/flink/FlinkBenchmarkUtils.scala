package uk.ac.ic.imperial.benchmark.flink

import java.io.IOException
import java.util.Properties

import spray.json._
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.java.tuple.Tuple4
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.operators.{AbstractUdfStreamOperator, ChainingStrategy, OneInputStreamOperator}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010.FlinkKafkaProducer010Configuration
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.serialization.{JSONKeyValueDeserializationSchema, SimpleStringSchema}
import org.apache.flink.util.Collector

import uk.ac.ic.imperial.benchmark.yahoo.Event

/**
 * Class used to aggregate windowed counts.
 * @param lastUpdate Event time of the last record received for a given `campaign_id` and `time_window`
 */
case class WindowedCount(
    time_window: java.sql.Timestamp,
    campaign_id: String,
    var count: Long,
    var lastUpdate: java.sql.Timestamp)

/**
 * Json protocol defined for Spray when writing out aggregation results to Kafka.
 */
object WindowedCountJsonProtocol extends DefaultJsonProtocol {
  implicit object WindowedCountJsonFormat extends RootJsonFormat[WindowedCount] {
    def write(c: WindowedCount) = JsObject(
      "time_window" -> JsNumber(c.time_window.getTime),
      "campaign_id" -> JsString(c.campaign_id),
      "count" -> JsNumber(c.count),
      "lastUpdate" -> JsNumber(c.lastUpdate.getTime)
    )
    def read(value: JsValue): WindowedCount = {
      new WindowedCount(null, "", 0, null) // we don't need to deserialize JSON in Flink
    }
  }
}

object FlinkBenchmarkUtils {
  
  import WindowedCountJsonProtocol._

  /** Standard Flink-provided kafka consumer using a json KV deserialization schema. */
  def getKafkaConsumer(topic: String, properties: Properties): FlinkKafkaConsumer010[String] = {

    properties.put("auto.offset.reset", "earliest")

    new FlinkKafkaConsumer010[String](topic, new SimpleStringSchema(), properties)
  }

  /** Flink Source that reads from Kafka. */
  def getKafkaJsonStream(env: StreamExecutionEnvironment, topic: String, props: Properties): DataStream[String] = {
    return env.addSource(getKafkaConsumer(topic, props))
  }

  /** Handle configuration of env here */
  def getExecutionEnvironment(parallelism: Int = Int.MinValue): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    if (parallelism > 0) env.setParallelism(parallelism)
    return env
  }

  /** Write strings to Kafka to the given topic */
  def writeStringStream(inputData: DataStream[String], topic: String, props: Properties): FlinkKafkaProducer010Configuration[String] = {
    FlinkKafkaProducer010.writeToKafkaWithTimestamps(inputData.javaStream, topic, new SimpleStringSchema, props)
  }

  /** Write an object as json to Kafka to the given topic */
  def writeJsonStream(inputData: DataStream[WindowedCount], topic: String, props: Properties): FlinkKafkaProducer010Configuration[String] = {
    FlinkKafkaProducer010.writeToKafkaWithTimestamps(
      inputData.map(_.toJson.compactPrint).javaStream,
      topic,
      new SimpleStringSchema,
      props)
  }
}
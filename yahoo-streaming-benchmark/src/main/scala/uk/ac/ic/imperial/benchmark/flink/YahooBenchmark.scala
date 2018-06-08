package uk.ac.ic.imperial.benchmark.flink

import java.sql.Timestamp
import java.util.{Properties, UUID}

import scala.io.Source

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.TimestampExtractor
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.triggers.Trigger._
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.api.java.tuple.Tuple2

import uk.ac.ic.imperial.benchmark.utils._
import uk.ac.ic.imperial.benchmark.yahoo._
/*import uk.ac.ic.imperial.benchmark.spark._*/

object YahooBenchmark {

    
  class TimestampingSink extends SinkFunction[WindowedCount] {

		final val serialVersionUID = 1876986644706201196L;		

		var latency = 0L
    var prevTime = -1L
    var curTime = -1L
		var counter = 0
		var maxLatency = 0L
		val startTime = System.currentTimeMillis()
		val endTime = startTime + 60 * 1000 * 5
    
    override def invoke(wc: WindowedCount) = {
    	//println("Last update " + wc.lastUpdate.getSeconds)
		  //println("Other " + wc.time_window.getSeconds)
		  if(endTime <= System.currentTimeMillis()) {
		    println("Terminating execution...")
		    throw new Exception("stop...")//System.exit(0)
		  }
		  if (prevTime == -1L) {
		    prevTime = System.currentTimeMillis()
		  }
		  else {
		     curTime = System.currentTimeMillis()
		     latency = (curTime - prevTime) - 10000
		     prevTime = curTime
		     
		     maxLatency = Math.max(maxLatency, latency)
		     counter += 1
		     if (counter == 100) {
		       counter = 0
		       println("Max Latency is " + maxLatency + " msec. Latency is " + latency + " msec")
		     }
		  }
		}
	}
  
  // Transcribed from https://github.com/dataArtisans/yahoo-streaming-benchmark/blob/d8381f473ab0b72e33469d2b98ed1b77317fe96d/flink-benchmarks/src/main/java/flink/benchmark/AdvertisingTopologyFlinkWindows.java#L179
  class EventAndProcessingTimeTrigger(triggerIntervalMs: Int) extends Trigger[Any, TimeWindow] {

    var nextTimer: Long = 0L

    override def onElement(element: Any, timestamp: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      ctx.registerEventTimeTimer(window.maxTimestamp())
      // register system timer only for the first time
      val firstTimerSet = ctx.getKeyValueState("firstTimerSet", classOf[java.lang.Boolean], new java.lang.Boolean(false))
      if (!firstTimerSet.value()) {
        nextTimer = System.currentTimeMillis() + triggerIntervalMs
        ctx.registerProcessingTimeTimer(nextTimer)
        firstTimerSet.update(true)
      }
      return TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      // schedule next timer
      nextTimer = System.currentTimeMillis() + triggerIntervalMs
      ctx.registerProcessingTimeTimer(nextTimer)
      TriggerResult.FIRE;
    }

    override def clear(window: TimeWindow, ctx: TriggerContext): Unit = {
      ctx.deleteProcessingTimeTimer(nextTimer)
      ctx.deleteEventTimeTimer(window.maxTimestamp())
    }
  }

  /**
   * A logger that prints out the number of records processed and the timestamp, which we can later use for throughput calculation.
   */
  class ThroughputLogger(logFreq: Long) extends FlatMapFunction[Event, Event] {
      private var lastTotalReceived: Long = 0L
      private var lastTime: Long = 0L
      private var totalReceived: Long = 0L
      private var averageThroughput = 0d
      private var throughputCounter = 0
      private var throughputSum = 0d
      
      private var count = 0
  
      override def flatMap(element: Event, collector: Collector[Event]): Unit = {       
        if (totalReceived == 0) {
          println(s"ThroughputLogging:${System.currentTimeMillis()},${totalReceived}")
        }
        totalReceived += 1
        if (totalReceived % logFreq == 0) {
          val currentTime = System.currentTimeMillis()
          val throughput = (totalReceived - lastTotalReceived) / (currentTime - lastTime) * 1000.0d
          if (throughput !=0) {
            throughputCounter +=1
            throughputSum += throughput
            averageThroughput = throughputSum / throughputCounter
          }
          println(s"Throughput:${throughput}, Average:${averageThroughput}")
          lastTime = currentTime
          lastTotalReceived = totalReceived
          println(s"ThroughputLogging:${System.currentTimeMillis()},${totalReceived}")
/*          println ("My count is " + count)
          count+=1*/
        }
        collector.collect(element)
      }
    }

  class StaticJoinMapper(campaigns: Map[String, String]) extends FlatMapFunction[Event, (String, String, Timestamp)] {
    override def flatMap(element: Event, collector: Collector[(String, String, Timestamp)]): Unit = {
      collector.collect((campaigns(element.ad_id), element.ad_id, element.event_time))
    }
  }

  class AdTimestampExtractor extends TimestampExtractor[(String, String, Timestamp)] {

    var maxTimestampSeen: Long = 0L

    override def extractTimestamp(element: (String, String, Timestamp), currentTimestamp: Long): Long = {
      val timestamp = element._3.getTime
      maxTimestampSeen = Math.max(timestamp, maxTimestampSeen)
      timestamp
    }

    override def extractWatermark(element: (String, String, Timestamp), currentTimestamp: Long) = Long.MinValue

    override def getCurrentWatermark(): Long = maxTimestampSeen - 1L
  }

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val windowMillis: Time = Time.milliseconds(params.getLong("windowMillis", 10000))
    var parallelism: Int = params.getInt("parallelism", 1)
    require(parallelism > 0, "Parallelism needs to be a positive integer.")
    // Used for assigning event times from out of order data

    // Used when generating input
    val numCampaigns: Int = params.getInt("numCampaigns", 100)
    var tuplesPerSecond: Int = params.getInt("tuplesPerSecond", 10)
    val rampUpTimeSeconds: Int = params.getInt("rampUpTimeSeconds", 10)
    val triggerIntervalMs: Int = params.getInt("triggerIntervalMs", 0)
    require(triggerIntervalMs >= 0, "Trigger interval can't be negative.")
    // Logging frequency in #records for throughput calculations
    val logFreq: Int = params.getInt("logFreq", 10000000)
    val outputTopic: String = params.get("outputTopic", "YahooBenchmarkOutput")

    val props = params.getProperties
    
    var i = 0
    var j = 0
		while (i < args.length ) {
		  j = i + 1
			if (j == args.length) {
				System.err.println("Wrong number of arguments");
				System.exit(1);
			}	
			if (args(i).equals("--flinkParallelism")) { 
				parallelism = Integer.parseInt(args(j));
			} else
			if (args(i).equals("--recordsPerSecond")) { 
				tuplesPerSecond = Integer.parseInt(args(j));
			} else {
				System.err.println(String.format("error: unknown flag %s %s", args(i), args(j)));
				System.exit(1);
			}
			i = j + 1;
		}
    
    val env: StreamExecutionEnvironment = FlinkBenchmarkUtils.getExecutionEnvironment(parallelism)

    env.getConfig.enableObjectReuse()

    val campaignAdSeq: Seq[CampaignAd] = generateCampaignMapping(numCampaigns)

    // According to https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/operators/StreamingRuntimeContext.java
    // it looks like broadcast variables aren't actually supported in streaming...
    // curious how others are achieving a join against a static set. For now we simply include the lookup map in the closure.
    val campaignLookup: Map[String, String] = campaignAdSeq.map(ca => (ca.ad_id, ca.campaign_id)).toMap

    val events = env.addSource(new EventGenerator(campaignAdSeq))
    
    val windowedEvents: WindowedStream[(String, String, Timestamp), Tuple, TimeWindow] = events
      .flatMap(new ThroughputLogger(logFreq))
      .filter(_.event_type == "view")
      .flatMap(new StaticJoinMapper(campaignLookup))
      .assignTimestamps(new AdTimestampExtractor())
      .keyBy(0) // campaign_id
      .window(TumblingEventTimeWindows.of(windowMillis))

    // set a trigger to reduce latency. Leave it out to increase throughput
    if (triggerIntervalMs > 0) {
      windowedEvents.trigger(new EventAndProcessingTimeTrigger(triggerIntervalMs))
    }

    val windowedCounts = windowedEvents.fold(new WindowedCount(null, "", 0, new java.sql.Timestamp(0L)),
      (acc: WindowedCount, r: (String, String, Timestamp)) => {
        val lastUpdate = if (acc.lastUpdate.getTime < r._3.getTime) r._3 else acc.lastUpdate
        acc.count += 1
        acc.lastUpdate = lastUpdate
        acc
      },
      (key: Tuple, window: TimeWindow, input: Iterable[WindowedCount], out: Collector[WindowedCount]) => {
        val windowedCount = input.iterator.next()
        //println(windowedCount.lastUpdate)        
        out.collect(new WindowedCount(
          new java.sql.Timestamp(window.getStart), key.getField(0), windowedCount.count, windowedCount.lastUpdate))
      }
    )
    
    windowedCounts.addSink(new TimestampingSink())
        

    // only write to kafka when specified
    if (params.has("bootstrap.servers")) {
      FlinkBenchmarkUtils.writeJsonStream(windowedCounts, outputTopic, props)
    }

    env.execute("Flink Yahoo Benchmark")
  }

  /** Generate in-memory ad_id to campaign_id map. We generate 10 ads per campaign. */
  private def generateCampaignMapping(numCampaigns: Int): Seq[CampaignAd] = {
    Seq.tabulate(numCampaigns) { _ =>
      val campaign = UUID.randomUUID().toString
      Seq.tabulate(10)(_ => CampaignAd(UUID.randomUUID().toString, campaign))
    }.flatten
  }
}
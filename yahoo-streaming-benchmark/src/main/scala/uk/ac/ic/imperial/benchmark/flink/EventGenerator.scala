package uk.ac.ic.imperial.benchmark.flink

import java.util.UUID

import uk.ac.ic.imperial.benchmark.yahoo._
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
 * Fixed event generator that is not rate-limited.
 *
 * @param campaigns The ad campaigns to generate events for
 */
class EventGenerator(
    campaigns: Seq[CampaignAd]) extends RichParallelSourceFunction[Event] {

  var running = true
  private val uuid = UUID.randomUUID().toString // used as a dummy value for all events, based on ref code

  private val adTypeLength = Variables.AD_TYPES.length
  private val eventTypeLength = Variables.EVENT_TYPES.length
  private val campaingsArray = campaigns.toArray
  private val campaignLength = campaingsArray.length
  private lazy val parallelism = getRuntimeContext().getNumberOfParallelSubtasks()

  override def run(sourceContext: SourceContext[Event]): Unit = {
    var i = 0
    var j = 0
    var k = 0
    var t = 0
    var ts = System.currentTimeMillis()

    while (running) {
      i += 1
      j += 1
      k += 1
      t += 1
      if (i >= campaignLength) {
        i = 0
      }
      if (j >= adTypeLength) {
        j = 0
      }
      if (k >= eventTypeLength) {
        k = 0
      }
      if (t >= 1000) {
        t = 0
        ts = System.currentTimeMillis()
      }

      val ad_id = campaingsArray(i).ad_id // ad id for the current event index
      val ad_type = Variables.AD_TYPES(j) // current adtype for event index
      val event_type = Variables.EVENT_TYPES(k) // current event type for event index

      val event = Event(
        uuid, // random user, irrelevant
        uuid, // random page, irrelevant
        ad_id,
        ad_type,
        event_type,
        new java.sql.Timestamp(ts),
        "255.255.255.255") // generic ipaddress, irrelevant
      sourceContext.collect(event)

    }
    sourceContext.close()
  }

  override def cancel(): Unit = {
    running = false
  }
}
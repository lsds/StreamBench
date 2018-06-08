package uk.ac.ic.imperial.benchmark.yahoo

/** Classes we use in the Yahoo Benchmark. */

/** Input data */
case class Event(
                  user_id: String,         // UUID
                  page_id: String,         // UUID
                  ad_id: String,           // UUID
                  ad_type: String,         // in {banner, modal, sponsored-search, mail, mobile}
                  event_type: String,      // in {view, click, purchase}
                  event_time: java.sql.Timestamp,
                  ip_address: String) {
  def this() = this(null, null, null, null, null, new java.sql.Timestamp(0L), null)
}

/** Output data */
case class Output(
                   time_window: java.sql.Timestamp,
                   campaign_id: String,
                   count: Long,
                   lastUpdate: java.sql.Timestamp) {
  def this() = this(new java.sql.Timestamp(0L), null, 0L, new java.sql.Timestamp(0L))
}

/** Event data after a projection */
case class ProjectedEvent(
                           ad_id: String, // UUID
                           event_time: java.sql.Timestamp) {
  def this() = this(null, new java.sql.Timestamp(0L))
}

/** Used for forming a map of ad to campaigns. */
case class CampaignAd(ad_id: String, campaign_id: String) {
  def this() = this(null, null)
}

/** Static variables used through out the benchmark. */
object Variables {
  val CAMPAIGNS_TOPIC = "campaigns"
  val EVENTS_TOPIC = "events"
  val OUTPUT_TOPIC = "output"

  val AD_TYPES = Seq("banner", "modal", "sponsored-search", "mail", "mobile")
  val EVENT_TYPES = Seq("view", "click", "purchase")
}
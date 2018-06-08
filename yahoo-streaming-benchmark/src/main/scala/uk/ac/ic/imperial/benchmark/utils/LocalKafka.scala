package uk.ac.ic.imperial.benchmark.utils

import org.apache.spark.sql._

import scala.sys.process._

/**
  * Class for managing Apache Kafka on a **generic** cluster. Currently supports one broker per instance.
  * Will install Zookeeper on a single node, colocated with the first broker.
  * Please do not use this code in production.
  *
  * @param spark                 SparkSession that will allow us to run Spark jobs to distribute ssh public keys to executors
  * @param numKafkaNodes         Number of Kafka brokers to install. Must be smaller than or equal to the number of executors
  * @param stopSparkOnKafkaNodes Whether to kill Spark executors on the instances we install Kafka for better isolation
  * @param kafkaVersion          The version of Kafka to install
  */
class LocalKafka(
                  spark: SparkSession,
                  numKafkaNodes: Int = 1,
                  stopSparkOnKafkaNodes: Boolean = false,
                  kafkaVersion: String = "0.10.2.1") extends Serializable with SSHUtils {

  @transient val sc = spark.sparkContext

  import LocalKafka._

  private val workers: List[String] = {
    val executors = sc.getExecutorMemoryStatus.keys.map(_.split(":").head).toSet
    if (executors.size == 1) {
      // just the driver
      List(myIp)
    } else {
      (executors - myIp).toList
    }
  }
  private val numExecutors = workers.length

  require(numExecutors >= numKafkaNodes,
    s"""You don't have enough executors to maintain $numKafkaNodes Kafka brokers. Please increase your cluster size,
       |or decrease the amount of Kafka brokers. Available executors: $numExecutors.
     """.stripMargin)
  if (stopSparkOnKafkaNodes) {
    require(List(myIp) != workers, "You can't stop Spark when running Kafka only on the driver.")
  }
  lazy val kafkaNodes = workers.take(numKafkaNodes)
  private lazy val zookeeper = workers(0)
  lazy val kafkaNodesString = kafkaNodes.map(_ + ":9092").mkString(",")
  lazy val zookeeperAddress = zookeeper + ":2181"
  private val dbfsDir = "streaming/benchmark"

  def init(): Unit = {
    generateSshKeys()
    writeInstallFile(dbfsDir, kafkaVersion)
  }

  init()

  /**
    * Setup Kafka in cluster
    */
  def setup() = {
    /* Assuming a cluster where we have access
    setupSSH(numExecutors)
    */

    workers.foreach { ip =>
      ssh(ip, s"bash /tmp/$dbfsDir/install-kafka.sh")
    }

    ssh(zookeeper, s"kafka/bin/zookeeper-server-start.sh -daemon kafka/config/zookeeper.properties")

    kafkaNodes.zipWithIndex.foreach { case (host, id) =>
      ssh(host, s"bash /tmp/$dbfsDir/configure-kafka.sh $zookeeper $id $host")
      ssh(host, s"kafka/bin/kafka-server-start.sh -daemon kafka/config/server.properties")
    }

    Thread.sleep(30 * 1000) // wait for Kafka to come up

    if (stopSparkOnKafkaNodes) {
      kafkaNodes.foreach { ip =>
        ssh(ip, s"sudo monit stop spark-slave")
      }
    }
  }

  /** Create a `topic` on Kafka with the given number of partitions and replication factor. */
  def createTopic(topic: String, partitions: Int = 8, replFactor: Int = 1): Unit = {
    ssh(kafkaNodes(0), s"kafka/bin/kafka-topics.sh --create --topic $topic --partitions $partitions " +
      s"--replication-factor $replFactor --zookeeper $zookeeper:2181")
  }

  /** Delete the given topic if it exists. */
  def deleteTopicIfExists(topic: String): Unit = {
    try {
      ssh(kafkaNodes(0), s"kafka/bin/kafka-topics.sh --delete --topic $topic --zookeeper $zookeeper:2181")
    } catch {
      case e: RuntimeException =>
    }
  }

  /**
    * Stop Kafka cluster (and Zookeeper)
    */
  def stopAll(): Unit = {
    kafkaNodes.foreach { ip =>
      ssh(ip, s"kafka/bin/kafka-server-stop.sh")
    }
    Thread.sleep(5 * 1000) // wait for Kafka to tear down
    ssh(zookeeper, s"kafka/bin/zookeeper-server-stop.sh")
  }

}

object LocalKafka extends Serializable {
  var cluster: LocalKafka = null

  def setup(spark: SparkSession, numKafkaNodes: Int = 1, stopSparkOnKafkaNodes: Boolean = false): LocalKafka = {
    if (cluster == null) {
      cluster = new LocalKafka(spark, numKafkaNodes, stopSparkOnKafkaNodes)
      cluster.setup()
    }
    cluster
  }

  private def myIp: String = {
    "hostname".!!.split("_").takeRight(4).map(_.trim).mkString(".")
  }

  private def writeFile(path: String, contents: String, append: Boolean = false): Unit = {
    val fw = new java.io.FileWriter(path, append)
    fw.write(contents)
    fw.close()
  }

  private def writeInstallFile(dbfsDir: String, kafkaVersion: String): Unit = {
    Seq("mkdir", "-p", s"/tmp/$dbfsDir").!!
    writeFile(s"/tmp/$dbfsDir/install-kafka.sh", getInstallKafkaScript(dbfsDir, kafkaVersion))
    writeFile(s"/tmp/$dbfsDir/configure-kafka.sh", configureKafkaScript)
  }

  /** Script that can be run on executors to install Kafka. */
  private def getInstallKafkaScript(dbfsDir: String, kafkaVersion: String) = {
    s"""#!/bin/bash
       |set -e
       |
       |mkdir -p kafka && cd kafka
       |if [ ! -r "/tmp/$dbfsDir/kafka-${kafkaVersion}.tgz" ]; then
       | wget -O /tmp/$dbfsDir/kafka-${kafkaVersion}.tgz "http://mirrors.advancedhosters.com/apache/kafka/${kafkaVersion}/kafka_2.11-${kafkaVersion}.tgz"
       |fi
       |tar -xvzf /tmp/$dbfsDir/kafka-${kafkaVersion}.tgz --strip 1 1> /dev/null 2>&1
     """.stripMargin
  }

  /** Default Kafka configuration file. */
  private val configureKafkaScript =
    """#!/bin/bash
set -e
cd kafka
cat > config/server.properties <<EOL
############################# Server Basics #############################
# The id of the broker. This must be set to a unique integer for each broker.
broker.id=$2
############################# Zookeeper #############################
# Zookeeper connection string (see zookeeper docs for details).
# This is a comma separated host:port pairs, each corresponding to a zk
# server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002".
# You can also append an optional chroot string to the urls to specify the
# root directory for all kafka znodes.
zookeeper.connect=$1:2181
# Timeout in ms for connecting to zookeeper
zookeeper.connection.timeout.ms=6000
# Switch to enable topic deletion or not, default value is false
delete.topic.enable=true
auto.create.topics.enable=false
############################# Socket Server Settings #############################
# The port the socket server listens on
port=9092
# Hostname the broker will bind to. If not set, the server will bind to all interfaces
host.name=$3
# Hostname the broker will advertise to producers and consumers. If not set, it uses the
# value for "host.name" if configured.  Otherwise, it will use the value returned from
# java.net.InetAddress.getCanonicalHostName().
advertised.host.name=$3
# The port to publish to ZooKeeper for clients to use. If this is not set,
# it will publish the same port that the broker binds to.
# advertised.port=9092
# The number of threads handling network requests
num.network.threads=3
# The number of threads doing disk I/O
num.io.threads=8
# The send buffer (SO_SNDBUF) used by the socket server
socket.send.buffer.bytes=102400
# The receive buffer (SO_RCVBUF) used by the socket server
socket.receive.buffer.bytes=102400
# The maximum size of a request that the socket server will accept (protection against OOM)
socket.request.max.bytes=104857600
# Use kafka broker receive time in message timestamps, instead of creation time
log.message.timestamp.type=LogAppendTime
############################# Log Basics #############################
# A comma seperated list of directories under which to store log files
log.dirs=/tmp/kafka-logs
# The default number of log partitions per topic. More partitions allow greater
# parallelism for consumption, but this will also result in more files across
# the brokers.
num.partitions=8
# The number of threads per data directory to be used for log recovery at startup and flushing at shutdown.
# This value is recommended to be increased for installations with data dirs located in RAID array.
num.recovery.threads.per.data.dir=1
############################# Log Retention Policy #############################
# The following configurations control the disposal of log segments. The policy can
# be set to delete segments after a period of time, or after a given size has accumulated.
# A segment will be deleted whenever *either* of these criteria are met. Deletion always happens
# from the end of the log.
# The minimum age of a log file to be eligible for deletion
log.retention.hours=168
# The maximum size of a log segment file. When this size is reached a new log segment will be created.
log.segment.bytes=1073741824
# The interval at which log segments are checked to see if they can be deleted according
# to the retention policies
log.retention.check.interval.ms=300000
# By default the log cleaner is disabled and the log retention policy will default to just delete segments after their retention expires.
# If log.cleaner.enable=true is set the cleaner will be enabled and individual logs can then be marked for log compaction.
log.cleaner.enable=false
EOL
    """
}
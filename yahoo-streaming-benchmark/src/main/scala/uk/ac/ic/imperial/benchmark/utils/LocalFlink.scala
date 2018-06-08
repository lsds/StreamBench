package uk.ac.ic.imperial.benchmark.utils

// Imports from LocalKafka that I haven't touched, even though some are probably not used
import java.io.File
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.nio.file.attribute.PosixFilePermissions
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import java.io.File

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter._
import java.io.{File, FileInputStream, FileOutputStream}
import java.util.jar.{JarEntry, JarOutputStream}
import java.util.jar.Attributes.Name
import java.util.jar.Manifest

import com.google.common.io.{ByteStreams, Files}

import org.apache.spark.sql._

import sys.process._

/**
 * Class for managing Apache Flink on a Databricks cluster. Currently installs one taskmanager per EC2 instance.
 * It will also require an additional instance for the jobmanager if used in distributed mode.
 * Please do not use this code in production.
 *
 * @param spark SparkSession that will allow us to run Spark jobs to distribute ssh public keys to executors
 * @param numFlinkNodes Number of taskmanagers to launch. Must be smaller than or equal to the number of executors - 1,
 *                      unless used on Community Edition in local mode.
 * @param stopSparkOnFlinkNodes Whether to kill Spark executors on the instances we install Flink for better isolation
 * @param flinkVersion The version of Flink to install
 * @param numTaskSlots Number of task slots for each taskmanager. Set this to the number of CPUs available for the instance
 *                     types you use.
 */
class LocalFlink(
    spark: SparkSession,
    numFlinkNodes: Int = 1,
    val stopSparkOnFlinkNodes: Boolean = false,
    numTaskSlots: Int = 4,
    flinkVersion: String = "1.4.2",
    scalaVersion: String = "2.11") extends Serializable with SSHUtils {
  import spark.implicits._
  @transient val sc = spark.sparkContext

  import LocalFlink._

  require(numTaskSlots >= 1, "Number of task slots per Flink taskmanager has to be greater than or equal to 1.")

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
  def isJobManagerOnDriver = workers == List(myIp)

  require(numExecutors >= numFlinkNodes + 1 || (numExecutors == 1 && numExecutors == 1),
    s"""You don't have enough executors to maintain $numFlinkNodes Flink task managers + the job manager. Please increase your cluster size,
       |or decrease the amount of Flink nodes. Available executors: $numExecutors.
     """.stripMargin)
  if (stopSparkOnFlinkNodes) {
    require(List(myIp) != workers, "You can't stop Spark when running Flink only on the driver.")
  }
  private lazy val flinkNodes = workers.take(numFlinkNodes + 1)
  // assuming exactly one flink job manager.
  private lazy val jobManager = flinkNodes.last
  lazy val taskManagers = if (isJobManagerOnDriver) flinkNodes else flinkNodes.dropRight(1)

  // jteoh: Flink workers (taskManagers) only require host name bc the master's startup script will ssh + start them.
  // It does require that paths are the same on all machines, but that's already happening anyways.
  lazy val taskManagersString = taskManagers.mkString(",")
  private val dbfsDir = "streaming/benchmark"

  def init(): Unit = {
    generateSshKeys()
    writeInstallFile(dbfsDir, flinkVersion, scalaVersion)
  }

  init()

  private def installFlink(): Unit = {
    s"bash /tmp/$dbfsDir/install-flink.sh".!!
    flinkNodes.foreach { ip =>
      ssh(ip, s"bash /tmp/$dbfsDir/install-flink.sh")
    }
  }

  private def configureFlink(): Unit = {
    if (!isJobManagerOnDriver) {
      s"bash /tmp/$dbfsDir/configure-flink.sh $jobManager $taskManagersString $numTaskSlots".!!
      flinkNodes.foreach { host =>
        ssh(host, s"bash /tmp/$dbfsDir/configure-flink.sh $jobManager $taskManagersString $numTaskSlots")
      }
    } else {
      // Akka actor system uses the `hostname` to bind the client which resolves to 127.0.1.1 instead of 127.0.0.1
      val hostname = "hostname".!!
      val hosts = s"127.0.0.1 localhost\n127.0.0.1 $hostname"
      writeFile("/etc/hosts", hosts)
      s"bash /tmp/$dbfsDir/configure-flink.sh $jobManager $taskManagersString $numTaskSlots".!!
    }
  }

  /** Copy the dependencies to Flink's classpath. */
  private def copyLibraries(): Unit = {
    val copyLibScript = getClasspath().map { case (fileName, filePath) =>
      s"cp $filePath flink/lib/$fileName"
    }.mkString("\n")
    writeFile(s"/tmp/$dbfsDir/copy-flink-libraries.sh", copyLibScript)
    val copyCommand = s"bash /tmp/$dbfsDir/copy-flink-libraries.sh"
    copyCommand.!!
    if (!isJobManagerOnDriver) {
      flinkNodes.foreach(ssh(_, copyCommand))
    }
  }

  private def startFlink(): Unit = {
    if (isJobManagerOnDriver) {
      "flink/bin/start-local.sh".!!
    } else {
      ssh(jobManager, "flink/bin/jobmanager.sh start cluster")
      taskManagers.foreach { host =>
        ssh(host, "flink/bin/taskmanager.sh start")
      }
    }
  }

  /**
   * Setup Flink in cluster
   */
  def setup(): Unit = {
    println(s"Job Manager: $jobManager")
    println(s"Task Managers: $taskManagersString")
    /*val key = publicKey
    sc.parallelize(0 until numExecutors, numExecutors).foreach { i =>
      addAuthorizedPublicKey(key)
    }*/

    //installFlink()
    //configureFlink()
    //copyLibraries()
    startFlink()

    Thread.sleep(15 * 1000) // wait for Flink to come up - probably longer than actually required

    if (stopSparkOnFlinkNodes) {
      flinkNodes.foreach { ip =>
        ssh(ip, s"sudo monit stop spark-slave")
      }
    }
  }

  def shutdown() {
    // Normally you can use stop-cluster.sh, but this requires ssh access between nodes
    // Alternatively, go to each node and call it yourself.
    //https://ci.apache.org/projects/flink/flink-docs-release-1.2/setup/cluster_setup.html#adding-jobmanagertaskmanager-instances-to-a-cluster
    //ssh(jobManager, s"flink/bin/stop-cluster.sh")
    flinkNodes.foreach { host =>
      println(s"Stopping task manager on $host")
      ssh(host, "flink/bin/taskmanager.sh stop")
    }
    println(s"Stopping job manager on $jobManager")
    ssh(jobManager, s"flink/bin/jobmanager.sh stop cluster")
  }

  /** Get all the dependencies required for running the benchmark. */
  private def getClasspath(): Seq[(String, String)] = {
    case class MavenCoordinate(orgId: String, artifactId: String, version: String) {
      def name: String = s"${artifactId}-${version}.jar"
      def path: String = s"/tmp/FileStore/jars/maven/${orgId.replace(".", "/")}/${artifactId}-${version}.jar"
    }
    Seq(
      MavenCoordinate("io.spray", "spray-json_2.11", "1.3.3"),
      MavenCoordinate("org.apache.kafka", "kafka-streams", "0.10.2.1"),
      MavenCoordinate("org.apache.kafka", "kafka-clients", "0.10.2.1"),
      MavenCoordinate("org.apache.kafka", "connect-api", "0.10.2.1"),
      MavenCoordinate("org.apache.kafka", "connect-json", "0.10.2.1"),
      MavenCoordinate("org.apache.flink", "flink-streaming-scala_2.11", flinkVersion),
      MavenCoordinate("org.apache.flink", "flink-streaming-java_2.11", flinkVersion),
      MavenCoordinate("org.apache.flink", "flink-scala_2.11", flinkVersion),
      MavenCoordinate("org.apache.flink", "flink-connector-kafka-0.10_2.11", flinkVersion),
      MavenCoordinate("org.apache.flink", "flink-connector-kafka-0.9_2.11", flinkVersion),
      MavenCoordinate("org.apache.flink", "flink-connector-kafka-base_2.11", flinkVersion),
      MavenCoordinate("org.rocksdb", "rocksdbjni", "5.0.1")
    ).map(coord => (coord.name, coord.path))
  }

  /** Run a jar as a Flink job. */
  def runJob(className: String, args: String*): Unit = {
    val jar = packJar(spark)
    (Seq("flink/bin/flink", "run", "-c", className, jar.toString) ++ args).!!
  }
}

// Same deal - unless otherwise noted, assume same as LocalKafka
object LocalFlink extends Serializable {
  var cluster: LocalFlink = null

  def setup(
             spark: SparkSession,
             numFlinkNodes: Int = 1,
             stopSparkOnFlinkNodes: Boolean = false,
             numTaskSlots: Int = 4,
             flinkVersion: String = "1.4.2",
             scalaVersion: String = "2.11"): LocalFlink = {
    if(cluster == null) {
      cluster = new LocalFlink(spark, numFlinkNodes, stopSparkOnFlinkNodes, numTaskSlots, flinkVersion, scalaVersion)
      cluster.setup()
    }
    cluster
  }

  def shutdown() {
    if (cluster != null) {
      cluster.shutdown()
      cluster = null
    }
  }
  private def myIp = {
    "hostname".!!.split("_").takeRight(4).map(_.trim).mkString(".")
  }

  def ssh(host: String, command: String): Unit = {
    if (cluster != null) {
      cluster.ssh(host, command)
    } else {
      println("No cluster set up!")
    }
  }

  private def writeFile(path: String, contents: String, append: Boolean = false): Unit = {
    val fw = new java.io.FileWriter(path, append)
    fw.write(contents)
    fw.close()
  }

  private def writeInstallFile(dbfsDir: String, flinkVersion: String, scalaVersion: String): Unit = {
    Seq("mkdir", "-p", s"/tmp/$dbfsDir").!!
    writeFile(s"/tmp/$dbfsDir/install-flink.sh", getInstallFlinkScript(dbfsDir, flinkVersion, scalaVersion))
    writeFile(s"/tmp/$dbfsDir/configure-flink.sh", configureFlinkScript)
  }

  /** Script to install Flink on an executor. */
  private def getInstallFlinkScript(dbfsDir: String, flinkVersion: String, scalaVersion: String = "2.11") = {
    s"""#!/bin/bash
       |set -e
       |
       |sudo chown ubuntu /home/ubuntu
       |mkdir -p flink && cd flink
       |if [ ! -r "/dbfs/$dbfsDir/flink-${flinkVersion}.tgz" ]; then
       | wget -O /dbfs/$dbfsDir/flink-${flinkVersion}.tgz "http://mirrors.advancedhosters.com/apache/flink/flink-${flinkVersion}/flink-${flinkVersion}-bin-hadoop27-scala_${scalaVersion}.tgz"
       |fi
       |tar -xvzf /dbfs/$dbfsDir/flink-${flinkVersion}.tgz --strip 1 1> /dev/null 2>&1
     """.stripMargin
  }

  /**
   * Script to configure Flink on an executor.
   * First argument = jobmanager (replaced jobmanager.rpc.address via sed, orig file as .bak)
   * Second argument = comma-separated list of taskmanagers, which gets written to 'slaves' file
   * Third argument = number of task slots for reach taskmanager
   * We set the heap sizes a bit higher than the default configurations here, but we don't have a memory intensive workload
   * therefore don't need to fine tune that.
   */
  private val configureFlinkScript =
    """#!/bin/bash
set -e
cd flink/conf
sed -i.bak -e "s/jobmanager.rpc.address:.*/jobmanager.rpc.address: $1/" flink-conf.yaml
sed -i.bak -e "s/taskmanager.numberOfTaskSlots: 1/taskmanager.numberOfTaskSlots: $3/" flink-conf.yaml
sed -i.bak -e "s/jobmanager.heap.mb:.*/jobmanager.heap.mb: 2048/" flink-conf.yaml
sed -i.bak -e "s/taskmanager.heap.mb:.*/taskmanager.heap.mb: 3096/" flink-conf.yaml
tr ',' '\n' <<<"$2" >slaves
echo Flink slaves:
cat slaves
echo
    """

  /** Packs a jar of compiled `package` cells so that we can submit a Databricks cell as a Flink job. */
  private def packJar(spark: SparkSession): File = {
    /*val compiledClassesDir = spark.conf.get("spark.repl.class.outputDir")

    val filter = new IOFileFilter {
      override def accept(file: File): Boolean = {
        accept(file.getParentFile, file.getName)
      }

      override def accept(dir: File, name: String): Boolean = {
        val subDir = Seq("com", "databricks", "benchmark").mkString(File.separator)
        dir.toString.contains(subDir) && name.endsWith(".class")
      }
    }

    val filesToPack = FileUtils.listFiles(new File(compiledClassesDir), filter, TrueFileFilter.INSTANCE).asScala.map { file =>
      file.toString.replace(compiledClassesDir + File.separator, "") -> file
    }

    val jarFile = new File("/tmp", "benchmark.jar")
    val jarFileStream = new FileOutputStream(jarFile)
    val jarStream = new JarOutputStream(jarFileStream, new Manifest())

    for (file <- filesToPack) {
      val jarEntry = new JarEntry(file._1)
      jarStream.putNextEntry(jarEntry)

      val in = new FileInputStream(file._2)
      ByteStreams.copy(in, jarStream)
      in.close()
    }
    jarStream.close()
    jarFileStream.close()

    
    
    jarFile*/
    new File("target/spark-bench-1.0-SNAPSHOT.jar")

  }
}

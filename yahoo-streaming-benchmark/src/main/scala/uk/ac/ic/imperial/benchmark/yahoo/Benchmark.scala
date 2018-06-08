package uk.ac.ic.imperial.benchmark.yahoo

import org.apache.spark.sql.SparkSession

trait Benchmark[Runner <: YahooBenchmarkRunner] {
  val spark: SparkSession
  var runner: Runner = _
  val tuplesPerSecond: Long
  val recordGenParallelism: Int
  val rampUpTimeSeconds: Int

  protected val benchmarkParams: Map[String, Any] = Map.empty

  private lazy val allParams = benchmarkParams ++ runner.params ++ Map(
    "tuplesPerSecond" -> tuplesPerSecond,
    "recordGenParallelism" -> recordGenParallelism,
    "rampUpTimeSeconds" -> rampUpTimeSeconds)

  private var readerThread: Thread = _
  protected val dontInterrupt = false

  var stoppedReader = false

  protected val readerWaitTimeMs: Long = 150000 // 5 minutes

  protected def init(): Unit = {
    if (readerThread != null) {
      try {
        if (!dontInterrupt) {
          readerThread.interrupt()
        }
        readerThread.join()
      } catch {
        case _: InterruptedException =>
      }
    }
    stoppedReader = false
  }

  protected def startReader(): Unit

  private def stopReader0(): Unit = {
    if (!stoppedReader) {
      stopReader()
      stoppedReader = true
    }
  }

  protected def stopReader(): Unit

  protected def produceRecords(): Unit

  private def timeIt(stage: String)(f: => Unit): Unit = {
    val start = System.nanoTime
    f
    println(s"$stage took ${(System.nanoTime - start) / 1e9} seconds")
  }

  final def run(runner: Runner, outputPath: String, numRuns: Int = 10): Unit = {
    this.runner = runner
    //    dbutils.fs.put(outputPath.stripSuffix("/") + "/_parameters",
    //      allParams.map { case (key, value) => s"$key: $value" }.mkString("\n"), overwrite = true)
    println("Configuration " + allParams)
    for (i <- 1 to numRuns) {
      timeIt(s"run $i") {
        runOnce(i, outputPath)
      }
      System.gc()
    }
  }

  protected def saveResults(outputPath: String, trial: Int): Unit

  private def runOnce(trial: Int, outputPath: String): Unit = {
    timeIt("initialization") {
      init()
    }
    timeIt("starting reader") {
      readerThread = new Thread("KafkaReader") {
        override def run(): Unit = {
          try {
            startReader()
          } catch {
            case t: Throwable =>
              println(t)
              println(t.getStackTrace.mkString("\n"))
              println(t.getCause)
              throw t
          }
          println("reader exiting")
        }
      }
      readerThread.setDaemon(true)
      readerThread.start()
      Thread.sleep(5000)
    }
    try {
      assert(readerThread.isAlive, "Reader thread died for a reason")
      val producerThread = new Thread("DataProducer") {
        override def run(): Unit = {
          try {
            timeIt("producing records") {
              produceRecords()
            }
          } catch {
            case t: Throwable =>
              println(t)
              println(t.getStackTrace.mkString("\n"))
              println(t.getCause)
              throw t
          }
        }
      }
      producerThread.setDaemon(true)
      producerThread.start()

      val start = System.currentTimeMillis
      while ((System.currentTimeMillis - start) < readerWaitTimeMs && readerThread.isAlive) {
        Thread.sleep(1000)
      }
      stopReader()
      saveResults(outputPath, trial)
    } finally {
      stopReader()
    }
  }
}


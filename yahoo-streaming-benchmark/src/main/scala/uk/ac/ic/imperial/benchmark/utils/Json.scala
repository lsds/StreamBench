package uk.ac.ic.imperial.benchmark.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.reflect._

/**
  * Utility functions for Json munging using Jackson. We use spray for Flink as there is a Jackson version incompatibility between
  * Spark's and Flink's jackson versions. Our tests did not show any significant performance degradation using one over the other.
  */
object Json {
  private val parser: ObjectMapper = new ObjectMapper() with ScalaObjectMapper
  parser.registerModule(DefaultScalaModule)

  def parse[T: ClassTag](json: String): T = parser.readValue(json, classTag[T].runtimeClass).asInstanceOf[T]

  def parseBytes[T: ClassTag](json: Array[Byte]): T = parser.readValue(json, classTag[T].runtimeClass).asInstanceOf[T]

  def getString[T](obj: T): String = parser.writeValueAsString(obj)

  def getBytes[T](obj: T): Array[Byte] = parser.writeValueAsBytes(obj)
}
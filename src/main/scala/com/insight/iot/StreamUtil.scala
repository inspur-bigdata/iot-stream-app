package com.insight.iot

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

/**
 * Created by root on 18-1-25.
 */
object StreamUtil {
  val rand = new Random()

  def createSocketStream(spark: SparkSession, args: Array[String]): DataFrame = {
    val lines = spark.readStream.format("socket").option("host", args(0))
      .option("port", args(1))
      .load()
    lines
  }

  def createMqttStream(spark: SparkSession, brokerUrl: String, topic: String,clientid:String): DataFrame = {
    val lines = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("clientId", clientid+rand.nextInt(10000))
      .option("brokerUrl", brokerUrl)
      .option("topic", topic)
      .load().select("value")
    lines
  }

  def createKafkaStream(spark: SparkSession, bootstrap_servers: String): DataFrame = {
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap_servers)
      .option("subscribe", "things")
      .load().select("value")
    lines
  }

}

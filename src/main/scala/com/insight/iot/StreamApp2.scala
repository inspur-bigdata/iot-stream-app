package com.insight.iot

import java.util.Arrays
import java.sql.Timestamp
import scala.util._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.mqtt.MQTTUtils
import org.apache.spark.sql.Encoders;
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

object StreamApp2 {
  case class Record(name: String, score: Float, time: Long, tags_host: String, tags_rack: String)
  def main(args: Array[String]) {
    val brokerUrl = "tcp://10.110.17.232:1883"
    val topic = "location"
    val sparkConf = new SparkConf().setAppName("JavaMQTT").setMaster("local[3]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate();
    import spark.implicits._
    //根据json解析得到schema
    val events = spark.sparkContext.parallelize(
      """{"time": 1465376157007,"name": "cpu_idle","score": 55,"tags":{"host": "server1","rack": "rack1","other": "something"}}""" :: Nil)
    // read it
    val df = spark.read.json(events)
    val schema = df.schema
    //val schema = StructType(Seq(StructField("time", LongType, true), StructField("name", StringType, true), StructField("score", LongType, true), StructField("tags", StringType, false)))
    //schema.printTreeString()

    val lines = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("clientId", "StreamApp")
      .option("brokerUrl", brokerUrl)
      .option("topic", topic)
      .load().as[(String, Timestamp)]
    //lines.printSchema()
    val df1 = lines.selectExpr("CAST(value AS STRING)").select(from_json($"value", schema).as("data")).select("data.*")
    df1.printSchema()

    //df1.withColumn("value", from_json($"value", schema))

    df1.createOrReplaceTempView("people")
    val rs = spark.sql("SELECT name AS metric, score AS _value, time AS _timestamp ,tags.host AS tags_host FROM people WHERE score > 50")
    //rs.writeStream.outputMode("update").format("console").start().awaitTermination()

    spark.sql("SELECT count(1) FROM people").writeStream.outputMode("update").format("console").start().awaitTermination()
  }
}
package com.insight.iot

import java.util.Arrays;
import scala.util._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.mqtt.MQTTUtils
import org.apache.spark.sql.Encoders;
import org.apache.spark.rdd.RDD

object StreamApp {
  case class Record(name: String, score: Float, time: Long, tags_host: String, tags_rack: String)
  def main(args: Array[String]) {
    val brokerUrl = "tcp://10.110.17.246:1883"
    val topic = "location"
    val sparkConf = new SparkConf().setAppName("JavaMQTT").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))

    // Create direct kafka stream with brokers and topics
    var messages = MQTTUtils.createStream(ssc, brokerUrl, topic)
    messages.foreachRDD((rdd: RDD[String], time: Time) => {
      println(s"========= $time =========")
      if (rdd.count() > 0) {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate();
        import spark.implicits._
        val df = spark.read.json(rdd)
        //df.show();
        df.createOrReplaceTempView("people")
        try {
          val rs = spark.sql("SELECT name AS metric, score AS _value, time AS _timestamp, tags.host, tags.rack FROM people WHERE score > 50")
          rs.show()
        } catch {
          case ex: Throwable => ex.printStackTrace()
        }

      }
    })

    ssc.start();
    ssc.awaitTermination();
  }
}
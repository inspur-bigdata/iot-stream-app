package myspark


import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from MQTT Server.
 *
 * Usage: MQTTStreamWordCount <brokerUrl> <topic>
 * <brokerUrl> and <topic> describe the MQTT server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, a MQTT Server should be up and running.
 *
 */
object MQTTStreamWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: MQTTStreamWordCount <brokerUrl> <topic>") // scalastyle:off println
      System.exit(1)
      //args = Array("tcp://10.110.17.246:1883","location")
    }

    val brokerUrl = args(0)
    val topic = args(1)

    val spark = SparkSession
      .builder
      .appName("MQTTStreamWordCount")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to mqtt server
    val lines = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", topic)
      .load(brokerUrl).as[(String, Timestamp)]
    lines.printSchema()

    val words = lines.map(_._1).flatMap(_.split(",")).selectExpr("value as word")
    words.printSchema()

    words.createOrReplaceTempView("words")
    val rs = spark.sql("SELECT word,count(1) FROM words group by word")

    val query = rs.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
  }
}

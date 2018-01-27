package com.insight.iot

import java.sql.Timestamp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 */

object SqlNetJsonApp {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SqlNetJsonApp").setMaster("local[3]");
    val spark = SparkSession.builder().config(sparkConf).getOrCreate();
    import spark.implicits._

    //根据json解析得到schema
    val events = spark.sparkContext.parallelize(
      """{"time": 1465376157007,"name": "cpu_idle","score": 55,"tags":{"host": "server1","rack": "rack1","other": "something"}}""" :: Nil)
    // read it
    val df = spark.read.json(events)
    val schema = df.schema
    //val schema = StructType(Seq(StructField("time", TimestampType, true), StructField("name", StringType, true), StructField("score", LongType, true), StructField("tags", StringType, false)))
    //schema.printTreeString()

    //val lines = StreamUtil.createSocketStream(spark,args)
    //val lines = StreamUtil.createMqttStream(spark, "tcp://10.110.17.232:1883", "location", "StreamApp")
    val lines = StreamUtil.createKafkaStream(spark, "localhost:9092")

    import org.apache.spark.sql.functions.udf
    //spark sql 自带的 转换 ： $"time" as TimestampType，是秒为单位，而我们的时间是毫秒单位
    def to_millis_timestamp: (Long => Timestamp) = {
      s => new Timestamp(s)
    }
    val millis_time = udf(to_millis_timestamp)
    val df1 = lines.selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", schema).as("data"))
      .select("data.*")
      .select(millis_time($"time") as "time", $"name", $"score", $"tags")
    df1.printSchema()
    df1.createOrReplaceTempView("people")



    //计数统计，输出到控制台
    //spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
    val q0 = spark.sql("SELECT count(1) FROM people")
    val job0 = q0.writeStream.outputMode("complete").format("console")
    job0.start()

    //过滤score>1 and score<100的记录，输出到tsdb
    //spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool2")
    val query1 = spark.sql("SELECT name AS metric, score AS value, cast(time as long) AS timestamp ,tags FROM people WHERE score > 1 and score < 100")
    //val job1 = query1.writeStream.outputMode("append").format("json").option("checkpointLocation", "./tmp-spark-1")
    val job1 = query1.writeStream.outputMode("append").format(classOf[RestStreamSinkProvider].getName).option("restUrl", "http://10.233.104.74:4242/api/put?details").option("checkpointLocation", "./tmp-spark-1")
    job1.start()

    //聚合操作,每隔10s统计窗口数据，并允许5秒的延迟（Append输出）。然后检查value均值大于50的name
    //输出到kafka
    //spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool3")
    val warn_df = df1.withWatermark("time", "5 seconds")
      .groupBy(window($"time", "10 seconds"), $"name")
      .agg(avg("score") as "score_avg")
      .select("name", "window.start", "window.end", "score_avg")
    warn_df.createOrReplaceTempView("thing")
    val q2 = spark.sql("select name as key, string((start,end,name,score_avg)) as value from thing where score_avg >= 30")
    val job2 = q2.writeStream.outputMode("update").format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "iot-warn").option("checkpointLocation", "./tmp-spark-2")
    job2.start()

    spark.streams.awaitAnyTermination()

//----其他测试
    //分组统计count
    val query2 = spark.sql("SELECT name, count(1) FROM people group by name")
    //query2.writeStream.outputMode("complete").format("console").start().awaitTermination()

    //基于time统计某个时间前5秒的数据
    val now_t = new Timestamp(1465376157017L)
    val query3 = spark.sql("SELECT name, count(1) FROM people where time >= '" + now_t + "' group by name ")
    //query3.writeStream.outputMode("update").format("console").start().awaitTermination()

    //每5秒计算一次平均值
    val score_avg = df1.
      groupBy(window($"time", "5 seconds")).
      agg(avg("score") as "score_avg").
      select("window.start", "window.end", "score_avg")
    //score_avg.writeStream.outputMode("complete").format("console").start().awaitTermination()


    //聚合操作,每隔5s统计一个10s窗口数据，并允许20秒的延迟（Append输出）。然后根据name分组统计count值
    val df_agg = df1.withWatermark("time", "20 seconds")
      .groupBy(window($"time", "10 seconds", "5 seconds"), $"name")
      .agg(count("name") as "c_count").
      select("name", "window.start", "window.end", "c_count")
    //df_agg.writeStream.outputMode("update").format("console").start().awaitTermination()
    df_agg.createOrReplaceTempView("thing")
    val q4 = spark.sql("select * from thing where c_count >= 2")
    //q4.writeStream.outputMode("update").format("console").start().awaitTermination()

  }

  def myFunc: (String => String) = { s => s.toLowerCase }
}
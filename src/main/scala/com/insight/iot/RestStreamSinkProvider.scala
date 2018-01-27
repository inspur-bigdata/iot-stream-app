package com.insight.iot

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

import Params.map2Params

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONObject

/**
 * Created by root on 18-1-27.
 */
class RestStreamSinkProvider extends StreamSinkProvider with DataSourceRegister {
  def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {
    new RestStreamSink(parameters.getRequiredString("restUrl"),
      parameters.getInt("maxPacketSize", 10 * 1024 * 1024));
  }

  override def shortName(): String = "restSink"
}


class RestStreamSink(httpPostURL: String, maxPacketSize: Int)
  extends Sink with Logging {
  //println("restUrl=" + httpPostURL)
  val httpClient: CloseableHttpClient = HttpClients.createDefault

  val RETRY_TIMES = 5;
  val SLEEP_TIME = 100;

  override def addBatch(batchId: Long, data: DataFrame) {
    //send data to the HTTP server
    var success = false;
    var retried = 0;
    while (!success && retried < RETRY_TIMES) {
      try {
        retried += 1;
        if (data.count() > 0)
          sendDataFrame(batchId, data, maxPacketSize);
        success = true;
      }
      catch {
        case e: Throwable ⇒ {
          e.printStackTrace()
          success = false;
          super.logWarning(s"failed to send", e);
          if (retried < RETRY_TIMES) {
            val sleepTime = SLEEP_TIME * retried;
            super.logWarning(s"will retry to send after ${sleepTime}ms");
            Thread.sleep(sleepTime);
          }
          else {
            throw e;
          }
        }
      }
    }
  }


  /**
   * send a dataframe to server
   * if the packet is too large, it will be split as several smaller ones
   */
  def sendDataFrame(batchId: Long, dataFrame: DataFrame, maxPacketSize: Int = 10 * 1024 * 1024): Int = {
    //performed on the driver node instead of worker nodes, so use local iterator
    val iter = dataFrame.toLocalIterator;
    val buffer = ArrayBuffer[Row]();
    //可以一次发送多行，提高效率
    var rows = 0;
    val httpPost: HttpPost = new HttpPost(httpPostURL)
    httpPost.setHeader("Content-Type", "application/json")

    val json = dataFrame.toJSON.collectAsList.toString //如果某个批次数据量太大，需要拆分
    println("POST:" + json)
    httpPost.setEntity(new StringEntity(json, ContentType.create("application/json", "utf-8")))
    val response = httpClient.execute(httpPost)
    //释放链接
    response.close();
    assert(response.getStatusLine.getStatusCode == 200)


    /*//逐行转换json，不支持嵌套
    while (iter.hasNext) {
      rows += 1
      val row = iter.next()
      val m = row.getValuesMap(row.schema.fieldNames) //不能嵌套
      val json = JSONObject(m).toString()
      println("row:" + json)

      //try {
      httpPost.setEntity(new StringEntity(json, ContentType.create("application/json", "utf-8")))
      val response = httpClient.execute(httpPost)
      assert(response.getStatusLine.getStatusCode == 200)
      //} catch {
      //  case e: Throwable ⇒ e.printStackTrace()
      //}
    }
    */

    rows
  }
}
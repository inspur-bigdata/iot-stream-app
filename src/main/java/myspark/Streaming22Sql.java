package myspark;

import java.io.IOException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;

public class Streaming22Sql {

	public static String doPostForJson(String url, String jsonParams) {
		CloseableHttpClient httpClient = HttpClients.createDefault();
		HttpPost httpPost = new HttpPost(url);
		/*
		 * RequestConfig requestConfig = RequestConfig.custom().
		 * setConnectTimeout(180 * 1000).setConnectionRequestTimeout(180 * 1000)
		 * .setSocketTimeout(180 * 1000).setRedirectsEnabled(true).build();
		 * httpPost.setConfig(requestConfig);
		 */
		httpPost.setHeader("Content-Type", "application/json");
		try {
			httpPost.setEntity(new StringEntity(jsonParams, ContentType.create("application/json", "utf-8")));
			// System.out.println("request parameters" +
			// EntityUtils.toString(httpPost.getEntity()));
			HttpResponse response = httpClient.execute(httpPost);
			// System.out.println("
			// code:"+response.getStatusLine().getStatusCode());
			// System.out.println("doPostForInfobipUnsub
			// response"+response.getStatusLine().toString());
			return String.valueOf(response.getStatusLine().getStatusCode());
		} catch (Exception e) {
			e.printStackTrace();
			return "post failure :caused by-->" + e.getMessage().toString();
		} finally {
			if (null != httpClient) {
				try {
					httpClient.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

		final String brokerUrl = "tcp://10.110.17.246:1883";
		final String topic = "location";
		final SparkConf sparkConf = new SparkConf().setAppName("JavaMQTT").setMaster("local[3]");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

		// Create direct kafka stream with brokers and topics
		JavaReceiverInputDStream<String> messages = MQTTUtils.createStream(jssc, brokerUrl, topic);

		messages.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			@Override
			public void call(JavaRDD<String> rdd) throws Exception {
				System.out.println("=======" + System.currentTimeMillis() + "=======");
				try {
					if (rdd != null && rdd.count() > 0) {
						SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
						Dataset<Row> ds = spark.read().json(rdd);
						ds.createOrReplaceTempView("people");
						Dataset<Row> result = spark.sql(
								"SELECT name AS metric, score AS _value, time AS _timestamp, tags.host, tags.rack FROM people WHERE score > 50");
						result.show();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		});

		jssc.start();
		jssc.awaitTermination();

	}

}
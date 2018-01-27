package myspark

import java.sql.Timestamp
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import java.util.Properties;

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.json.JSONObject

import scala.util.Random

object KafkaDataGen {
  private[myspark] var rand = new Random()

  def main(args: Array[String]): Unit = {
    val topic = "orders";
    val brokers = "172.18.0.6:6667";
    val props = get_properties(brokers);
    val producer = new KafkaProducer[Int, String](props);
    for (i <- 1 to 10) {
      val now_t: Long = System.currentTimeMillis
      val json: String = DeviceMirror.generate(now_t)
      val record = new ProducerRecord(topic, i, json);
      producer.send(record);
      producer.flush();
      println(json)
      Thread.sleep(rand.nextInt(10))
    }
    producer.close();
  }

  def get_properties(brokers: String): Properties = {
    val props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("acks", "all");
    props.put("retries", "0");
    props.put("batch.size", "16384");
    props.put("linger.ms", "1");
    props.put("buffer.memory", "33554432");
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return props;
  }
}

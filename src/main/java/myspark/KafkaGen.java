package myspark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

//bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic things
public class KafkaGen {
    static final Logger log = LoggerFactory.getLogger(KafkaGen.class);

    public static void main(String[] args) throws InterruptedException {
        String topic = "things";
        //Properties props = get_properties("172.18.0.6:6667");
        Properties props = get_properties("localhost:9092");
        Producer<Integer, String> producer = new KafkaProducer(props);
        Random rand = new Random();
        for (int i = 0; i < 200; i++) {
            Long now_t = System.currentTimeMillis();
            String json = DeviceMirror.generate(now_t);
            ProducerRecord<Integer, String> record = new ProducerRecord(topic, i, json);
            producer.send(record);
            System.out.println(json);
            Thread.sleep(rand.nextInt(1000)); //随机休息
        }
        producer.flush();
        producer.close();

    }

    private static Properties get_properties(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("metadata.broker.list", brokers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}

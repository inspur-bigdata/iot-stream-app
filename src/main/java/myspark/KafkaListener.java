package myspark;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

//bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic things
public class KafkaListener {
    static final Logger log = LoggerFactory.getLogger(KafkaListener.class);
    private static final String GROUP = "KafkaListener";
    private static final List TOPICS = Arrays.asList("iot-warn");

    public static void main(String[] args) throws InterruptedException {
        Properties props = get_properties("localhost:9092");
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        consumer.subscribe(TOPICS);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);//100ms 拉取一次数据
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(" key: " + record.key() + " value: " + record.value() + " partition: " + record.partition());
            }
        }

    }

    private static Properties get_properties(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        //消费者GroupId
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);//自动提交
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        return props;
    }
}

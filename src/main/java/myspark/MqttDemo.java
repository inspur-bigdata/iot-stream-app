package myspark;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import org.json.JSONObject;

public class MqttDemo {

    public static final String HOST = "tcp://10.110.17.232:1883";
    public static final String TOPIC = "location";
    private String userName = "admin";
    private String passWord = "password";

    private ScheduledExecutorService scheduler;

    public static void main(String[] args) throws MqttException {
        sendMsg();
    }


    private static void createTopic() throws MqttException {

    }

    private static void sendMsg() throws MqttException {
        MqttClient client = null;
        try {
            client = createMqttClient(HOST, "client002");

            Random rand = new Random();
            for (int i = 0; i < 10; i++) {
                Long now_t = System.currentTimeMillis();
                String json = DeviceMirror.generate(now_t);
                client.publish(TOPIC, new MqttMessage(json.getBytes()));
                System.out.println(json);
                Thread.sleep(rand.nextInt(500)); //随机休息
            }
            //String str = "{\"time\": 1465376157007,\"name\": \"cpu_idle\",\"score\": 55,\"tags\":{\"host\": \"server1\",\"rack\": \"rack1\",\"other\": \"something\"}}";

            //client.publish(TOPIC, new MqttMessage(jobj.toString().getBytes()));

        } catch (Exception e) {
            e.printStackTrace();
        }
        client.disconnect();
        client.close();
        System.out.println("Send Message end.");
    }

    private void receive() {
        try {
            // host为主机名，clientid即连接MQTT的客户端ID，一般以唯一标识符表示，MemoryPersistence设置clientid的保存形式，默认为以内存保存
            MqttClient client = createMqttClient(HOST, "client002");
            // 订阅消息
            int[] Qos = {1};
            String[] topic1 = {TOPIC};
            client.subscribe(topic1, Qos);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static MqttClient createMqttClient(String host, String clientid) throws MqttException {
        // host为主机名，clientid即连接MQTT的客户端ID，一般以唯一标识符表示，MemoryPersistence设置clientid的保存形式，默认为以内存保存
        MqttClient client = new MqttClient(host, clientid, new MemoryPersistence());
        // MQTT的连接设置
        MqttConnectOptions options = new MqttConnectOptions();
        // 设置是否清空session,这里如果设置为false表示服务器会保留客户端的连接记录，这里设置为true表示每次连接到服务器都以新的身份连接
        options.setCleanSession(true);
        // 设置连接的用户名
        // options.setUserName(userName);
        // 设置连接的密码
        // options.setPassword(passWord.toCharArray());
        // 设置超时时间 单位为秒
        options.setConnectionTimeout(10);
        // 设置会话心跳时间 单位为秒 服务器会每隔1.5*20秒的时间向客户端发送个消息判断客户端是否在线，但这个方法并没有重连的机制
        options.setKeepAliveInterval(20);
        // 设置回调
        // client.setCallback(new PushCallback());
        MqttTopic topic = client.getTopic(TOPIC);
        // setWill方法，如果项目中需要知道客户端是否掉线可以调用该方法。设置最终端口的通知消息
        options.setWill(topic, "close".getBytes(), 2, true);

        client.connect(options);
        return client;
    }

    class PushCallback implements MqttCallback {

        public void connectionLost(Throwable cause) {
            // 连接丢失后，一般在这里面进行重连
            System.out.println("连接断开，可以做重连");
        }

        public void deliveryComplete(IMqttDeliveryToken token) {
            System.out.println("deliveryComplete---------" + token.isComplete());
        }

        public void messageArrived(String topic, MqttMessage message) throws Exception {
            // subscribe后得到的消息会执行到这里面
            System.out.println("接收消息主题 : " + topic);
            System.out.println("接收消息Qos : " + message.getQos());
            System.out.println("接收消息内容 : " + new String(message.getPayload()));
        }
    }
}
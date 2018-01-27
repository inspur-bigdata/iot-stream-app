package myspark;

import org.apache.spark.deploy.worker.Sleeper;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

public class SocketDemo {
    static Random rand = new Random();
    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("localhost", 9999);
        System.out.println("客户端启动成功");
        //获取输出流，向服务器端发送信息
        PrintWriter write = new PrintWriter(socket.getOutputStream());
        //生成数据
        for(int i =0 ;i< 1000;i++) {
            Long now_t = System.currentTimeMillis();
            String json = DeviceMirror.generate(now_t);
            write.println(json);
            write.flush();
            Thread.sleep(rand.nextInt(100)); //随机休息
        }
        socket.close();
    }
}
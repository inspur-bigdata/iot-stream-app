package myspark;

import org.apache.spark.sql.catalyst.expressions.Rand;
import org.json.JSONObject;

import java.util.Random;

/**
 * Created by root on 18-1-25.
 */
public class DeviceMirror {
    private static String[] cpu_names = new String[]{"cpu_idle", "cpu_user", "cpu_sys"};
    //private static String[] disk_names = new String[]{"disk_read", "disk_write"};
    private static String[] hosts = new String[]{"server1", "server2", "server3", "server4", "server5"};
    private static String[] racks = new String[]{"rack1", "rack2"};
    static Random rand = new Random();

    //模拟物联网设备生成数据
    public static String generate(Long time) {
        JSONObject jobj = new JSONObject();
        jobj.put("time", time);
        jobj.put("name", cpu_names[rand.nextInt(cpu_names.length)]);
        jobj.put("score", rand.nextInt(100));
        JSONObject tags = new JSONObject();
        tags.put("host", hosts[rand.nextInt(hosts.length)]);
        tags.put("rack", racks[rand.nextInt(racks.length)]);
        tags.put("other", "something");
        jobj.put("tags", tags);
        //System.out.println(jobj.toString());
        return jobj.toString();
    }
}

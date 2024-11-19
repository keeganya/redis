import com.xiaohu.jedis.util.JedisConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.resps.ScanResult;

import java.util.List;
import java.util.Map;

public class jedisTest {

    private Jedis jedis;

    @BeforeEach
    void setUp(){
        // 建立连接
//         jedis = new Jedis("127.0.0.1", 6379);

        // 连接池建立连接
        jedis = JedisConnectionFactory.getJedis();
        //设置密码
//        jedis.auth("123");

        // 选择库
        jedis.select(0);
    }

    @Test
    public void testString() {
        String result = jedis.set("name", "胡歌");
        System.out.println(result);

        String name = jedis.get("name");
        System.out.println(name);
    }

    @Test
    public void testHash() {
        jedis.hset("user:1", "name", "Jack");
        jedis.hset("user:2","age","21");

        Map<String, String> map = jedis.hgetAll("user:1");
    }

    @AfterEach
    void tearDown(){
        if (jedis != null) {
            jedis.close();
        }
    }

    /** BigKey的危害
     * 网络阻塞：对BigKey执行读请求时，少量的QPS就可能导致带宽使用率被占满，导致Redis实例，乃至所在物理机变慢
     * 数据倾斜：BigKey所在的Redis实例内存使用率远超其他实例，无法是数据分片的内存资源达到均衡
     * Redis阻塞：对元素较多的hash，list，zset等做运算会耗时较久，使主线程被阻塞
     * CPU压力：对BigKey的数据序列化和反序列化会导致CPU的使用率飙升，影响Redis实例和本机其他应用
     * * */
    final static int STR_MAX_LEN = 10 * 1024; // 字符串最大值

    /** hash的entry数量超过 500 时，会使用哈希表而不是ZipList，内存占用较多
     * 可以通过hash-max-ziplist-entries配置entry上限，但是如果entry过多就会导致BigKey问题
     * Hash结构的entry数量不要超过1000
    * */
    final static int HASH_MAX_LEN = 500; // hash结构最大值

    @Test
    void testScan() {
        int maxLen = 0;
        long len = 0;

        String cursor = "0";
        do {
            // 扫描并获取一部分key
            ScanResult<String> result = jedis.scan(cursor);
            // 记录curcor
            cursor = result.getCursor();
            List<String> list = result.getResult();
            if (list == null || list.isEmpty()) {
                break;
            }
            // 遍历
            for (String key : list) {
                // 判断key的类型
                String type = jedis.type(key);
                switch (type) {
                    case "string":
                        len = jedis.strlen(key);
                        maxLen = STR_MAX_LEN;
                        break;
                    case "hash":
                        len = jedis.hlen(key);
                        maxLen = HASH_MAX_LEN;
                        break;
                    case "list":
                        len = jedis.llen(key);
                        maxLen = HASH_MAX_LEN;
                        break;
                    case "set":
                        len = jedis.scard(key);
                        maxLen = HASH_MAX_LEN;
                        break;
                    case "zset":
                        len = jedis.zcard(key);
                        maxLen = HASH_MAX_LEN;
                        break;
                    default:
                        break;
                }
                if (len >= maxLen) {
                    System.out.printf("Found big key : %s, type: %s, length or size: %d %n", key, type, len);
                }
            }
        } while (!cursor.equals("0"));
    }

    /**
     * 批量插入10w条数据
     * */
    @Test
    void testMxx() {
        String[] arr = new String[2000];
        int j;
        long b = System.currentTimeMillis();
        for (int i = 1; i < 100000; i++) {
            j = (i % 1000) << 1; // j = 0,2,4,6,8,...,98
            arr[j] = "test:key_" + i;
            arr[j+1] = "value_" + i;
            if (j == 0) {
                jedis.mset(arr);
            }
        }
        long e = System.currentTimeMillis();
        System.out.println("time: " + (e - b));
    }

    /**
     * Pipeline 批处理
     * 批量插入10w条数据
    * */
    void testPipeline() {
        // 创建管道
        Pipeline pipeline = jedis.pipelined();

        for (int i = 1; i < 100000; i++) {
            // 放入命令到管道
            pipeline.mset("test:key_" + i,"value_" + i);
            if (i % 1000 == 0) {
                // 没放入1000条命令，批量执行
                pipeline.sync();
            }
        }
    }
}

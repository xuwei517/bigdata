package tech.xuwei.redis;

import redis.clients.jedis.Jedis;

/**
 * Redis单连接方式
 * Created by xuwei
 */
public class RedisSingle {
    /**
     * 提示：此代码能够正常执行的前提是：
     * 1. Redis所在服务器的防火墙已关闭
     * 2. redis.conf中的bind参数被指定为192.168.182.103
     * @param args
     */
    public static void main(String[] args) {
        //获取Jedis连接
        Jedis jedis = new Jedis("192.168.182.103",6379);
        //向Redis中添加数据，key=xuwei，value=hello bigdata!
        jedis.set("xuwei","hello bigdata!");
        //从Redis中查询key=xuwei的值
        String value = jedis.get("xuwei");

        System.out.println(value);

        //关闭Jedis连接
        jedis.close();

    }
}

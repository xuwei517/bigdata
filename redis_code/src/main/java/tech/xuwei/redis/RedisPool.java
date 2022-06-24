package tech.xuwei.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis连接池方式
 * Created by xuwei
 */
public class RedisPool {
    public static void main(String[] args) {
        //创建连接池配置对象
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        //连接池中最大空闲连接数
        poolConfig.setMaxIdle(10);
        //连接池中创建的最大连接数
        poolConfig.setMaxTotal(100);
        //创建连接的超时时间
        poolConfig.setMaxWaitMillis(2000);
        //从连接池中获取连接时会先测试一下连接是否可用，这样可以保证取出的连接都是可用的
        poolConfig.setTestOnBorrow(true);

        //获取Jedis连接池
        JedisPool jedisPool = new JedisPool(poolConfig, "192.168.182.103", 6379);

        //从Jedis连接池中取出一个连接
        Jedis jedis = jedisPool.getResource();
        String value = jedis.get("xuwei");
        System.out.println(value);
        //提示：此处的close()方法有两层含义：
        //1. 如果Jedis是直接创建的单连接，则直接关闭这个连接
        //2. 如果Jedis是从连接池中获取的连接，则把这个连接返给连接池
        jedis.close();

        //关闭Jedis连接池
        jedisPool.close();
    }
}

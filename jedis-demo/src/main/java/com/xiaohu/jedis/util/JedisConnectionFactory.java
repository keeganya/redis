package com.xiaohu.jedis.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisConnectionFactory {

    private static final JedisPool jedisPool;

    static {
        // 配置连接池
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        // 设置最大连接数量
        poolConfig.setMaxTotal(8);
        // 设置最大空闲连接数量
        poolConfig.setMaxIdle(8);
        // 设置最小空闲连接数量
        poolConfig.setMinIdle(0);
        // 设置无连接时等待连接的时间
        poolConfig.setMaxWaitMillis(1000);

        // 创建连接池对象
        jedisPool = new JedisPool(poolConfig, "127.0.0.1", 6379, 1000);
    }

    public static Jedis getJedis(){
        return jedisPool.getResource();
    }
}

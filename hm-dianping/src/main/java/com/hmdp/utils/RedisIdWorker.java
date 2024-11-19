package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @Author xiaohu
 * @Date 2024/11/11 16:45
 * @PackageName:com.hmdp.utils
 * @ClassName: RedisIdWorker
 * @Description: 数据库自增id存在的问题：
 *  *               比如订单数据：使用数据库自增id的话，规律太明显，不安全
 *  *               当数据太多了，会需要分表，此时每张表id在自己表内自增会导致id重复问题
 *               给每个业务数据生成一个全局唯一ID
 *               符号位（不变 为0 表示正数）                  时间戳                                     序列号
 *                     0                     00000000 00000000 00000000 00000000     00000000 00000000 00000000 00000000
 *
 * @Version 1.0
 */
@Component
public class RedisIdWorker {
    /**
     *  开始时间戳
     * */
    private static final long BEGIN_TIMESTAMP = 1640995200L;

    /**
     *  序列号位数
     * */
    private static final int COUNT_BITS = 32;

    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String keyPrefix) {
        // 1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;

        // 2.生成序列号
        // 2.1 获取当前日期 精确到天
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        Long count = stringRedisTemplate.opsForValue().increment("icr" + keyPrefix + ":" + date);

        // 3.拼接并返回
        return timestamp << COUNT_BITS | count;
    }
}

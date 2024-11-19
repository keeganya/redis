package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;

/**
 * @Author xiaohu
 * @Date 2024/11/11 15:05
 * @PackageName:com.hmdp.utils
 * @ClassName: CacheClient
 * @Description: 封装写入【带ttl缓存】，【带逻辑过期缓存】，【缓存穿透】，
 * @Version 1.0
 */
@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    // 往redis里面写入不带ttl的数据
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    // 往redis写入带逻辑过期时间的数据
    public void setWithLogicalExpire(String key,Object value,Long time,TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));

        // 写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(value));
    }

    // 解决缓存穿透的读取redis缓存数据
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback ,Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 从redis 读取商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 存在 直接返回
            return JSONUtil.toBean(json,type);
        }
        // 判断是否为空值
        if (json != null) {
            // 缓存的是空字符串‘’ 避免缓存穿透
            return null;
        }
        // 缓存中不存在 根据id查询数据库
        R r = dbFallback.apply(id);

        // 数据库中不存在 返回错误
        if (r == null) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
        }

        // 数据库中存在 写入到redis
        this.set(key,r,time,unit);

        return r;

    }
}

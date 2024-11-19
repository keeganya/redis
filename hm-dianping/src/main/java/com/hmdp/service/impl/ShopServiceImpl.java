package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.User;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author keeganya
 * @since 2023-06-17
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
//        Shop shop = queryWithPassThrough(id);
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);

        // 缓存击穿（互斥锁）
//        Shop shop = queryWithMutex(id);
//        if (shop == null) {
//            return Result.fail("店铺信息不存在！");
//        }
//        Shop shop = queryWithLogicalExpire(id);
        return Result.ok(shop);
    }

    /**
     * 解决 缓存穿透 问题
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;
        // 1.去 redis 查是否存在店铺信息
        String cacheShop = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(cacheShop)) {
            // 3.存在直接返回
            Shop shop = JSONUtil.toBean(cacheShop, Shop.class);
            return shop;
        }
        if (cacheShop != null) {
            return null;
        }
        // 4.不存在去数据库查询店铺信息
        Shop shop = getById(id);
        if (shop == null) {
            // 不存在 redis 缓存一个null 避免缓存穿透
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            // 5.数据库不存在该店铺信息，返回错误
            return null;
        }
        // 6.数据库存在该店铺信息，存入 redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 7.返回店铺信息
        return shop;
    }

    /**
     * 解决 缓存击穿 问题(互斥锁)
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
        // 1.去 redis 查是否存在店铺信息
        String cacheShop = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(cacheShop)) {
            // 3.存在直接返回
            Shop shop = JSONUtil.toBean(cacheShop, Shop.class);
            return shop;
        }
        if (cacheShop != null) {
            return null;
        }
        // 4.实现缓存重建
        // 4.1. 获取互斥锁
        Shop shop = null;
        try {
            Boolean lock = tryLock(LOCK_SHOP_KEY + id);
            // 4.2. 判断是否获取互斥锁成功
            if (!lock) {
                // 4.3。失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 4.4。成功，去数据库查询店铺信息
            shop = getById(id);
            // 模拟重建的延时
            Thread.sleep(200);
            if (shop == null) {
                // 不存在 redis 缓存一个null 避免缓存穿透
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                // 5.数据库不存在该店铺信息，返回错误
                return null;
            }
            // 6.数据库存在该店铺信息，存入 redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 7. 释放互斥锁
            unlock(LOCK_SHOP_KEY + id);
        }
        // 8.返回店铺信息
        return shop;
    }

    /**
     * 尝试获取互斥锁
     */
    private Boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放互斥锁
     */
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    // 创建10个线程的线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 解决 缓存击穿 问题(逻辑过期)
     * @param id
     * @return
     */
    public Shop queryWithLogicalExpire(Long id){
        String key = CACHE_SHOP_KEY + id;
        // 1.去 redis 查是否存在店铺信息
        String cacheShop = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isBlank(cacheShop)) {
            // 3.存在直接返回
            return null;
        }
        // 4.命中，需要把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(cacheShop, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5. 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1 未过期 直接返回店铺信息
            return shop;
        }
        // 5.2 已过期 需要缓存重建
        // 6.实现缓存重建
        // 6.1. 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Boolean isLock = tryLock(lockKey);
        // 6.2 判断是否获取锁成功
        if (isLock) {
            // 6.3 获取锁成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() ->{
                try {
                    // 重建缓存
                    this.saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unlock(lockKey);
                }
            });
        }
        // 6.4.返回过期的店铺信息
        return shop;
    }

    /**
     * 热点 key 事先存入redis
     * @param id , expireSeconds
     * @return
     */
    public void saveShop2Redis(Long id,Long expireSeconds) throws InterruptedException {
        // 1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        // 2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 3.写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY,JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("");
        }
        // 1. 更新数据库
        updateById(shop);
        // 2. 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}

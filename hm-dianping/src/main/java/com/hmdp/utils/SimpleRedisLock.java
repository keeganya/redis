package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @Author xiaohu
 * @Date 2024/11/12 9:18
 * @PackageName:com.hmdp.utils
 * @ClassName: SimpleRedisLock
 * @Description: 分布式锁(基于setnx实现的分布式锁)
 *                存在的问题：
 *                【不可重入】 同一个线程无法多次获取同一把锁; method1()中获取锁之后，去调用method2(),此时method2()中无法获取锁
 *                【不可重试】 获取锁只尝试一次就返回false，没有重试机制
 *                【超时释放】 锁超时释放虽然可以避免死锁，但如果是业务执行耗时较长，锁到期直接自己释放了，存在隐患
 *                【主从一致性】 如果redis是主从集群架构，主从同步存在延迟，当 主redis 宕机时，这时如果 从redis 还未同步成功锁，
 *                            从redis 会被选举为新的 主redis ，就会产生多个线程在访问 新的 主redis 时创建锁的情况
 * @Version 1.0
 */
public class SimpleRedisLock implements ILock{
    // 业务名称 加到redis分布式锁的key中 用来标识不同业务的锁
    private String name;
    private StringRedisTemplate stringRedisTemplate;
    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }
    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";// 拼接在线程id上 区分不同JVM
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    // 在静态代码块中赋值 初始化 （饿汉式单例）类加载时就已经初始化完成了
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        // 获取线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 获取锁
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        // 获取线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 获取锁的value 即线程标识
        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
        // 判断标识是否一致
        /** 解决误删锁的问题
         * 线程1业务阻塞，其锁过期了，
         * 这时线程2获取到锁，然而这时线程1结束，直接删除锁，删掉的是线程2的锁
        * */
        if (threadId.equals(id)) {
            /** 存在问题：
             * 当线程1判断完以后，走到这里，阻塞了 直到锁过期
             * 线程2获取到锁 ，这时线程1阻塞结束，进行下面的删除锁
             * 就会把线程2的锁删掉
             * 解决方案：由于判断和删除锁是两步 不具备原子性
             *          采用执行lua脚本方式保证原子性
            * */
            // 释放锁
            stringRedisTemplate.delete(KEY_PREFIX + name);
        }
    }

    @Override
    public void unlockByLua() {
        // 调用lua脚本
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),// key
                ID_PREFIX + Thread.currentThread().getId() // value
        );
    }
}

package com.hmdp;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * @Author xiaohu
 * @Date 2024/11/12 16:11
 * @PackageName:com.hmdp
 * @ClassName: RedissonTest
 * @Description: redisson 可重入测试类
 * @Version 1.0
 */
@Slf4j
@SpringBootTest
public class RedissonTest {
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private RedissonClient redissonClient1;
    @Resource
    private RedissonClient redissonClient2;
    private RLock lock;

    @BeforeEach
    void setUp() {
//        lock = redissonClient.getLock("order");

       /*
        // 联锁
        RLock lock1 = redissonClient.getLock("order");
        RLock lock2 = redissonClient1.getLock("order");
        RLock lock3 = redissonClient2.getLock("order");
        lock = redissonClient.getMultiLock(lock1, lock2, lock3);
        */
    }

    @Test
    void method1() throws InterruptedException {
        // 尝试获取锁
        boolean isLock = lock.tryLock(1L, TimeUnit.SECONDS);
        if (!isLock) {
            log.error("获取锁失败 .... 1");
            return;
        }
        try {
            log.info("获取锁成功 .... 1");
            method2();
            log.info("开始执行业务 ... 1");
        } finally {
            log.warn("准备释放锁 .... 1");
            lock.unlock();
        }
    }
    void method2() {
        // 尝试获取锁
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("获取锁失败 .... 2");
            return;
        }
        try {
            log.info("获取锁成功 .... 2");
            log.info("开始执行业务 ... 2");
        } finally {
            log.warn("准备释放锁 .... 2");
            lock.unlock();
        }
    }
}

package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author xiaohu
 * @Date 2024/11/12 15:32
 * @PackageName:com.hmdp.config
 * @ClassName: RedissonClient
 * @Description: redisson 配置类
 * @Version 1.0
 */
@Configuration
public class RedissonConfig {
    @Bean
    public RedissonClient redissonClient() {
        // 配置
        Config config = new Config();

        // config.useClusterServers() 添加redis集群地址
        // 添加redis地址
        config.useSingleServer()
                .setAddress("redis://127.0.0.1:6379");
//                .setPassword() 我本地redis没有设置密码

        // 创建RedissonClient对象
        return Redisson.create(config);
    }

   /*
   // 假设有三个redis 这是redis1
    @Bean
    public RedissonClient redissonClient1() {
        // 配置
        Config config = new Config();

        // config.useClusterServers() 添加redis集群地址
        // 添加redis地址
        config.useSingleServer()
                .setAddress("redis://127.0.0.1:6380");
//                .setPassword() 我本地redis没有设置密码

        // 创建RedissonClient对象
        return Redisson.create(config);
    }

    // 这是redis2
    @Bean
    public RedissonClient redissonClient2() {
        // 配置
        Config config = new Config();

        // config.useClusterServers() 添加redis集群地址
        // 添加redis地址
        config.useSingleServer()
                .setAddress("redis://127.0.0.1:6381");
//                .setPassword() 我本地redis没有设置密码

        // 创建RedissonClient对象
        return Redisson.create(config);
    }
    */
}

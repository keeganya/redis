package com.xiaohu.redisdemo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiaohu.redisdemo.pojo.User;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Map;

@SpringBootTest
class RedisDemoApplicationTests {

//    @Autowired
//    private RedisTemplate redisTemplate;
    // 自定义序列化方式
//    private RedisTemplate<String,Object> redisTemplate;


/*    @Test
    void testString() {
        // 写入一条String
        redisTemplate.opsForValue().set("name","胡歌");

        // 获取string数据
        Object name = redisTemplate.opsForValue().get("name");
        System.out.println("name = " + name);
    }

    @Test
    void saveUser(){
        redisTemplate.opsForValue().set("user:4",new User("小明",21));

        User user = (User) redisTemplate.opsForValue().get("user:4");
        System.out.println(user);

    }*/

    // key和value的序列化方式默认为String方式
    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testStringTemplate() throws JsonProcessingException {
        // 准备对象
        User user = new User("小红", 32);
        // 手动序列化
        String json = mapper.writeValueAsString(user);
        // 写入redis
        redisTemplate.opsForValue().set("user:5",json);

        // 读取数据
        String jsonUser = redisTemplate.opsForValue().get("user:5");
        // 手动反序列化
        User user1 = mapper.readValue(jsonUser, User.class);
        System.out.println(user1);
    }

    @Test
    void testHash(){
        redisTemplate.opsForHash().put("user:6","name","小张");
        redisTemplate.opsForHash().put("user:6","age","22");

        Map<Object, Object> entries = redisTemplate.opsForHash().entries("user:6");
        System.out.println(entries);
    }


}

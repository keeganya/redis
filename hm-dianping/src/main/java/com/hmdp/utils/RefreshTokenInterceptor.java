package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RefreshTokenInterceptor implements HandlerInterceptor {

    private StringRedisTemplate redisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response, Object handler) throws Exception {
        /*// 1.获取session
        HttpSession session = request.getSession();
        // 2. 获取session中的用户信息
        Object user = session.getAttribute("user");*/
        // 获取token
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)) {
            // 不存在 拦截
            /*response.setStatus(401);
            return false;*/
            return true;
        }
        // 根据token获取redis中用户信息
        String key = RedisConstants.LOGIN_USER_KEY + token;
        Map<Object, Object> userMap = redisTemplate.opsForHash().entries(key);
        // 3. 判断用户是否存在
       /* if (user == null) {
            // 4. 用户不存在 拦截
           *//*
           response.setStatus(401);
            return false;
            *//*
            throw new Exception("用户不存在");
        }*/
        if (userMap.isEmpty()) {
            /*response.setStatus(401);
            return false;*/
            return true;
        }
        // 5. 存在，保存用户信息到 threadlocal
//        UserHolder.saveUser(((UserDTO) user));
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        UserHolder.saveUser(userDTO);

        // 刷新token有效期
        redisTemplate.expire(key,RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);

        // 6. 放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request,
                                HttpServletResponse response, Object handler, Exception ex) throws Exception {
        // 移除用户
        UserHolder.removeUser();
    }
}

package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class RedisUtils {
    private final StringRedisTemplate stringRedisTemplate;

    public RedisUtils(StringRedisTemplate redisTemplate){
        this.stringRedisTemplate = redisTemplate;
    }


    public void setStringWithTTL(String key,Object value,Long ttl,TimeUnit timeUnit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),ttl,timeUnit);
    }

    public void setStringWithLogicExpireTime(String key, Object value, Long ttl, TimeUnit timeUnit){
        RedisData data = new RedisData();
        data.setData(value);
        data.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(ttl)));
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(data));
    }

    public <T> T getWithPassThrough(String keyPrefix,Long id,Class<T> tClass,
                                    Function<Long,T> dbFallback,Long ttl,TimeUnit timeUnit){
        String jsonStr = stringRedisTemplate.opsForValue().get(keyPrefix+id);

        if(StrUtil.isNotBlank(jsonStr)){
            return JSONUtil.toBean(jsonStr,tClass);
        }

        if(jsonStr != null){
            return null;
        }

        T entity = dbFallback.apply(id);

        if(entity == null){
            stringRedisTemplate.opsForValue().set(keyPrefix+id,"",ttl,timeUnit);
            return null;
        }

        stringRedisTemplate.opsForValue().set(keyPrefix+id,JSONUtil.toJsonStr(entity),ttl,timeUnit);

        return entity;

    }

    public <T> T getWithLogicExpire(String keyPrefix,Long id,Class<T> tClass,
                                    Function<Long,T> dbFallback,Long ttl,TimeUnit timeUnit){

        String jsonStr = stringRedisTemplate.opsForValue().get(keyPrefix+id);

        if(StrUtil.isBlank(jsonStr)){
            return null;
        }

        RedisData redisData = JSONUtil.toBean(jsonStr, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        JSONObject data = (JSONObject) redisData.getData();
        T entity = JSONUtil.toBean(jsonStr,tClass);

        if(expireTime.isAfter(LocalDateTime.now())){
            return entity;
        }
        //尝试加锁，更新缓存，然后返回数据

        return entity;

    }




}

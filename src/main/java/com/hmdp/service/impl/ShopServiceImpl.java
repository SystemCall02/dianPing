package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import com.sun.org.apache.bcel.internal.generic.RETURN;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    @Override
    public Result queryById(Long id) {
        //queryWithPassThrough(id);

        Shop shop = queryWithPassThrough(id);

        if (shop == null){
            return Result.fail("店铺信息不存在");
        }
        return Result.ok(shop);
    }

    private Boolean tryLock(Long id){
        return stringRedisTemplate.opsForValue().setIfAbsent(RedisConstants.LOCK_SHOP_KEY+id,"lock",
                RedisConstants.LOCK_SHOP_TTL,TimeUnit.SECONDS);
    }

    private Boolean unLock(Long id){
        return stringRedisTemplate.delete(RedisConstants.LOCK_SHOP_KEY+id);
    }

    private Shop queryWithPassThrough(Long id){
        String shop = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);

        if(StrUtil.isNotBlank(shop)){
            return JSONUtil.toBean(shop, Shop.class);
        }

        if(shop != null){
            return null;
        }

        Shop sqlShop = getById(id);

        if(sqlShop == null){
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,"",
                    RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);

            return null;
        }

        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(sqlShop)
                ,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return sqlShop;
    }

    private Shop queryWithMutex(Long id){
        String shop = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);

        if(StrUtil.isNotBlank(shop)){
            return JSONUtil.toBean(shop, Shop.class);
        }

        if(shop != null){
            return null;
        }

        Shop sqlShop = null;
        try {
            if(tryLock(id)){
                Thread.sleep(50);
                //递归太狠
                return queryWithMutex(id);
            }

            sqlShop = getById(id);

            if(sqlShop == null){
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,"",
                        RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);

                return null;
            }

            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(sqlShop)
                    ,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            unLock(id);
        }


        return sqlShop;
    }

    private Shop queryWithLogicExpireTime(Long id){
        String jsonStr = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);

        if(StrUtil.isBlank(jsonStr)){
            return null;
        }

        RedisData redisData = JSONUtil.toBean(jsonStr, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        JSONObject data = (JSONObject)redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);

        if(expireTime.isAfter(LocalDateTime.now())){
            return shop;
        }

        if(tryLock(id)){
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    saveShop2Redis(id,30L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    unLock(id);
                }

            });
        }

        return shop;
    }

    private void saveShop2Redis(Long id , Long expireTimeSeconds){
        Shop shop = getById(id);

        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireTimeSeconds));

        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result updateConsistent(Shop shop) {

        Long id = shop.getId();

        if(id == null){
            return Result.fail("店铺id不能为空");
        }

        updateById(shop);

        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY+shop.getId());

        return Result.ok();

    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {

        if (x == null || y == null){
            // 根据类型分页查询
        Page<Shop> page = query()
                .eq("type_id", typeId)
                .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
        // 返回数据
        return Result.ok(page.getRecords());
        }

        int from = (current - 1 ) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance()
                                .limit(end)
                );

        if(results == null)
            return Result.ok(Collections.emptyList());

        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = results.getContent();

        if(content.size() <= from)
            return Result.ok(Collections.emptyList());

        List<Long> ids = new ArrayList<>(content.size());
        Map<String,Distance> distanceMap = new HashMap<>(content.size());
        content.stream().skip(from).forEach(result -> {
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));

            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });

        String idStr = StrUtil.join(",", ids);

        List<Shop> shops = query().in("id", ids)
                .last("ORDER BY FIELD(id " + idStr + " )").list();

        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }

        return Result.ok(shops);
    }
}

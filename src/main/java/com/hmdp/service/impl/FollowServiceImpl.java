package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result follow(Long id, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();

        if(isFollow){
            Follow follow = new Follow();
            follow.setFollowUserId(id);
            follow.setUserId(userId);
            boolean isSuccess = save(follow);
            //存Redis太浪费，仅参考
/*            if (isSuccess){
                String key = "follows:" + userId;
                stringRedisTemplate.opsForSet().add(key,id.toString());
            }*/
        }else{
            remove(new QueryWrapper<Follow>().eq("follow_user_id",id)
                    .eq("user_id",userId));
            //删除Redis中的关注用户，懒得写
        }

        return Result.ok();
    }

    @Override
    public Result isFollow(Long id) {
        Long userId = UserHolder.getUser().getId();

        Integer count = query().eq("user_id", userId).eq("follow_user_id", id).count();

        return Result.ok(count > 0);

    }

    @Override
    public Result followCommon(Long id) {

        Long userId = UserHolder.getUser().getId();


        //使用Redis太浪费，仅参考
/*        String key1 = "follows:" + userId;
        String key2 = "follows:" + id;

        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key1, key2);
        List<UserDTO> users = userService.listByIds(intersect).stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());*/


        Set<Long> followUser1 = query().eq("user_id", userId).list().stream()
                .map(Follow::getFollowUserId)
                .collect(Collectors.toSet());

        Set<Long> followUser2 = query().eq("user_id", id).list().stream()
                .map(Follow::getFollowUserId)
                .collect(Collectors.toSet());

        followUser1.retainAll(followUser2);

        if(followUser1.isEmpty()){
            return Result.ok(Collections.emptyList());
        }

        List<UserDTO> users = userService.listByIds(followUser1).stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        return Result.ok(users);

    }

}

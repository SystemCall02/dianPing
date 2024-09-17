package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
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
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.setBlogAttr(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    private void setBlogAttr(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result queryBlogById(Long id) {

        Blog blog = getById(id);

        if(blog == null){
            return Result.fail("博客不存在");
        }

        setBlogAttr(blog);
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog){
        UserDTO user = UserHolder.getUser();

        if(user == null)
            return;

        Long userId = user.getId();

        String key = RedisConstants.BLOG_LIKED_KEY + blog.getId();

        Double isLike = stringRedisTemplate.opsForZSet().score(key,
                userId.toString());

        blog.setIsLike(isLike != null);
    }

    @Override
    public void likeBlog(Long id) {
        // 修改点赞数量
       /* blogService.update()
                .setSql("liked = liked + 1").eq("id", id).update();*/

        String key = RedisConstants.BLOG_LIKED_KEY + id;

        Long userId = UserHolder.getUser().getId();
        Double isLike = stringRedisTemplate.opsForZSet().score(key,
                userId.toString());

        if(isLike != null){
            boolean isCancel = update().setSql("liked = liked - 1").eq("id", id).update();

            if (isCancel) {
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }
            return;
        }

        boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
        if (isSuccess) {
            stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());

        }

    }

    @Override
    public Result likes(Long id) {
        String key = RedisConstants.BLOG_LIKED_KEY + id;

        Set<String> range = stringRedisTemplate.opsForZSet().range(key, 0, 4);

        if(range == null || range.isEmpty())
            return Result.ok(Collections.emptyList());

        List<Long> ids = range.stream().map(Long::valueOf).collect(Collectors.toList());
        String idsStr = StrUtil.join(",", ids);

        List<UserDTO> users = userService.query().in("id", ids)
                .last("ORDER BY FIELD(id,"+idsStr+")")
                .list().stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        return Result.ok(users);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSave = save(blog);

        if(isSave){
            List<Long> userIds = followService.query().eq("follow_user_id", user.getId()).list()
                    .stream().map(Follow::getUserId).collect(Collectors.toList());

            for (Long userId : userIds) {
                String key = RedisConstants.FEED_KEY + userId;
                stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
            }

        }
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        Long userId = UserHolder.getUser().getId();
        String key = RedisConstants.FEED_KEY + userId;

        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, max, 0, offset, 2);

        if(typedTuples == null || typedTuples.isEmpty())
            return Result.ok();

        int nextOffset = 1;
        long minTime = 0L;
        List<Long> ids = new ArrayList<>(typedTuples.size());
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            ids.add(Long.valueOf(typedTuple.getValue()));
            if(typedTuple.getScore().longValue() == minTime){
                nextOffset++;
            }else{
                minTime = typedTuple.getScore().longValue();
                nextOffset = 1;
            }
        }

        String idStr = StrUtil.join(",", ids);

        List<Blog> blogs = query().in("id", ids)
                .last("ORDER BY FIELD(id " + idStr + " )").list().stream()
                .map(blog -> {
                    setBlogAttr(blog);
                    isBlogLiked(blog);
                    return blog;
                }).collect(Collectors.toList());

        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(nextOffset);

        return Result.ok(scrollResult);


    }
}

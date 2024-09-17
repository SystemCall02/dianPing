package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IFollowService;
import com.hmdp.utils.RedisConstants;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/follow")
public class FollowController {

    @Resource
    private IFollowService followService;

    @PutMapping("/{id}/{isFollow}")
    public Result follow(@PathVariable("id") Long id,@PathVariable("isFollow") Boolean isFollow){
        return followService.follow(id,isFollow);
    }

    @PutMapping("/or/not/{id}")
    public Result follow(@PathVariable("id") Long id){
        return followService.isFollow(id);
    }

    @PutMapping("/follow/{id}")
    public Result followCommon(@PathVariable("id") Long id){
        return followService.followCommon(id);
    }




}

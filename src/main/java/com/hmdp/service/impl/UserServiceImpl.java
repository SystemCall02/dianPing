package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.Editor;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import io.lettuce.core.BitFieldArgs;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import com.hmdp.utils.RedisConstants;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
//@RequiredArgsConstructor
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //验证手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号错误，请检查后重新输入");
        }

        //生成验证码
        String code = RandomUtil.randomNumbers(6);

        //存入session
        //session.setAttribute("code",code);

        //存入Redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);



        //模拟发送验证码
        log.info("code:{}",code);


        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //验证手机号
        if (RegexUtils.isPhoneInvalid(loginForm.getPhone())) {
            return Result.fail("手机号错误，请检查后重新输入");
        }

        String code = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY+loginForm.getPhone());

        if(code == null || !code.equals(loginForm.getCode())){
            return Result.fail("验证码错误");
        }

        User user = query().eq("phone", loginForm.getPhone()).one();

        if(user == null){
            user = new User();
            user.setPhone(loginForm.getPhone());
            user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX +RandomUtil.randomNumbers(8));
            save(user);
        }

        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);

        //session.setAttribute(SystemConstants.SESSION_USER_INFO,userDTO);
        String token = UUID.randomUUID().toString(true);

        Map<String, Object> map = BeanUtil.beanToMap(userDTO, new HashMap<>(), CopyOptions.create().setIgnoreNullValue(true)
                .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));

        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY+token,map);

        stringRedisTemplate.expire(LOGIN_USER_KEY+token,LOGIN_USER_TTL,TimeUnit.MINUTES);



        return Result.ok(token);

    }

    @Override
    public Result sign() {
        Long userId = UserHolder.getUser().getId();

        LocalDateTime now = LocalDateTime.now();

        String keyPost = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));

        String key = USER_SIGN_KEY + userId + keyPost;

        int day = now.getDayOfMonth();

        stringRedisTemplate.opsForValue().setBit(key,day,true);

        return Result.ok();
    }

    @Override
    public Result signCount() {
        Long userId = UserHolder.getUser().getId();

        LocalDateTime now = LocalDateTime.now();

        String keyPost = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));

        String key = USER_SIGN_KEY + userId + keyPost;

        int day = now.getDayOfMonth();

        List<Long> list = stringRedisTemplate.opsForValue()
                .bitField(key, BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(day)).valueAt(0));

        if(list == null || list.isEmpty())
            return Result.ok(0);

        Long bitNUm = list.get(0);

        int count = 0;

        while(true){
            if((bitNUm & 1) == 0){
                break;
            }else{
                count++;
            }

            bitNUm = bitNUm >>> 1;
        }

        return Result.ok(count);

    }
}

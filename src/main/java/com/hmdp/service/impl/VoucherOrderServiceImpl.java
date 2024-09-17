package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;


    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //使用JVM内存储存订单信息，可能出现内存溢出，订单丢失问题（处理时异常）
   // private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    private ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    private IVoucherOrderService proxy;

    //private ExecutorService SECKILL_ORDER_EXECUTOR2 = new ThreadPoolExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable{
        String queueName = "streams.orders";

        @Override
        public void run() {
            while (true){
                try {
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    if(list == null || list.isEmpty()){
                        continue;
                    }

                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                    handleVoucherOrder(voucherOrder);

                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());


                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            String queueName = "streams.order";
            while (true){
                try {
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    if(list == null || list.isEmpty()){
                        break;
                    }

                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                    handleVoucherOrder(voucherOrder);

                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());


                } catch (Exception e) {
                    log.error("处理pendingList订单异常",e);

                }
            }
        }
    }



   /* private class VoucherOrderHandler implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    VoucherOrder voucherOrder = orderTasks.take();

                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                }
            }
        }
    }*/

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //多线程，线程池，不在主线程的任务，无法用ThreadLocal获取用户Id
        Long userId = voucherOrder.getUserId();


        //RedisLock redisLock = new RedisLock("order:"+userId, stringRedisTemplate);
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);

        boolean isLockSuccess = redisLock.tryLock();

        if(!isLockSuccess)
            return;

        try {
            //当前在子线程，同样无法从ThreadLocal中获取到当前代理对象，只能在主线程中获取
           // IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            proxy.stockAndOrder(voucherOrder);
        }finally {
            redisLock.unlock();
        }
    }

    @Override
    public Result buySeckillVoucher(Long voucherId) {

        long orderId = redisIdWorker.nextId("order");

        Long res = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), UserHolder.getUser().getId().toString(),
                String.valueOf(orderId)
        );

        if(res.intValue() != 0){
            return Result.fail(res.intValue() == 1?"库存不足":"不能重复下单");
        }


        proxy = (IVoucherOrderService) AopContext.currentProxy();


        return Result.ok(orderId);
    }

/*    @Override
    public Result buySeckillVoucher(Long voucherId) {
        Long res = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId, UserHolder.getUser().getId()
        );

        if(res.intValue() != 0){
            return Result.fail(res.intValue() == 1?"库存不足":"不能重复下单");
        }

        long orderId = redisIdWorker.nextId("order");

        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(UserHolder.getUser().getId());
        voucherOrder.setVoucherId(voucherId);

        orderTasks.add(voucherOrder);

        proxy = (IVoucherOrderService) AopContext.currentProxy();


        return Result.ok(orderId);
    }*/


  /*  @Override
    public Result buySeckillVoucher(Long voucherId) {
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);

        if(seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())){
            return Result.fail("秒杀尚未开始");
        }

        if(seckillVoucher.getEndTime().isBefore(LocalDateTime.now())){
            return  Result.fail("秒杀已经结束");
        }

        if(seckillVoucher.getStock()<1){
            return Result.fail("已售罄");
        }

        Long userId = UserHolder.getUser().getId();


        synchronized (userId.toString().intern()){
            //该方法非事务方法，直接调用事务方法相当于是用this来调用而非代理
            //意味着事务会失效，需要使用代理对象来调用
            //锁不可以加在下面的方法中，因为会导致锁释放了但事务仍未提交的情况，就会出现线程安全问题
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.stockAndOrder(voucherId, userId);
        }

        RedisLock redisLock = new RedisLock("order:"+userId, stringRedisTemplate);
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);

        boolean isLockSuccess = redisLock.tryLock();

        if(!isLockSuccess)
            return Result.fail("一个用户仅能秒杀一次");

        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.stockAndOrder(voucherId, userId);
        }finally {
            redisLock.unlock();
        }


    }*/

    @Transactional
    public void stockAndOrder(VoucherOrder voucherOrder) {

        Integer count = query().eq("user_id", voucherOrder.getUserId()).count();

        if(count > 0){
            return;
        }

        boolean isSuccess = seckillVoucherService.update().setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock",0)
                .update();

        if(!isSuccess){
            return;
        }

        save(voucherOrder);

    }
}

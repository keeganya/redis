package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
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

    @Override
    public Result streamSeckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        // 1.执行lua脚本 (下单信息放入队列的任务交给lua脚本执行了)
        int result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        ).intValue();

        // 2.判断结果是否为0
        if (result != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }
        // 3.返回订单id
        return Result.ok(orderId);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    /** @PostConstruct注解 通常是初始化的操作，初始化可能依赖于注入其他的组件，所以要等待依赖全部加载完再执行
     *                     依赖加载后，对象使用前执行，（就是这个类初始化之前执行），而且只执行一次
     *                     这里VoucherOrderHandler要在订单秒杀之前执行，因为订单秒杀一开始，就可能会有下单信息进入阻塞队列
     *                     这时候VoucherOrderHandler已经执行，在等待获取阻塞队列中的下单信息了
    * */
    // 队列取消息，执行落库的任务线程
//    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    // 定义一个阻塞队列（阻塞队列必须有放才有取，否则消费者会阻塞等待）
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    // 单独定义一个任务，从队列中取消息，再在数据库中创建订单
    private class VoucherOrderHandler implements Runnable {
        /**
         *  从【阻塞队列】中获取下单信息的任务
        * */
        /*
        @Override
        public void run() {
            while (true){
                try {
                    // 1.获取阻塞队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    createVoucherOrder(voucherOrder); // 操作数据库
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }*/


        /**
         * 从redis的stream【消息队列】中获取下单消息的任务
        * */
        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取redis消息队列中的订单消息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> readList = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );

                    // 2. 判断获取的订单信息是否为空
                    if (readList == null || readList.isEmpty()) {
                        // 如果为空 说明没有消息 继续下一次执行
                        continue;
                    }

                    // 3. 解析数据
                    MapRecord<String, Object, Object> record = readList.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                    // 4. 创建订单
                    createVoucherOrder(voucherOrder);

                    // 5.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge("s1","g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    // 发生异常的处理方案 (pending List中获取消息，Stream 内部有一个队列(pending List)保存每个消费者读取但是还没有执行 ACK 的消息)
                    handlePendingList();
                }
            }
        }
    }

    private void handlePendingList() {
        while (true) {
            try {
                // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 0
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create("stream.orders", ReadOffset.from("0"))
                );

                // 2.判断订单信息是否为空
                if (list == null || list.isEmpty()) {
                    // 如果为null，说明没有异常消息，结束循环
                    break;
                }

                // 解析数据
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> value = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                // 3.创建订单
                createVoucherOrder(voucherOrder);
                // 4.确认消息 XACK
                stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());

            } catch (Exception e){
                log.error("处理订单异常", e);
            }
        }
    }

    private void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if (!isLock) {
            // 获取锁失败，直接返回失败或者重试
            log.error("不允许重复下单！");
            return;
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                log.error("不允许重复下单！");
                return;
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                log.error("库存不足！");
                return;
            }

            // 7.创建订单
            save(voucherOrder);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }


    /**
     * 秒杀业务优化
     * 使用【阻塞队列】，把下单的订单信息放到阻塞队列中，
     * 这个方法内只是会在redis中判断是否库存充足，用户是否下过单，不会把下单信息落库
     * */
    @Override
    public Result optimizeSeckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();

        /** 前提是需要调用新增秒杀券的接口 去新增一种秒杀券，这个接口同步在redis中缓存了秒杀券的库存
         *  在lua脚本里判断 1：库存是否充足，
         *                2：当前用户是否已经下过单
         *                3：库存充足且没有下过单，库存 -1；下单用户的userId保存到orderKey的set中，用于下次判断
        * */
        // 1.执行lua脚本
        int result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        ).intValue();

        // 2.判断结果是否为0
        if (result != 0) {
            // 2.1 等于1 是库存不足 ；等于2 是重复下单
            return Result.fail(result == 1 ? "库存不足" : "重复下单");
        }

        /** 第 1 ，2 ，2.1 步 只是在redis中判断了库存是否不足，和当前用户是否对当前voucherId下单过
         *  并没有真正的落入数据库中
         *  需要把下单的订单消息放到阻塞队列中，等订单落库的任务来从阻塞队列中获取订单信息
        * */

        // 2.2 lua脚本执行结果为0，可以购买秒杀券，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        // 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 用户id
        voucherOrder.setUserId(userId);
        // 代金券id
        voucherOrder.setVoucherId(voucherId);

        // 3.放入阻塞队列
        orderTasks.add(voucherOrder);

        // 4.返回订单id
        return Result.ok(orderId);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        // 2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始");
        }

        // 3.判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 已经结束
            return Result.fail("秒杀已经结束");
        }

        // 4.判断库存是否充足
        if (voucher.getStock() < 1) {
            // 秒杀优惠券库存不足
            return Result.fail("秒杀券库存不足");
        }

        // 通过synchronized锁方式实现一人一单（仅限单体系统）
//        return createVoucherOrderBySync(voucherId);

        // 通过redis分布式锁方式实现一人一单 （分布式系统）存在问题
//        return createVoucherOrderByRedisLock(voucherId);

        // 通过redisson分布式锁实现一人一单
        return createVoucherOrderByRedisson(voucherId);
    }

    // 加synchronized锁实现
    @Transactional
    public Result createVoucherOrderBySync(Long voucherId) {
        /** 一人一单
         *  这里存在问题 多线程进来之后，查询数据库都会是第一次抢优惠券
         *  还是会有一人买多单的情况 所以加synchronized锁
        * */
        Long userId = UserHolder.getUser().getId();
        /** 这里加上synchronized锁之后，对于单体系统可以保证线程依次执行
         *  但是对于分布式系统 每个JVM中都有一个锁监视器
         *  不同线程在不同 JVM 中 还是无法控制住
        * */
        synchronized (userId.toString().intern()) {
            // 查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            if (count > 0) {
                // 用户已经购买过（第一次抢优惠券）
                return Result.fail("您已经购买过该秒杀优惠券");
            }

            // 5.扣减库存
        /* 多线程下，这种方式会超卖
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .update();*/

        /* 失败率上升 没卖完
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .eq("stock",voucher.getStock()) // 同一阶段stock下，多个线程进入发现原stock和现阶段stock不一致（有其他线程扣减了），就会放弃扣减（多个线程扣减失败）
                .update();*/

            // 解决超卖问题
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId)
                    .gt("stock", 0) // 只要stock > 0 就继续扣减
                    .update();

            if (!success) {
                // 扣减失败
                return Result.fail("库存不足");
            }

            // 6.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 6.1 优惠券订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 6.2 用户id
            voucherOrder.setUserId(userId);
            // 6.3 代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 7. 返回订单id
            return Result.ok(orderId);
        }
    }

    // 通过setnx实现分布式锁（也存在问题，见SimpleRedisLock类）
    @Transactional
    public Result createVoucherOrderByRedisLock(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        // 5.1 创建锁对象
        SimpleRedisLock redisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        // 5.2 尝试获取锁
        boolean isLock = redisLock.tryLock(1200);
        // 5.3 判断
        if(!isLock){
            // 获取锁失败，直接返回失败或者重试
            return Result.fail("不允许重复下单！");
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }

            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 7.返回订单id
            return Result.ok(orderId);
        } finally {
            // 释放锁
//            redisLock.unlock();

            // lua脚本方式释放锁
            redisLock.unlockByLua();
        }

    }

    // 通过redisson分布式锁实现
    @Transactional
    public Result createVoucherOrderByRedisson(Long voucherId) {
        // 5. 一人一单
        Long userId = UserHolder.getUser().getId();

        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);

        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        /** 有参数的tryLock()
         *  param1: 获取锁的最大等待时间（期间会重试） 【不传最大等待时间参数，则不会重试获取锁，无论成功/失败。都会立即返回】
         *  param2: 锁自动释放时间 【不传过期时间参数，为-1，默认为30s】 由于看门狗机制 不传该参数时，锁不会过期 传了该参数 看门狗失效
         *                      这是通过getLockWatchdogTimeout()方法获取的，该方法返回的默认值是30秒（30*1000毫秒）
         *                      Watchdog会基于Netty的时间轮启动一个后台任务，定期向Redis发送命令，重新设置锁的过期时间，
         *                      通常是锁的租约时间的1/3。这确保了即使客户端处理时间较长，所持有的锁也不会过期。
         *                      【每次续期的时长】：默认情况下，每10s钟做一次续期，续期时长是30s。（每隔10s看一下，如果当前还持有锁，延长生存时间）
         *                      【停止续期】：当锁被释放或者客户端实例被关闭时，Watchdog会自动停止对应锁的续租任务。
         *  param3：时间单位
        * */
//        boolean isLock = redisLock.tryLock(1, 10, TimeUnit.SECONDS);
        // 判断是否获取锁成功
        if (!isLock) {
            // 获取锁失败
            return Result.fail("不允许重复下单");
        }

        try {
            // 查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId)
                    .gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }

            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 7.返回订单id
            return Result.ok(orderId);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }
}

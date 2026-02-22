# 秒杀系统技术方案

## 一、系统概述

### 1.1 业务场景

秒杀是一种常见的营销活动，特点是：

- 库存有限，限量抢购
- 瞬时高并发，请求峰值极高
- 限购约束，每人仅限购买一次

### 1.2 核心目标

| 目标 | 说明 |
|------|------|
| 不超卖 | 库存扣减不能超过实际库存 |
| 不重复 | 同一用户只能购买一次 |
| 最终一致性 | Redis库存与MySQL库存保持一致 |

### 1.3 技术栈

| 技术 | 用途 |
|------|------|
| Java | 开发语言 |
| MySQL | 持久化存储 |
| Redis | 缓存、限流、库存预扣减 |
| RocketMQ | 异步下单、削峰填谷 |

---

## 二、系统架构

### 2.1 整体流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           秒杀接口                                       │
├─────────────────────────────────────────────────────────────────────────┤
│  1. 限流检查（Redis） → 超出返回"系统繁忙"                               │
│  2. IP/设备防刷（1分钟10次） → 超出返回"操作频繁"                        │
│  3. 原子操作：库存扣减 + 用户标记（Lua脚本）                             │
│     - 库存不足 → 返回"已抢光"                                           │
│     - 用户已购买 → 返回"您已参与"                                       │
│  4. 发送RocketMQ事务消息（Half消息 → 本地事务 → 真实消息）           │
│  5. 发送RocketMQ → 失败自动重试3次                                      │
│     - 事务失败 → 回滚库存+移除用户标记                              │
├─────────────────────────────────────────────────────────────────────────┤
│                           MQ消费                                         │
├─────────────────────────────────────────────────────────────────────────┤
│  6. 检查订单号是否已存在（幂等）                                        │
│  7. MySQL库存扣减（WHERE stock > 0）                                    │
│  8. 创建订单                                                              │
│     - 库存不足 → 回滚Redis库存+用户标记                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                           定时任务                                       │
├─────────────────────────────────────────────────────────────────────────┤
│  - 订单回滚：每1分钟检查超时订单，回滚库存                               │
│  - 库存对账：每5分钟比对Redis/MySQL，不一致自动补偿+告警                 │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心架构图

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  限流层     │ ──▶ │  防刷检查   │ ──▶ │  库存扣减   │ ──▶ │  发送MQ     │
│  (Redis)    │     │  (IP/设备)  │     │  (Lua脚本)  │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                                    │
                                                                    ▼
                                                            ┌─────────────┐
                                                            │  消费消息   │
                                                            │  创建订单   │
                                                            └─────────────┘
                                                                    │
                                                                    ▼
                                                            ┌─────────────┐
                                                            │  库存回滚   │
                                                            │  (定时任务) │
                                                            └─────────────┘
```

---

## 三、Redis Key设计

| Key | 类型 | 说明 |
|-----|------|------|
| `seckill:stock:{skuId}` | String | 库存数量 |
| `seckill:users:{skuId}` | Set | 已下单用户 |
| `seckill:rate_limit:{skuId}` | String | 接口限流 |
| `seckill:ip:{skuId}:{ip}` | String | IP防刷 |
| `seckill:device:{skuId}:{deviceId}` | String | 设备防刷 |

---

## 四、核心流程伪代码

### 4.1 秒杀接口

```java
public SeckillResult seckill(SeckillRequest request) {
    String skuId = request.getSkuId();
    String userId = request.getUserId();
    String ip = request.getIp();
    String deviceId = request.getDeviceId();

    // 1. 限流检查
    if (!checkRateLimit(skuId)) {
        return SeckillResult.systemBusy();
    }

    // 2. IP/设备防刷（1分钟10次）
    if (!checkIpDevice(skuId, ip, deviceId)) {
        return SeckillResult.frequentRequests();
    }

    // 3. 原子操作：库存扣减 + 用户标记
    String stockKey = "seckill:stock:" + skuId;
    String userKey = "seckill:users:" + skuId;

    Long result = redis.eval(ATOMIC_SCRIPT, 2, stockKey, userKey, userId);

    if (result == 0) {
        return SeckillResult.soldOut();
    }
    if (result == -1) {
        return SeckillResult.alreadyPurchased();
    }

    // 4. 发送RocketMQ事务消息
    SeckillMessage msg = new SeckillMessage();
    msg.setOrderId(generateOrderId(skuId, userId));
    msg.setSkuId(skuId);
    msg.setUserId(userId);

    Message message = new Message("seckill_topic", JSON.toJSONString(msg).getBytes());
    message.putUserProperty("orderId", msg.getOrderId());

    try {
        TransactionSendResult result = rocketMQTemplate.sendMessageInTransaction(
            "seckill_group", message, null);
        if (result.getSendStatus() == SendStatus.SEND_OK) {
            return SeckillResult.success(msg.getOrderId());
        }
    } catch (Exception e) {
        log.error("事务消息发送失败", e);
    }

    // 事务失败，回滚
    rollbackStockAndUser(skuId, userId);
    return SeckillResult.systemBusy();
}

/**
 * 生成订单号
 * 格式：SKU_ID + 用户ID哈希 + 时间戳 + 随机数
 */
private String generateOrderId(String skuId, String userId) {
    long timestamp = System.currentTimeMillis();
    int hash = userId.hashCode();
    int random = (int) (Math.random() * 9000) + 1000;
    return String.format("%s_%d_%d_%d", skuId, hash, timestamp, random);
}
```

### 4.2 库存扣减Lua脚本

```java
private static final String ATOMIC_SCRIPT = """
    local stock = redis.call('get', KEYS[1])
    local userKey = KEYS[2]
    local userId = ARGV[1]

    -- 检查库存
    if not stock or tonumber(stock) <= 0 then
        return 0
    end

    -- 检查用户是否已购买
    if redis.call('sismember', userKey, userId) == 1 then
        return -1
    end

    -- 扣减库存 + 标记用户（原子操作）
    redis.call('decr', KEYS[1])
    redis.call('sadd', userKey, userId)

    return 1
    """;
```

### 4.3 限流检查

```java
private boolean checkRateLimit(String skuId) {
    String key = "seckill:rate_limit:" + skuId;
    Long count = redis.incr(key);
    if (count == 1) {
        redis.expire(key, 60); // 60秒窗口
    }
    return count <= 1000; // 每分钟1000请求
}
```

### 4.4 IP/设备防刷

```java
private boolean checkIpDevice(String skuId, String ip, String deviceId) {
    String ipKey = "seckill:ip:" + skuId + ":" + ip;
    String deviceKey = "seckill:device:" + skuId + ":" + deviceId;

    Long ipCount = redis.incr(ipKey);
    if (ipCount == 1) redis.expire(ipKey, 60);
    if (ipCount > 10) return false;

    Long deviceCount = redis.incr(deviceKey);
    if (deviceCount == 1) redis.expire(deviceKey, 60);
    return deviceCount <= 10;
}
```

### 4.5 MQ消费者

```java
@Autowired
private RedisTemplate<String, Object> redisTemplate;

/**
 * 库存回滚Lua脚本（原子操作）
 */
private static final String ROLLBACK_SCRIPT = """
    local stockKey = KEYS[1]
    local userKey = KEYS[2]
    local userId = ARGV[1]

    -- 回滚库存
    redis.call('incr', stockKey)
    -- 移除用户标记
    redis.call('srem', userKey, userId)

    return 1
    """;

@RocketMQListener(topic = "seckill_topic")
public void onMessage(SeckillMessage msg) {
    // 1. 幂等检查
    if (orderMapper.existsByOrderId(msg.getOrderId())) {
        return;
    }

    // 2. MySQL库存扣减（防超卖）
    int affected = productMapper.decrementStock(msg.getSkuId());
    if (affected == 0) {
        // 库存不足，原子回滚Redis（Lua脚本保证原子性）
        rollbackStockAndUser(msg.getSkuId(), msg.getUserId());
        return;
    }

    // 3. 创建订单
    Order order = new Order();
    order.setOrderId(msg.getOrderId());
    order.setUserId(msg.getUserId());
    order.setSkuId(msg.getSkuId());
    order.setStatus(OrderStatus.PENDING_PAY);
    order.setExpireTime(LocalDateTime.now().plusMinutes(15));
    orderMapper.insert(order);
}

/**
 * 原子回滚库存和用户标记
 *
 * @param skuId  商品ID
 * @param userId 用户ID
 * @author lvdaxianer
 */
private void rollbackStockAndUser(String skuId, String userId) {
    String stockKey = "seckill:stock:" + skuId;
    String userKey = "seckill:users:" + skuId;

    DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(ROLLBACK_SCRIPT, Long.class);
    redisTemplate.execute(redisScript, Arrays.asList(stockKey, userKey), userId);
}
```

### 4.6 库存回滚Lua脚本

```java
private static final String ROLLBACK_SCRIPT = """
    local stockKey = KEYS[1]
    local userKey = KEYS[2]
    local userId = ARGV[1]

    redis.call('incr', stockKey)
    redis.call('srem', userKey, userId)

    return 1
    """;
```

### 4.7 RocketMQ事务监听器

```java
@RocketMQTransactionListener(txProducerGroup = "seckill_group")
public class SeckillTransactionListener implements RocketMQLocalTransactionListener {

    @Autowired
    private OrderMapper orderMapper;

    @Autowired
    private ProductMapper productMapper;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        String orderId = msg.getHeaders().get("orderId", String.class);
        try {
            // 1. 解析消息
            SeckillMessage seckillMsg = JSON.parseObject(
                new String(msg.getBody()), SeckillMessage.class);

            // 2. MySQL库存扣减（防超卖）
            int affected = productMapper.decrementStock(seckillMsg.getSkuId());
            if (affected == 0) {
                // 库存不足，回滚Redis
                rollbackStockAndUser(seckillMsg.getSkuId(), seckillMsg.getUserId());
                return RocketMQLocalTransactionState.ROLLBACK;
            }

            // 3. 创建订单
            Order order = new Order();
            order.setOrderId(seckillMsg.getOrderId());
            order.setUserId(seckillMsg.getUserId());
            order.setSkuId(seckillMsg.getSkuId());
            order.setStatus(OrderStatus.PENDING_PAY);
            order.setExpireTime(LocalDateTime.now().plusMinutes(15));
            orderMapper.insert(order);

            return RocketMQLocalTransactionState.COMMIT;
        } catch (Exception e) {
            log.error("本地事务执行失败, orderId: {}", orderId, e);
            return RocketMQLocalTransactionState.ROLLBACK;
        }
    }

    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
        // 事务回查：根据订单号检查本地事务是否成功
        String orderId = msg.getHeaders().get("orderId", String.class);
        try {
            Order order = orderMapper.findByOrderId(orderId);
            if (order != null) {
                return RocketMQLocalTransactionState.COMMIT;
            }
        } catch (Exception e) {
            log.error("事务回查失败, orderId: {}", orderId, e);
        }
        return RocketMQLocalTransactionState.UNKNOWN;
    }

    /**
     * 回滚库存和用户标记（原子操作）
     */
    private void rollbackStockAndUser(String skuId, String userId) {
        String stockKey = "seckill:stock:" + skuId;
        String userKey = "seckill:users:" + skuId;

        String script = """
            if redis.call('sismember', KEYS[2], ARGV[1]) == 0 then
                return 0
            end
            redis.call('incr', KEYS[1])
            redis.call('srem', KEYS[2], ARGV[1])
            return 1
            """;

        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(script, Long.class);
        redisTemplate.execute(redisScript, Arrays.asList(stockKey, userKey), userId);
    }
}
```

---

### 5.1 超时订单回滚

```java
/**
 * 超时订单回滚Lua脚本（Redis原子操作）
 */
private static final String ROLLBACK_EXPIRED_SCRIPT = """
    local stockKey = KEYS[1]
    local userKey = KEYS[2]
    local userId = ARGV[1]

    -- 回滚库存 + 移除用户标记（原子操作）
    redis.call('incr', stockKey)
    redis.call('srem', userKey, userId)

    return 1
    """;

@Scheduled(cron = "0 * * * * ?")
public void rollbackExpiredOrders() {
    List<Order> expiredOrders = orderMapper.findExpiredOrders();
    for (Order order : expiredOrders) {
        orderMapper.updateStatus(order.getOrderId(), OrderStatus.CANCELLED);

        // 回滚MySQL库存
        productMapper.incrementStock(order.getSkuId());

        // 原子回滚Redis（Lua脚本保证原子性）
        String stockKey = "seckill:stock:" + order.getSkuId();
        String userKey = "seckill:users:" + order.getSkuId();
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>(ROLLBACK_EXPIRED_SCRIPT, Long.class);
        redisTemplate.execute(redisScript, Arrays.asList(stockKey, userKey), order.getUserId());
    }
}
```

### 5.2 库存对账+自动补偿

```java
@Scheduled(cron = "0 0/5 * * * ?")
public void stockReconciliation() {
    List<String> skuIds = productMapper.findSeckillSkus();
    for (String skuId : skuIds) {
        long redisStock = Long.parseLong(redis.get("seckill:stock:" + skuId));
        long mysqlStock = productMapper.getStock(skuId);

        if (redisStock != mysqlStock) {
            log.error("库存不一致, skuId:{}, redis:{}, mysql:{}", skuId, redisStock, mysqlStock);

            // 自动补偿
            if (redisStock > mysqlStock) {
                productMapper.setStock(skuId, (int) redisStock);
            } else {
                redis.set("seckill:stock:" + skuId, String.valueOf(mysqlStock));
            }

            // 告警通知
            sendAlert("库存不一致已自动补偿", skuId, redisStock, mysqlStock);
        }
    }
}
```

---

## 六、Redis降级策略

```java
try {
    result = redis.eval(luaScript, keys, args);
} catch (Exception e) {
    log.warn("Redis故障，启用降级策略");
    return deductStockWithDbAndLock(skuId, userId);
}

private boolean deductStockWithDbAndLock(String skuId, String userId) {
    synchronized (skuId.intern()) {
        // 1. 检查用户是否已购买
        if (orderMapper.existsByUserIdAndSkuId(userId, skuId)) {
            return -1;
        }
        // 2. 扣减库存
        int affected = productMapper.decrementStock(skuId);
        if (affected == 0) {
            return 0;
        }
        // 3. 创建本地消息...
        return 1;
    }
}
```

---

## 七、数据库设计

### 7.1 订单表

```sql
CREATE TABLE `order` (
    `id` BIGINT PRIMARY KEY AUTO_INCREMENT,
    `order_id` VARCHAR(64) NOT NULL COMMENT '订单号',
    `user_id` VARCHAR(32) NOT NULL COMMENT '用户ID',
    `sku_id` VARCHAR(32) NOT NULL COMMENT '商品ID',
    `status` INT NOT NULL DEFAULT 0 COMMENT '订单状态 0:待支付 1:已支付 2:已取消',
    `expire_time` DATETIME COMMENT '过期时间',
    `created_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `updated_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_order_id` (`order_id`),
    KEY `idx_user_sku` (`user_id`, `sku_id`),
    KEY `idx_expire_time` (`expire_time`)
) COMMENT '订单表';
```

### 7.2 本地消息表（可选，保留用于Debug）

```sql
CREATE TABLE `local_message` (
    `id` BIGINT PRIMARY KEY AUTO_INCREMENT,
    `order_id` VARCHAR(64) NOT NULL COMMENT '订单号',
    `sku_id` VARCHAR(32) NOT NULL COMMENT '商品ID',
    `user_id` VARCHAR(32) NOT NULL COMMENT '用户ID',
    `status` INT NOT NULL DEFAULT 0 COMMENT '状态 0:待发送 1:已发送 -1:失败',
    `created_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `updated_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_order_id` (`order_id`)
) COMMENT '本地消息表（已废弃，仅用于Debug）';
```

> **注意**：采用RocketMQ事务消息后，本地消息表已不再需要，可保留用于问题排查。

---

## 八、核心场景解决方案

### 8.1 超卖问题

#### 问题描述
在并发场景下，多个请求同时读取到库存为1，都执行扣减操作，导致超卖。

#### 解决方案
采用多层防护：

**第一层：Redis Lua脚本防护**
```lua
-- 检查库存大于0才扣减
if not stock or tonumber(stock) <= 0 then
    return 0
end
redis.call('decr', KEYS[1])
```

**第二层：MySQL乐观锁防护**
```java
@Update("UPDATE product SET stock = stock - 1, version = version + 1 " +
        "WHERE sku_id = #{skuId} AND stock > 0 AND version = #{version}")
int decrementStockWithVersion(SkuStockDto dto);
```
- 使用 `WHERE stock > 0` 确保库存足够才扣减
- 使用 `version` 乐观锁防止并发更新冲突

**第三层：库存对账+告警**
- 每5分钟比对Redis和MySQL库存
- 发现不一致自动补偿
- 发送告警通知人工介入

---

### 8.2 一人一单

#### 问题描述
同一用户可能通过多线程并发请求，多次抢购成功。

#### 解决方案
采用Redis原子操作：

```lua
-- 检查用户是否已购买
if redis.call('sismember', userKey, userId) == 1 then
    return -1
end
-- 标记用户已购买
redis.call('sadd', userKey, userId)
```

关键点：
- 使用 `SISMEMBER` 检查用户是否已购买（O(1)复杂度）
- 检查和标记在同一Lua脚本中，保证原子性
- Redis Set数据结构，天然支持去重

兜底方案：
- MySQL订单表有联合索引 `idx_user_sku(user_id, sku_id)`
- 即使Redis有问题，数据库层面也不会出现重复订单

---

### 8.3 Redis与MySQL库存最终一致性

#### 问题描述
秒杀过程中，Redis作为前置扣减，MySQL作为最终落库，两者可能存在数据不一致。

#### 一致性保障机制

| 阶段 | 一致性保障 |
|------|------------|
| 库存预热 | 启动时从MySQL同步到Redis |
| 扣减阶段 | Redis先扣，MQ异步落库MySQL |
| 消费阶段 | MySQL扣减失败则回滚Redis |
| 超时订单 | 回滚时同时更新Redis和MySQL |
| 定时对账 | 每5分钟比对，不一致自动补偿 |

#### 详细流程

```
正常流程：
1. Redis库存扣减成功 ✓
2. 发送MQ消息 ✓
3. MQ消费者扣减MySQL库存 ✓
4. 创建订单 ✓
5. 一致 ✓

异常流程-MQ消费失败：
1. Redis库存扣减成功 ✓
2. 发送MQ消息成功 ✓
3. MQ消费者扣减MySQL库存失败（库存不足）
4. 回滚Redis库存 ✓
5. 一致 ✓

异常流程-订单超时未支付：
1. Redis库存扣减成功 ✓
2. 扣减MySQL库存 ✓
3. 创建订单 ✓
4. 15分钟未支付
5. 定时任务回滚Redis+MySQL ✓
6. 一致 ✓
```

---

### 8.4 事务消息方案

#### 问题描述
秒杀流程中，需要保证Redis扣减、MySQL下单、MQ消息的分布式事务一致性。

#### 解决方案：RocketMQ事务消息

RocketMQ事务消息通过"半消息(Half Message)"机制保证本地事务与消息发送的最终一致性。

**事务消息流程**
```
┌─────────────┐     Half消息      ┌─────────────┐
│   秒杀请求   │ ──────────────▶ │  RocketMQ   │
│             │    (预处理)       │   事务日志   │
└──────┬──────┘                  └──────┬──────┘
       │                                 │
       ▼                                 │
┌─────────────┐                          │
│  本地事务   │                          │
│ (扣减MySQL  │                          │
│  创建订单)  │                          │
└──────┬──────┘                          │
       │   提交成功                       │
       ├─────────────────────────────────┤
       │                                 │
       ▼                         发送真实消息
                          ┌─────────────┐
                          │  消费者订阅  │
                          └─────────────┘
```

**事务消息状态机**
```
┌──────────┐   提交成功     ┌──────────┐
│  Half   │ ────────────▶ │  Commit  │
│  消息   │                │  (已提交) │
└──────────┘               └──────────┘
       │                          │
       │   回滚/未知              │
       ├──────────────────────────┤
       │                          │
       ▼                          ▼
┌──────────┐               ┌──────────┐
│  Rollback│               │  Commit   │
│  (已回滚)│               │ (已提交)  │
└──────────┘               └──────────┘
```

**事务监听器执行流程**
```
1. 发送Half消息成功
2. 执行本地事务（MySQL扣减库存+创建订单）
   - 成功 → 返回COMMIT，RocketMQ发送真实消息
   - 失败 → 返回ROLLBACK，消息作废
   - 超时/异常 → 返回UNKNOWN，触发回查
3. 回查：检查订单是否存在
   - 订单存在 → 返回COMMIT
   - 订单不存在 → 返回ROLLBACK
```

**关键代码见 4.7 节**

**方案对比**

| 对比项 | 本地消息表+补偿 | RocketMQ事务消息 |
|--------|-----------------|------------------|
| 实现复杂度 | 中（需维护消息表+定时任务） | 低（MQ原生支持） |
| 一致性 | 最终一致性 | 最终一致性 |
| 延迟 | 定时补偿有延迟 | 实时性好 |
| 可靠性 | 消息表+补偿双重保障 | 依赖MQ稳定性 |
| 运维成本 | 需监控消息表 | RocketMQ高可用 |

**适用场景建议**
- 小规模秒杀：本地消息表方案足够，简单可控
- 大规模秒杀：RocketMQ事务消息更优，减少延迟和运维成本

---

### 8.5 防刷策略

#### 问题描述
黄牛或恶意用户通过脚本刷接口，抢占正常用户资源。

#### 解决方案：多维度限流

| 维度 | 限制 | 粒度 |
|------|------|------|
| 全局限流 | 1000请求/分钟 | 每个SKU |
| IP防刷 | 10次/分钟 | IP+SKU |
| 设备防刷 | 10次/分钟 | 设备ID+SKU |

```java
// IP防刷
String ipKey = "seckill:ip:" + skuId + ":" + ip;
Long ipCount = redis.incr(ipKey);
if (ipCount == 1) redis.expire(ipKey, 60);
if (ipCount > 10) return false;

// 设备防刷
String deviceKey = "seckill:device:" + skuId + ":" + deviceId;
Long deviceCount = redis.incr(deviceKey);
if (deviceCount == 1) redis.expire(deviceKey, 60);
if (deviceCount > 10) return false;
```

---

### 8.6 Redis降级策略

#### 问题描述
Redis故障时，整个秒杀系统不可用。

#### 解决方案：降级查MySQL

```java
try {
    result = redis.eval(luaScript, keys, args);
} catch (Exception e) {
    log.warn("Redis故障，启用降级策略");
    return deductStockWithDbAndLock(skuId, userId);
}

private boolean deductStockWithDbAndLock(String skuId, String userId) {
    synchronized (skuId.intern()) {
        // 1. 检查用户是否已购买
        if (orderMapper.existsByUserIdAndSkuId(userId, skuId)) {
            return -1;
        }
        // 2. 扣减库存（MySQL乐观锁）
        int affected = productMapper.decrementStock(skuId);
        if (affected == 0) {
            return 0;
        }
        // 3. 创建本地消息记录...
        return 1;
    }
}
```

降级注意事项：
- 使用synchronized同步块保护（单机降级）
- 降级期间性能降低，但保证基本可用
- Redis恢复后自动切回

---

## 九、关键技术点总结

| 技术点 | 说明 |
|--------|------|
| Lua脚本 | 保证Redis库存扣减和用户标记的原子性 |
| Redis Set | O(1)复杂度检查用户是否已购买 |
| 限流防刷 | 固定窗口限流 + IP/设备维度防刷 |
| MQ异步 | 削峰填谷，保护MySQL |
| RocketMQ事务消息 | Half消息机制保证本地事务与消息一致性 |
| 幂等设计 | 订单号唯一索引防止重复创建 |
| 定时任务 | 订单超时回滚 + 库存对账 |
| 降级策略 | Redis故障时降级查MySQL |

---

## 十、确认清单

| 序号 | 项 | 值 |
|------|-----|-----|
| 1 | 开发语言 | Java + MySQL + Redis + RocketMQ |
| 2 | 限流 | 每分钟每商品1000请求 |
| 3 | 防刷 | IP/设备1分钟10次 |
| 4 | 库存扣减 | Lua脚本原子操作 + MySQL WHERE stock > 0 |
| 5 | 重复购买 | Redis Set + Lua脚本检查 |
| 6 | 消息可靠性 | RocketMQ事务消息（Half消息+回查） |
| 7 | 消费幂等 | 订单号唯一索引 |
| 8 | 降级策略 | Redis故障降级查MySQL |
| 9 | 订单超时 | 15分钟未支付回滚库存 |
| 10 | 库存对账 | 每5分钟自动补偿+告警 |
| 11 | 多商品 | 支持 |

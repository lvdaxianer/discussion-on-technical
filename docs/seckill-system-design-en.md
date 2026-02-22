# Seckill System Technical Design

## 1. System Overview

### 1.1 Business Scenario

Seckill (flash sale) is a common marketing activity characterized by:

- Limited inventory, limited quantity purchase
- Instant high concurrency, extreme request peaks
- Purchase limit, each user can only buy once

### 1.2 Core Objectives

| Objective | Description |
|-----------|-------------|
| No overselling | Inventory deduction cannot exceed actual inventory |
| No duplicates | Same user can only purchase once |
| Eventual consistency | Redis inventory and MySQL inventory remain consistent |

### 1.3 Technology Stack

| Technology | Usage |
|------------|-------|
| Java | Development language |
| MySQL | Persistent storage |
| Redis | Caching, rate limiting, inventory pre-deduction |
| RocketMQ | Async order creation, peak shaving |

---

## 2. System Architecture

### 2.1 Overall Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Seckill API                                    │
├─────────────────────────────────────────────────────────────────────────┤
│  1. Rate limit check (Redis) → return "System Busy" if exceeded         │
│  2. IP/Device anti-brush (10 times/min) → return "Too Frequent"         │
│  3. Atomic operation: inventory deduction + user marking (Lua script)  │
│     - Insufficient inventory → return "Sold Out"                        │
│     - User already purchased → return "Already Participated"           │
│  4. Send RocketMQ transaction message (Half message → Local transaction → Real message) │
│  5. Send RocketMQ → auto retry 3 times on failure                       │
│     - Transaction failed → rollback inventory + remove user mark         │
├─────────────────────────────────────────────────────────────────────────┤
│                           MQ Consumer                                    │
├─────────────────────────────────────────────────────────────────────────┤
│  6. Check if order ID exists (idempotent)                               │
│  7. MySQL inventory deduction (WHERE stock > 0)                        │
│  8. Create order                                                         │
│     - Insufficient inventory → rollback Redis inventory + user mark    │
├─────────────────────────────────────────────────────────────────────────┤
│                           Scheduled Tasks                                │
├─────────────────────────────────────────────────────────────────────────┤
│  - Order rollback: check expired orders every 1min, rollback inventory │
│  - Inventory reconciliation: compare Redis/MySQL every 5min,          │
│    auto compensate + alert if inconsistent                             │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Core Architecture Diagram

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Rate Limit │ ──▶ │ Anti-Brush  │ ──▶ │ Inventory   │ ──▶ │  Send MQ    │
│  (Redis)    │     │ (IP/Device) │     │ (Lua Script)│     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                                    │
                                                                    ▼
                                                            ┌─────────────┐
                                                            │  Consume    │
                                                            │  Create Order│
                                                            └─────────────┘
                                                                    │
                                                                    ▼
                                                            ┌─────────────┐
                                                            │  Inventory  │
                                                            │  Rollback   │
                                                            │  (Scheduler)│
                                                            └─────────────┘
```

---

## 3. Redis Key Design

| Key | Type | Description |
|-----|------|-------------|
| `seckill:stock:{skuId}` | String | Inventory count |
| `seckill:users:{skuId}` | Set | Users who have placed orders |
| `seckill:rate_limit:{skuId}` | String | API rate limit |
| `seckill:ip:{skuId}:{ip}` | String | IP anti-brush |
| `seckill:device:{skuId}:{deviceId}` | String | Device anti-brush |

---

## 4. Core Flow Pseudocode

### 4.1 Seckill API

```java
public SeckillResult seckill(SeckillRequest request) {
    String skuId = request.getSkuId();
    String userId = request.getUserId();
    String ip = request.getIp();
    String deviceId = request.getDeviceId();

    // 1. Rate limit check
    if (!checkRateLimit(skuId)) {
        return SeckillResult.systemBusy();
    }

    // 2. IP/Device anti-brush (10 times/min)
    if (!checkIpDevice(skuId, ip, deviceId)) {
        return SeckillResult.frequentRequests();
    }

    // 3. Atomic operation: inventory deduction + user marking
    String stockKey = "seckill:stock:" + skuId;
    String userKey = "seckill:users:" + skuId;

    Long result = redis.eval(ATOMIC_SCRIPT, 2, stockKey, userKey, userId);

    if (result == 0) {
        return SeckillResult.soldOut();
    }
    if (result == -1) {
        return SeckillResult.alreadyPurchased();
    }

    // 4. Send RocketMQ transaction message
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
        log.error("Transaction message send failed", e);
    }

    // Transaction failed, rollback
    rollbackStockAndUser(skuId, userId);
    return SeckillResult.systemBusy();
}

/**
 * Generate order ID
 * Format: SKU_ID + userId hash + timestamp + random number
 */
private String generateOrderId(String skuId, String userId) {
    long timestamp = System.currentTimeMillis();
    int hash = userId.hashCode();
    int random = (int) (Math.random() * 9000) + 1000;
    return String.format("%s_%d_%d_%d", skuId, hash, timestamp, random);
}
```

### 4.2 Inventory Deduction Lua Script

```java
private static final String ATOMIC_SCRIPT = """
    local stock = redis.call('get', KEYS[1])
    local userKey = KEYS[2]
    local userId = ARGV[1]

    -- Check inventory
    if not stock or tonumber(stock) <= 0 then
        return 0
    end

    -- Check if user already purchased
    if redis.call('sismember', userKey, userId) == 1 then
        return -1
    end

    -- Deduct inventory + mark user (atomic operation)
    redis.call('decr', KEYS[1])
    redis.call('sadd', userKey, userId)

    return 1
    """;
```

### 4.3 Rate Limit Check

```java
private boolean checkRateLimit(String skuId) {
    String key = "seckill:rate_limit:" + skuId;
    Long count = redis.incr(key);
    if (count == 1) {
        redis.expire(key, 60); // 60 second window
    }
    return count <= 1000; // 1000 requests per minute
}
```

### 4.4 IP/Device Anti-Brush

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

### 4.5 MQ Consumer

```java
@RocketMQListener(topic = "seckill_topic")
public void onMessage(SeckillMessage msg) {
    // 1. Idempotent check
    if (orderMapper.existsByOrderId(msg.getOrderId())) {
        return;
    }

    // 2. MySQL inventory deduction (prevent overselling)
    int affected = productMapper.decrementStock(msg.getSkuId());
    if (affected == 0) {
        // Insufficient inventory, rollback Redis
        redis.incr("seckill:stock:" + msg.getSkuId());
        redis.srem("seckill:users:" + msg.getSkuId(), msg.getUserId());
        return;
    }

    // 3. Create order
    Order order = new Order();
    order.setOrderId(msg.getOrderId());
    order.setUserId(msg.getUserId());
    order.setSkuId(msg.getSkuId());
    order.setStatus(OrderStatus.PENDING_PAY);
    order.setExpireTime(LocalDateTime.now().plusMinutes(15));
    orderMapper.insert(order);
}
```

### 4.6 Inventory Rollback Lua Script

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

### 4.7 RocketMQ Transaction Listener

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
            // 1. Parse message
            SeckillMessage seckillMsg = JSON.parseObject(
                new String(msg.getBody()), SeckillMessage.class);

            // 2. MySQL inventory deduction (prevent overselling)
            int affected = productMapper.decrementStock(seckillMsg.getSkuId());
            if (affected == 0) {
                // Insufficient inventory, rollback Redis
                rollbackStockAndUser(seckillMsg.getSkuId(), seckillMsg.getUserId());
                return RocketMQLocalTransactionState.ROLLBACK;
            }

            // 3. Create order
            Order order = new Order();
            order.setOrderId(seckillMsg.getOrderId());
            order.setUserId(seckillMsg.getUserId());
            order.setSkuId(seckillMsg.getSkuId());
            order.setStatus(OrderStatus.PENDING_PAY);
            order.setExpireTime(LocalDateTime.now().plusMinutes(15));
            orderMapper.insert(order);

            return RocketMQLocalTransactionState.COMMIT;
        } catch (Exception e) {
            log.error("Local transaction execution failed, orderId: {}", orderId, e);
            return RocketMQLocalTransactionState.ROLLBACK;
        }
    }

    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
        // Transaction check: verify if local transaction succeeded based on order ID
        String orderId = msg.getHeaders().get("orderId", String.class);
        try {
            Order order = orderMapper.findByOrderId(orderId);
            if (order != null) {
                return RocketMQLocalTransactionState.COMMIT;
            }
        } catch (Exception e) {
            log.error("Transaction check failed, orderId: {}", orderId, e);
        }
        return RocketMQLocalTransactionState.UNKNOWN;
    }

    /**
     * Rollback inventory and user mark (atomic operation)
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

## 5. Scheduled Tasks

### 5.1 Expired Order Rollback

```java
@Scheduled(cron = "0 * * * * ?")
public void rollbackExpiredOrders() {
    List<Order> expiredOrders = orderMapper.findExpiredOrders();
    for (Order order : expiredOrders) {
        orderMapper.updateStatus(order.getOrderId(), OrderStatus.CANCELLED);

        // Rollback inventory
        redis.incr("seckill:stock:" + order.getSkuId());
        productMapper.incrementStock(order.getSkuId());

        // Remove user mark
        redis.srem("seckill:users:" + order.getSkuId(), order.getUserId());
    }
}
```

### 5.2 Inventory Reconciliation + Auto Compensation

```java
@Scheduled(cron = "0 0/5 * * * ?")
public void stockReconciliation() {
    List<String> skuIds = productMapper.findSeckillSkus();
    for (String skuId : skuIds) {
        long redisStock = Long.parseLong(redis.get("seckill:stock:" + skuId));
        long mysqlStock = productMapper.getStock(skuId);

        if (redisStock != mysqlStock) {
            log.error("Inventory inconsistent, skuId:{}, redis:{}, mysql:{}", skuId, redisStock, mysqlStock);

            // Auto compensate
            if (redisStock > mysqlStock) {
                productMapper.setStock(skuId, (int) redisStock);
            } else {
                redis.set("seckill:stock:" + skuId, String.valueOf(mysqlStock));
            }

            // Alert notification
            sendAlert("Inventory inconsistency auto compensated", skuId, redisStock, mysqlStock);
        }
    }
}
```

---

## 6. Redis Degradation Strategy

```java
try {
    result = redis.eval(luaScript, keys, args);
} catch (Exception e) {
    log.warn("Redis failed, enabling degradation strategy");
    return deductStockWithDbAndLock(skuId, userId);
}

private boolean deductStockWithDbAndLock(String skuId, String userId) {
    synchronized (skuId.intern()) {
        // 1. Check if user already purchased
        if (orderMapper.existsByUserIdAndSkuId(userId, skuId)) {
            return -1;
        }
        // 2. Deduct inventory
        int affected = productMapper.decrementStock(skuId);
        if (affected == 0) {
            return 0;
        }
        // 3. Create local message...
        return 1;
    }
}
```

---

## 7. Database Design

### 7.1 Order Table

```sql
CREATE TABLE `order` (
    `id` BIGINT PRIMARY KEY AUTO_INCREMENT,
    `order_id` VARCHAR(64) NOT NULL COMMENT 'Order ID',
    `user_id` VARCHAR(32) NOT NULL COMMENT 'User ID',
    `sku_id` VARCHAR(32) NOT NULL COMMENT 'Product ID',
    `status` INT NOT NULL DEFAULT 0 COMMENT 'Order status 0:pending payment 1:paid 2:cancelled',
    `expire_time` DATETIME COMMENT 'Expire time',
    `created_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `updated_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_order_id` (`order_id`),
    KEY `idx_user_sku` (`user_id`, `sku_id`),
    KEY `idx_expire_time` (`expire_time`)
) COMMENT 'Order table';
```

### 7.2 Local Message Table (Optional, retained for Debug)

```sql
CREATE TABLE `local_message` (
    `id` BIGINT PRIMARY KEY AUTO_INCREMENT,
    `order_id` VARCHAR(64) NOT NULL COMMENT 'Order ID',
    `sku_id` VARCHAR(32) NOT NULL COMMENT 'Product ID',
    `user_id` VARCHAR(32) NOT NULL COMMENT 'User ID',
    `status` INT NOT NULL DEFAULT 0 COMMENT 'Status 0:to be sent 1:sent -1:failed',
    `created_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `updated_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_order_id` (`order_id`)
) COMMENT 'Local message table (deprecated, only for debugging)';
```

> **Note**: After adopting RocketMQ transaction messages, local message table is no longer required. It can be retained for issue investigation.

---

## 8. Core Scenario Solutions

### 8.1 Overselling Problem

#### Problem Description
In high concurrency scenarios, multiple requests may read inventory as 1 simultaneously and all execute deduction, causing overselling.

#### Solution
Multi-layer protection:

**Layer 1: Redis Lua Script Protection**
```lua
-- Check inventory > 0 before deduction
if not stock or tonumber(stock) <= 0 then
    return 0
end
redis.call('decr', KEYS[1])
```

**Layer 2: MySQL Optimistic Lock Protection**
```java
@Update("UPDATE product SET stock = stock - 1, version = version + 1 " +
        "WHERE sku_id = #{skuId} AND stock > 0 AND version = #{version}")
int decrementStockWithVersion(SkuStockDto dto);
```
- Use `WHERE stock > 0` to ensure sufficient inventory before deduction
- Use `version` optimistic lock to prevent concurrent update conflicts

**Layer 3: Inventory Reconciliation + Alert**
- Compare Redis and MySQL inventory every 5 minutes
- Auto compensate when inconsistency found
- Send alert notification for manual intervention

---

### 8.2 One User One Order

#### Problem Description
The same user may use multiple threads to make concurrent requests and purchase multiple times.

#### Solution
Use Redis atomic operation:

```lua
-- Check if user already purchased
if redis.call('sismember', userKey, userId) == 1 then
    return -1
end
-- Mark user as purchased
redis.call('sadd', userKey, userId)
```

Key points:
- Use `SISMEMBER` to check if user purchased (O(1) complexity)
- Check and mark in the same Lua script, ensuring atomicity
- Redis Set data structure naturally supports deduplication

Fallback solution:
- MySQL order table has composite index `idx_user_sku(user_id, sku_id)`
- Even if Redis fails, database level will not have duplicate orders

---

### 8.3 Redis and MySQL Inventory Eventual Consistency

#### Problem Description
During seckill, Redis acts as frontend deduction and MySQL as final persistence, there may be data inconsistency between them.

#### Consistency Guarantee Mechanism

| Stage | Consistency Guarantee |
|-------|----------------------|
| Inventory preload | Sync from MySQL to Redis at startup |
| Deduction phase | Redis deducts first, MQ async persists to MySQL |
| Consumption phase | MySQL deduction failure rolls back Redis |
| Expired orders | Rollback updates both Redis and MySQL |
| Scheduled reconciliation | Compare every 5 minutes, auto compensate if inconsistent |

#### Detailed Flow

```
Normal flow:
1. Redis inventory deducted ✓
2. MQ message sent ✓
3. MQ consumer deducts MySQL inventory ✓
4. Order created ✓
5. Consistent ✓

Exception - MQ consumption failed:
1. Redis inventory deducted ✓
2. MQ message sent ✓
3. MQ consumer failed to deduct MySQL inventory (insufficient)
4. Rollback Redis inventory ✓
5. Consistent ✓

Exception - Order timeout unpaid:
1. Redis inventory deducted ✓
2. MySQL inventory deducted ✓
3. Order created ✓
4. 15 minutes unpaid
5. Scheduled task rolls back Redis+MySQL ✓
6. Consistent ✓
```

---

### 8.4 Transaction Message Solution

#### Problem Description
Need to ensure distributed transaction consistency among Redis deduction, MySQL order creation, and MQ messages during seckill process.

#### Solution: RocketMQ Transaction Message

RocketMQ transaction messages ensure eventual consistency between local transactions and message sending through "Half Message" mechanism.

**Transaction Message Flow**
```
┌─────────────┐     Half Message     ┌─────────────┐
│  Seckill    │ ────────────────▶  │   RocketMQ  │
│   Request   │    (Prepared)       │  Tx Log     │
└──────┬──────┘                     └──────┬──────┘
       │                                    │
       ▼                                    │
┌─────────────┐                             │
│  Local Tx   │                             │
│ (Deduct     │                             │
│  MySQL +    │                             │
│  Create     │                             │
│  Order)     │                             │
└──────┬──────┘                             │
       │   Commit Success                    │
       ├────────────────────────────────────┤
       │                                    │
       ▼                              Send Real Message
                          ┌─────────────┐
                          │  Consumer   │
                          │  Subscribe  │
                          └─────────────┘
```

**Transaction Message State Machine**
```
┌──────────┐   Commit Success    ┌──────────┐
│  Half    │ ─────────────────▶  │  Commit  │
│  Message  │                     │ (Committed) │
└──────────┘                     └──────────┘
       │                               │
       │   Rollback/Unknown            │
       ├───────────────────────────────┤
       │                               │
       ▼                               ▼
┌──────────┐                    ┌──────────┐
│ Rollback │                    │  Commit  │
│(Rolled  )│                    │(Committed) │
└──────────┘                    └──────────┘
```

**Transaction Listener Execution Flow**
```
1. Half message sent successfully
2. Execute local transaction (MySQL deduct inventory + create order)
   - Success → Return COMMIT, RocketMQ sends real message
   - Failure → Return ROLLBACK, message discarded
   - Timeout/Exception → Return UNKNOWN, trigger check
3. Check: verify if order exists
   - Order exists → Return COMMIT
   - Order not exists → Return ROLLBACK
```

**See Section 4.7 for key code**

**Solution Comparison**

| Comparison | Local Message Table + Compensation | RocketMQ Transaction Message |
|------------|-----------------------------------|------------------------------|
| Implementation Complexity | Medium (maintain message table + scheduled tasks) | Low (native MQ support) |
| Consistency | Eventual consistency | Eventual consistency |
| Latency | Scheduled compensation has delay | Real-time |
| Reliability | Message table + compensation dual guarantee | Depends on MQ stability |
| Ops Cost | Monitor message table | RocketMQ HA |

**Recommended Scenarios**
- Small-scale seckill: Local message table is sufficient, simple and controllable
- Large-scale seckill: RocketMQ transaction message is better, less latency and ops cost

---

### 8.5 Anti-Brush Strategy

#### Problem Description
Scalpers or malicious users brush interfaces through scripts, taking resources from normal users.

#### Solution: Multi-dimensional Rate Limiting

| Dimension | Limit | Granularity |
|-----------|-------|------------|
| Global rate limit | 1000 requests/min | Per SKU |
| IP anti-brush | 10 times/min | IP+SKU |
| Device anti-brush | 10 times/min | Device ID+SKU |

```java
// IP anti-brush
String ipKey = "seckill:ip:" + skuId + ":" + ip;
Long ipCount = redis.incr(ipKey);
if (ipCount == 1) redis.expire(ipKey, 60);
if (ipCount > 10) return false;

// Device anti-brush
String deviceKey = "seckill:device:" + skuId + ":" + deviceId;
Long deviceCount = redis.incr(deviceKey);
if (deviceCount == 1) redis.expire(deviceKey, 60);
if (deviceCount > 10) return false;
```

---

### 8.6 Redis Degradation Strategy

#### Problem Description
When Redis fails, the entire seckill system becomes unavailable.

#### Solution: Fallback to MySQL

```java
try {
    result = redis.eval(luaScript, keys, args);
} catch (Exception e) {
    log.warn("Redis failed, enabling degradation strategy");
    return deductStockWithDbAndLock(skuId, userId);
}

private boolean deductStockWithDbAndLock(String skuId, String userId) {
    synchronized (skuId.intern()) {
        // 1. Check if user already purchased
        if (orderMapper.existsByUserIdAndSkuId(userId, skuId)) {
            return -1;
        }
        // 2. Deduct inventory (MySQL optimistic lock)
        int affected = productMapper.decrementStock(skuId);
        if (affected == 0) {
            return 0;
        }
        // 3. Create local message record...
        return 1;
    }
}
```

Degradation notes:
- Use synchronized block for protection (single-machine degradation)
- Performance degrades during fallback, but ensures basic availability
- Automatically switches back when Redis recovers

---

## 9. Key Technical Points Summary

| Technical Point | Description |
|-----------------|-------------|
| Lua script | Ensures atomic operation of Redis inventory deduction and user marking |
| Redis Set | O(1) complexity to check if user has purchased |
| Rate limiting & anti-brush | Fixed window rate limiting + IP/Device dimension anti-brush |
| MQ async | Peak shaving, protects MySQL |
| RocketMQ transaction message | Half message mechanism ensures local tx and message consistency |
| Idempotent design | Order ID unique index prevents duplicate creation |
| Scheduled tasks | Order timeout rollback + inventory reconciliation |
| Degradation strategy | Fallback to MySQL when Redis fails |

---

## 10. Confirmation Checklist

| No. | Item | Value |
|-----|------|-------|
| 1 | Development language | Java + MySQL + Redis + RocketMQ |
| 2 | Rate limiting | 1000 requests per minute per product |
| 3 | Anti-brush | IP/Device 10 times per minute |
| 4 | Inventory deduction | Lua script atomic operation + MySQL WHERE stock > 0 |
| 5 | Duplicate purchase | Redis Set + Lua script check |
| 6 | Message reliability | RocketMQ transaction message (Half message + check) |
| 7 | Consumer idempotency | Order ID unique index |
| 8 | Degradation strategy | Redis failure fallback to MySQL |
| 9 | Order timeout | Rollback inventory after 15 minutes without payment |
| 10 | Inventory reconciliation | Auto compensate + alert every 5 minutes |
| 11 | Multiple products | Supported |

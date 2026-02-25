# DB and Cache Consistency Solutions in High Concurrency Scenarios

> 📖 **中文版本**: [cache-consistency-solution.md](./cache-consistency-solution.md)

## 1. Problem Background

### 1.1 Classic Consistency Problem

When DB and cache are used together in distributed systems, data inconsistency issues frequently occur, especially under high concurrency.

```
Timeline (Classic Concurrency Problem):
T1: Thread A reads data → cache miss → reads DB (old value v1)
T2: Thread B updates data → writes DB (new value v2) → deletes cache
T3: Thread A writes old value v1 to cache  ← Old data overwrites new data!
```

### 1.2 Consistency Types

| Type | Description | Scenario |
|------|-------------|----------|
| Strong Consistency | Latest value readable immediately after update | Finance, transfers |
| Eventual Consistency | Brief inconsistency allowed, eventually consistent | Most business scenarios |

---

## 2. Solution Comparison

### 2.1 Solution Overview

| Solution | Consistency | Blocking | Performance | Complexity | Applicable Scenario |
|:--------:|:-----------:|:--------:|:-----------:|:----------:|---------------------|
| Cache Aside | Eventual | No | High | Low | Low concurrency simple business |
| Delayed Double Delete | Eventual | Yes | Medium | Low | Low concurrency simple business |
| Distributed Lock | Strong | Yes | Low | Medium | High data accuracy requirements |
| Binlog Sync | Eventual | No | High | High | Large high concurrency systems |
| Message Queue | Eventual | No | High | Medium | Medium-high concurrency systems |

### 2.2 Why Delayed Double Delete and Distributed Lock Are Not Suitable for High Concurrency?

**Delayed Double Delete Issues**:
- Thread.sleep() blocks the thread, wasting CPU resources
- Sleep duration is difficult to determine: too short won't solve the problem, too long affects performance
- Thread accumulation under high concurrency, system throughput drops significantly

**Distributed Lock Issues**:
- Acquiring lock requires waiting, threads block
- Lock competition intensifies, performance drops sharply
- May cause deadlock, livelock issues
- Suitable for low concurrency strong consistency scenarios, not for high concurrency

---

## 3. Detailed Solutions

### 3.1 Cache Aside

#### 3.1.1 Principle

```
Read: Cache hit → return
      Cache miss → query DB → write to cache → return

Write: Update DB → delete cache (not update!)
```

#### 3.1.2 Code Implementation

```java
/**
 * UserService - Cache Aside Pattern
 * @author lvdaxianer
 */
@Service
public class UserServiceCacheAside {

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final String CACHE_KEY_PREFIX = "user:";
    private static final long CACHE_EXPIRE_SECONDS = 3600;

    /**
     * Query user
     * 1. Check cache first
     * 2. Query DB on cache miss
     * 3. Write to cache
     *
     * @param id user ID
     * @return user info
     * @author lvdaxianer
     */
    public User getUserById(Long id) {
        String cacheKey = CACHE_KEY_PREFIX + id;

        // 1. Query cache
        String cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            return JSON.parseObject(cached, User.class);
        }

        // 2. Cache miss, query DB
        User user = userMapper.selectById(id);

        // 3. Write to cache (note: write, not update)
        if (user != null) {
            redisTemplate.opsForValue().set(
                    cacheKey,
                    JSON.toJSONString(user),
                    CACHE_EXPIRE_SECONDS,
                    TimeUnit.SECONDS
            );
        }

        return user;
    }

    /**
     * Update user
     * 1. Update DB
     * 2. Delete cache (not update!)
     *
     * @param user user info
     * @author lvdaxianer
     */
    @Transactional(rollbackFor = Exception.class)
    public void updateUser(User user) {
        // 1. Update DB
        userMapper.updateById(user);

        // 2. Delete cache
        String cacheKey = CACHE_KEY_PREFIX + user.getId();
        redisTemplate.delete(cacheKey);
    }
}
```

#### 3.1.3 Pros and Cons

| Pros | Cons |
|------|------|
| Simple implementation | Old value may overwrite new value under high concurrency |
| High performance | Needs other solutions for consistency |
| No blocking | |

#### 3.1.4 Applicable Scenarios

- Read-heavy, write-light scenarios
- Business tolerant to brief inconsistency
- Low concurrency simple business

---

### 3.2 Delayed Double Delete

#### 3.2.1 Principle

```
1. Delete cache
2. Update DB
3. Delayed delete cache (solves old value overwrite issue in concurrency)
```

#### 3.2.2 Code Implementation

```java
/**
 * UserService - Delayed Double Delete Pattern
 * @author lvdaxianer
 */
@Service
public class UserServiceDelayDelete {

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final String CACHE_KEY_PREFIX = "user:";

    /**
     * Update user - Delayed double delete
     * 1. Delete cache first
     * 2. Update DB
     * 3. Delayed delete cache
     *
     * Delay duration: slightly longer than average request response time
     * Usually 100ms~500ms, depends on business scenario
     *
     * @param user user info
     * @author lvdaxianer
     */
    @Transactional(rollbackFor = Exception.class)
    public void updateUser(User user) {
        String cacheKey = CACHE_KEY_PREFIX + user.getId();

        // 1. Delete cache
        redisTemplate.delete(cacheKey);

        // 2. Update DB
        userMapper.updateById(user);

        // 3. Delayed double delete (use thread pool, don't block main thread)
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(100);  // 100ms delay
                redisTemplate.delete(cacheKey);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
}
```

#### 3.2.3 Pros and Cons

| Pros | Cons |
|------|------|
| Solves old value overwrite issue | Delay duration difficult to precisely determine |
| Relatively simple implementation | Still blocks business threads (though async) |
| | Poor performance under high concurrency |

#### 3.2.4 Applicable Scenarios

- Low concurrency simple business
- Scenarios with low consistency requirements
- **Not recommended for high concurrency scenarios**

---

### 3.3 Distributed Lock

#### 3.3.1 Principle

```
1. Acquire distributed lock
2. Read latest data from DB
3. Write to cache
4. Release distributed lock
```

#### 3.3.2 Code Implementation

```java
/**
 * UserService - Distributed Lock Pattern
 * @author lvdaxianer
 */
@Service
public class UserServiceDistLock {

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private RedissonClient redissonClient;

    private static final String CACHE_KEY_PREFIX = "user:";
    private static final String LOCK_KEY_PREFIX = "lock:user:";

    /**
     * Update user - Distributed lock
     * 1. Acquire distributed lock
     * 2. Update DB
     * 3. Delete cache
     * 4. Release lock
     *
     * Applicable scenario: High data accuracy requirements
     *
     * @param user user info
     * @author lvdaxianer
     */
    @Transactional(rollbackFor = Exception.class)
    public void updateUser(User user) {
        String lockKey = LOCK_KEY_PREFIX + user.getId();
        RLock lock = redissonClient.getLock(lockKey);

        try {
            // 1. Acquire lock (wait 5s, hold 10s)
            boolean acquired = lock.tryLock(5, 10, TimeUnit.SECONDS);
            if (!acquired) {
                throw new RuntimeException("Failed to acquire lock, please try again later");
            }

            // 2. Update DB
            userMapper.updateById(user);

            // 3. Delete cache
            String cacheKey = CACHE_KEY_PREFIX + user.getId();
            redisTemplate.delete(cacheKey);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("System busy", e);
        } finally {
            // 4. Release lock
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
```

#### 3.3.3 Pros and Cons

| Pros | Cons |
|------|------|
| Strong consistency | Uncontrollable wait time for lock acquisition |
| High data accuracy | Performance drops sharply under high concurrency |
| | May cause deadlock, livelock |
| | Complex implementation, needs various exception handling |

#### 3.3.4 Applicable Scenarios

- Finance, transfers and other strong consistency scenarios
- Business with extremely high data accuracy requirements
- **Not suitable for high concurrency ordinary business**

---

### 3.4 Binlog Sync

#### 3.4.1 Principle

```
MySQL → Binlog → Canal → Message Queue → Consumer → Redis
```

Completely async decoupling, business threads only write to DB, cache consistency guaranteed by independent program.

#### 3.4.2 Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Binlog Sync Architecture                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   Business Thread                    Background Sync Thread            │
│   ┌────────┐                         ┌────────────┐                    │
│   │ WriteDB │                         │  Canal     │                    │
│   └───┬────┘                         │  Monitor   │                    │
│       │                              │  Binlog    │                    │
│       │                              └──────┬─────┘                    │
│       │                                     │                          │
│       ▼                                     ▼                          │
│   ┌────────┐                         ┌────────────┐                   │
│   │  MySQL │ ──Binlog──▶             │  RocketMQ  │                    │
│   └────────┘                         └──────┬─────┘                    │
│                                             │                          │
│                                             ▼                          │
│                                      ┌────────────┐                    │
│                                      │  Consumer  │                    │
│                                      │ Update Redis│                    │
│                                      └────────────┘                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 3.4.3 Code Implementation

```java
/**
 * Canal Binlog Consumer
 * Monitor MySQL changes, async update Redis cache
 * @author lvdaxianer
 */
@Component
@Slf4j
public class CanalBinlogConsumer {

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final String CACHE_KEY_PREFIX = "user:";

    /**
     * Process Canal message
     * 1. Parse Binlog
     * 2. Determine operation type
     * 3. Update/delete cache
     *
     * @param entry Binlog entry
     * @author lvdaxianer
     */
    public void process(CanalEntry.Entry entry) {
        // 1. Parse table name and operation type
        String tableName = entry.getHeader().getTableName();
        CanalEntry.EventType eventType = entry.getHeader().getEventType();

        // Only process user table
        if (!"user".equals(tableName)) {
            return;
        }

        // 2. Parse row data
        CanalEntry.RowChange rowChange;
        try {
            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse Binlog", e);
            return;
        }

        // 3. Process each row
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            switch (eventType) {
                case INSERT:
                case UPDATE:
                    // Get updated data
                    List<CanalEntry.Column> afterColumns = rowData.getAfterColumnsList();
                    Long userId = getColumnValue(afterColumns, "id");
                    String userData = JSON.toJSONString(parseUserData(afterColumns));

                    // Update cache
                    redisTemplate.opsForValue().set(
                            CACHE_KEY_PREFIX + userId,
                            userData,
                            3600,
                            TimeUnit.SECONDS
                    );
                    break;

                case DELETE:
                    // Get data before deletion
                    List<CanalEntry.Column> beforeColumns = rowData.getBeforeColumnsList();
                    Long deleteUserId = getColumnValue(beforeColumns, "id");

                    // Delete cache
                    redisTemplate.delete(CACHE_KEY_PREFIX + deleteUserId);
                    break;

                default:
                    break;
            }
        }
    }

    /**

     *
     * @param columns * Get column value column list
     * @param columnName column name
     * @return column value
     * @author lvdaxianer
     */
    private Long getColumnValue(List<CanalEntry.Column> columns, String columnName) {
        return columns.stream()
                .filter(c -> c.getName().equals(columnName))
                .map(CanalEntry.Column::getGetValue)
                .map(Long::parseLong)
                .findFirst()
                .orElse(null);
    }

    /**
     * Parse user data
     *
     * @param columns column list
     * @return user data Map
     * @author lvdaxianer
     */
    private Map<String, Object> parseUserData(List<CanalEntry.Column> columns) {
        Map<String, Object> data = new HashMap<>();
        for (CanalEntry.Column column : columns) {
            data.put(column.getName(), column.getGetValue());
        }
        return data;
    }
}
```

#### 3.4.4 Pros and Cons

| Pros | Cons |
|------|------|
| Completely async, no blocking | Complex implementation, requires Canal setup |
| No business code intrusion | Need to maintain additional sync service |
| High reliability | Need to maintain message queue |
| Suitable for large-scale high concurrency | |

#### 3.4.5 Applicable Scenarios

- Large high concurrency systems
- Multi-service shared cache scenarios
- Business with high consistency requirements
- **Recommended: Canal + RocketMQ (Alibaba solution)**

---

### 3.5 Message Queue (Recommended for High Concurrency)

#### 3.5.1 Principle

```
1. Business thread writes to DB
2. Send transaction message
3. Consumer async updates cache
```

#### 3.5.2 Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      Message Queue Sync Architecture                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   Business Thread                  Background Consumer Thread           │
│   ┌────────┐                       ┌────────────┐                      │
│   │ WriteDB│                       │  Consumer  │                      │
│   └───┬────┘                       │ UpdateCache│                      │
│       │                            └──────┬─────┘                       │
│       │                                   │                             │
│       ▼                                   │                             │
│   ┌────────┐                       ┌──────▼─────┐                       │
│   │Transaction│───Async──▶ RocketMQ ──▶│  Consumer │───▶ Redis        │
│   │ Message │                       └────────────┘                       │
│   └────────┘                                                         │
│                                                                         │
│   Features:                                                           │
│   - Transaction message ensures no message loss                       │
│   - Consumer ensures idempotency                                      │
│   - Failed messages auto retry                                        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 3.5.3 Code Implementation

**Message Entity**:

```java
/**
 * Cache Sync Message
 * Used for async sync to Redis after DB changes
 * @author lvdaxianer
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CacheSyncMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Entity type (e.g., user, order, product)
     */
    private String entityType;

    /**
     * Entity ID
     */
    private Long entityId;

    /**
     * Operation type
     */
    private OperationType operation;

    /**
     * Message ID (for idempotency)
     */
    private String messageId;

    /**
     * Creation time
     */
    private LocalDateTime createTime;

    /**
     * Operation type enum
     */
    @AllArgsConstructor
    public enum OperationType {
        CREATE("Create"),
        UPDATE("Update"),
        DELETE("Delete");

        private final String description;
    }
}
```

**Producer (Service Layer)**:

```java
/**
 * UserService - Message Queue Sync Pattern
 * @author lvdaxianer
 */
@Service
@Slf4j
public class UserServiceMQ {

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    private static final String CACHE_TOPIC = "cache-sync-topic";

    /**
     * Update user - Transaction message
     * 1. Write to DB
     * 2. Send transaction message (ensure DB and message consistency)
     *
     * Advantages:
     * - No blocking of business threads
     * - Reliable message delivery
     * - Async cache update
     *
     * @param user user info
     * @author lvdaxianer
     */
    @Transactional(rollbackFor = Exception.class)
    public void updateUser(User user) {
        // 1. Write to DB
        userMapper.updateById(user);

        // 2. Send transaction message
        CacheSyncMessage message = CacheSyncMessage.builder()
                .entityType("user")
                .entityId(user.getId())
                .operation(CacheSyncMessage.OperationType.UPDATE)
                .messageId(UUID.randomUUID().toString())
                .createTime(LocalDateTime.now())
                .build();

        // Send transaction message
        Message<?> rocketMsg = MessageBuilder.withPayload(JSON.toJSONString(message))
                .build();

        rocketMQTemplate.sendMessageInTransaction(CACHE_TOPIC, rocketMsg, message);
    }

    /**
     * Delete user
     *
     * @param userId user ID
     * @author lvdaxianer
     */
    @Transactional(rollbackFor = Exception.class)
    public void deleteUser(Long userId) {
        // 1. Delete from DB
        userMapper.deleteById(userId);

        // 2. Send message
        CacheSyncMessage message = CacheSyncMessage.builder()
                .entityType("user")
                .entityId(userId)
                .operation(CacheSyncMessage.OperationType.DELETE)
                .messageId(UUID.randomUUID().toString())
                .createTime(LocalDateTime.now())
                .build();

        Message<?> rocketMsg = MessageBuilder.withPayload(JSON.toJSONString(message))
                .build();

        rocketMQTemplate.send(CACHE_TOPIC, rocketMsg);
    }
}
```

**Consumer**:

```java
/**
 * Cache Sync Message Consumer
 * Consume messages, async update Redis cache
 * @author lvdaxianer
 */
@Component
@Slf4j
@RocketMQMessageListener(
        topic = "cache-sync-topic",
        consumerGroup = "cache-sync-consumer-group",
        consumeThreadMax = 10
)
public class CacheSyncConsumer implements MessageListenerConcurrently {

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private StringRedisTemplate duplicateCheckRedis;

    private static final String CACHE_KEY_PREFIX = "user:";
    private static final String DUPLICATE_KEY_PREFIX = "msg:dedup:";
    private static final long CACHE_EXPIRE_SECONDS = 3600;

    /**
     * Consume messages, update cache
     * 1. Idempotency check
     * 2. Parse message
     * 3. Update/delete cache
     *
     * @param msgs message list
     * @param context consume context
     * @return consume status
     * @author lvdaxianer
     */
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                     ConsumeConcurrentlyContext context) {
        for (MessageExt msg : msgs) {
            try {
                // 1. Idempotency check
                if (isDuplicate(msg)) {
                    log.info("Message already processed, skip: {}", msg.getMsgId());
                    continue;
                }

                // 2. Parse message
                String body = new String(msg.getBody());
                CacheSyncMessage cacheMsg = JSON.parseObject(body, CacheSyncMessage.class);

                // 3. Handle message
                handleMessage(cacheMsg);

                // 4. Record idempotency marker
                recordDuplicate(msg.getMsgId());

            } catch (Exception e) {
                log.error("Failed to process message, msgId: {}", msg.getMsgId(), e);
                // Consumption failed, RocketMQ will auto retry
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    /**
     * Handle cache sync message
     *
     * @param message cache sync message
     * @author lvdaxianer
     */
    private void handleMessage(CacheSyncMessage message) {
        String cacheKey = CACHE_KEY_PREFIX + message.getEntityId();

        switch (message.getOperation()) {
            case CREATE:
            case UPDATE:
                // Query latest data and write to cache
                if ("user".equals(message.getEntityType())) {
                    User user = userMapper.selectById(message.getEntityId());
                    if (user != null) {
                        redisTemplate.opsForValue().set(
                                cacheKey,
                                JSON.toJSONString(user),
                                CACHE_EXPIRE_SECONDS,
                                TimeUnit.SECONDS
                        );
                    }
                }
                break;

            case DELETE:
                // Delete cache
                redisTemplate.delete(cacheKey);
                break;

            default:
                log.warn("Unknown operation type: {}", message.getOperation());
        }
    }

    /**
     * Idempotency check
     * Use Redis to record processed messageId
     *
     * @param msg message
     * @return already processed
     * @author lvdaxianer
     */
    private boolean isDuplicate(MessageExt msg) {
        String dedupKey = DUPLICATE_KEY_PREFIX + msg.getMsgId();
        return Boolean.TRUE.equals(duplicateCheckRedis.hasKey(dedupKey));
    }

    /**
     * Record idempotency marker
     *
     * @param messageId message ID
     * @author lvdaxianer
     */
    private void recordDuplicate(String messageId) {
        String dedupKey = DUPLICATE_KEY_PREFIX + messageId;
        duplicateCheckRedis.opsForValue().set(dedupKey, "1", 10, TimeUnit.MINUTES);
    }
}
```

#### 3.5.4 Reliability Guarantee

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Reliability Guarantee                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. Transaction Message                                                │
│     ┌────────────┐    ┌────────────┐    ┌────────────┐               │
│     │ Pre-send  │ -> │ Execute DB │ -> │ Commit    │               │
│     │ Message   │    │ Transaction│    │ Message   │               │
│     │(HalfMsg)  │    │            │    │ (Visible) │               │
│     └────────────┘    └────────────┘    └────────────┘               │
│          ↓                                                            │
│     If DB fails -> message discarded, not delivered                   │
│                                                                         │
│  2. Message Persistence                                                │
│     - Messages flushed to disk                                        │
│     - Broker multi-replica storage                                    │
│                                                                         │
│  3. Consumer ACK                                                      │
│     - Success -> CONSUME_SUCCESS                                      │
│     - Fail -> RECONSUME_LATER (max 16 retries)                      │
│                                                                         │
│  4. Dead Letter Queue                                                 │
│     After 16 retries still fails -> enters DLQ                        │
│     Manual intervention required                                      │
│                                                                         │
│  5. Idempotency                                                       │
│     - messageId deduplication                                         │
│     - No duplicate processing within 10 minutes                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 3.5.5 Pros and Cons

| Pros | Cons |
|------|------|
| Completely async, no blocking of business threads | Need to maintain message queue |
| Transaction message guarantees no message loss | Need to handle idempotency |
| Failed messages auto retry | Relatively complex implementation |
| High performance | |
| Suitable for medium-high concurrency | |

#### 3.5.6 Applicable Scenarios

- Medium-high concurrency systems
- Scenarios with high performance requirements
- Business allowing brief inconsistency
- **Recommended for most high concurrency business**

---

## 4. Solution Selection Guide

### 4.1 Decision Tree

```
                        ┌─────────────────┐
                        │ What's the      │
                        │ business scene? │
                        └────────┬────────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          │                      │                      │
          ▼                      ▼                      ▼
   ┌─────────────┐        ┌─────────────┐        ┌─────────────┐
   │  Finance/   │        │  Ordinary  │        │  Simple    │
   │  Transfer   │        │  Business   │        │  Query     │
   │  Strong     │        │  Eventual   │        │  Read-heavy│
   │  consistency│        │  consistency│        │  Write-light│
   └──────┬──────┘        └──────┬──────┘        └──────┬──────┘
          │                      │                      │
          ▼                      ▼                      ▼
   ┌─────────────┐        ┌─────────────┐        ┌─────────────┐
   │Distributed  │        │  Message    │        │Cache Aside │
   │Lock or      │        │  Queue      │        │  (Simple)  │
   │Binlog       │        │(Recommended) │        │             │
   └─────────────┘        └─────────────┘        └─────────────┘
```

### 4.2 Scenario Recommendations

| Scenario | Recommended Solution | Reason |
|----------|---------------------|--------|
| Low concurrency simple business | Cache Aside | Simple implementation, high performance |
| Extremely high data accuracy | Distributed Lock | Strong consistency |
| Large high concurrency system | Binlog Sync | No intrusion, high reliability |
| **Medium-high concurrency (Recommended)** | **Message Queue** | **Balance of performance and consistency** |

---

## 5. Summary

### 5.1 Core Points

1. **Delayed double delete and distributed lock are not good choices for high concurrency**
   - Blocks business threads
   - Affects system throughput

2. **Message Queue solution is most suitable for high concurrency scenarios**
   - Completely async, no blocking
   - Transaction messages guarantee reliability
   - Relatively simple implementation

3. **Binlog Sync is suitable for large systems**
   - Completely decoupled
   - But higher complexity

### 5.2 Alibaba Recommendation

Java Development Manual recommends: Canal + RocketMQ Solution

```
MySQL -> Binlog -> Canal -> RocketMQ -> Consumer -> Redis
```

This is a production-proven solution with high reliability, suitable for high concurrency scenarios.

---

## 6. References

- Alibaba Java Development Manual
- RocketMQ Official Documentation
- Canal Official Documentation
- Redis Cache Design Best Practices

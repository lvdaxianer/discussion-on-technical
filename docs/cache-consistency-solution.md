# 高并发场景下DB与缓存数据一致性方案

> 📖 **English Version**: [cache-consistency-solution-en.md](./cache-consistency-solution-en.md)

## 一、问题背景

### 1.1 经典一致性问题

在分布式系统中，DB和缓存配合使用时，经常会遇到数据不一致的问题。尤其在高并发场景下，这种问题更为突出。

```
时间线（经典并发问题）：
T1: 线程A读取数据 → 缓存未命中 → 读取DB（旧值v1）
T2: 线程B更新数据 → 写DB（新值v2）→ 删除缓存
T3: 线程A将旧值v1写入缓存  ← 旧数据覆盖新数据！
```

### 1.2 一致性分类

| 类型 | 说明 | 场景 |
|------|------|------|
| 强一致 | 更新后立即可读到最新值 | 金融、转账 |
| 最终一致 | 更新后允许短暂不一致，最终达到一致 | 大多数业务场景 |

---

## 二、方案对比

### 2.1 方案总览

| 方案 | 一致性 | 阻塞 | 性能 | 复杂度 | 适用场景 |
|:----:|:------:|:----:|:----:|:------:|----------|
| Cache Aside | 最终一致 | 否 | 高 | 低 | 低并发简单业务 |
| 延迟双删 | 最终一致 | 是 | 中 | 低 | 低并发简单业务 |
| 分布式锁 | 强一致 | 是 | 低 | 中 | 数据准确性要求极高 |
| Binlog同步 | 最终一致 | 否 | 高 | 高 | 大型高并发系统 |
| 消息队列 | 最终一致 | 否 | 高 | 中 | 中高并发系统 |

### 2.2 为什么高并发场景不适合延迟双删和分布式锁？

**延迟双删问题**：
- Thread.sleep() 会阻塞线程，浪费CPU资源
- sleep时间难以确定：太短无法解决问题，太长影响性能
- 高并发下线程堆积，系统吞吐量大降

**分布式锁问题**：
- 获取锁需要等待，线程阻塞
- 锁竞争激烈，性能急剧下降
- 可能引发死锁、活锁问题
- 适合低并发强一致场景，不适合高并发

---

## 三、详细方案

### 3.1 Cache Aside（旁路缓存）

#### 3.1.1 原理

```
读：缓存命中 → 返回
   缓存未命中 → 查DB → 写入缓存 → 返回

写：更新DB → 删除缓存（不是更新！）
```

#### 3.1.2 代码实现

```java
/**
 * 用户Service - Cache Aside模式
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
     * 查询用户
     * 1. 先查缓存
     * 2. 缓存未命中查DB
     * 3. 写入缓存
     *
     * @param id 用户ID
     * @return 用户信息
     * @author lvdaxianer
     */
    public User getUserById(Long id) {
        String cacheKey = CACHE_KEY_PREFIX + id;

        // 1. 查询缓存
        String cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            return JSON.parseObject(cached, User.class);
        }

        // 2. 缓存未命中，查询DB
        User user = userMapper.selectById(id);

        // 3. 写入缓存（注意：不是更新，是写入）
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
     * 更新用户
     * 1. 更新DB
     * 2. 删除缓存（不是更新！）
     *
     * @param user 用户信息
     * @author lvdaxianer
     */
    @Transactional(rollbackFor = Exception.class)
    public void updateUser(User user) {
        // 1. 更新DB
        userMapper.updateById(user);

        // 2. 删除缓存
        String cacheKey = CACHE_KEY_PREFIX + user.getId();
        redisTemplate.delete(cacheKey);
    }
}
```

#### 3.1.3 优缺点

| 优点 | 缺点 |
|------|------|
| 实现简单 | 高并发下可能出现旧值覆盖新值 |
| 性能高 | 需要配合其他方案解决一致性问题 |
| 无阻塞 | |

#### 3.1.4 适用场景

- 读多写少的场景
- 对短暂不一致不敏感的业务
- 低并发简单业务

---

### 3.2 延迟双删

#### 3.2.1 原理

```
1. 删除缓存
2. 更新DB
3. 延迟删除缓存（解决并发时的旧值覆盖问题）
```

#### 3.2.2 代码实现

```java
/**
 * 用户Service - 延迟双删模式
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
     * 更新用户 - 延迟双删
     * 1. 先删除缓存
     * 2. 更新DB
     * 3. 延迟删除缓存
     *
     * 延迟时间设置：略大于请求的平均响应时间
     * 通常100ms~500ms，取决于业务场景
     *
     * @param user 用户信息
     * @author lvdaxianer
     */
    @Transactional(rollbackFor = Exception.class)
    public void updateUser(User user) {
        String cacheKey = CACHE_KEY_PREFIX + user.getId();

        // 1. 删除缓存
        redisTemplate.delete(cacheKey);

        // 2. 更新DB
        userMapper.updateById(user);

        // 3. 延迟双删（使用线程池，不阻塞主线程）
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(100);  // 延迟100ms
                redisTemplate.delete(cacheKey);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
}
```

#### 3.2.3 优缺点

| 优点 | 缺点 |
|------|------|
| 解决旧值覆盖问题 | 延迟时间难以精确确定 |
| 实现相对简单 | 仍然会阻塞业务线程（虽然用了异步） |
| | 高并发下性能不佳 |

#### 3.2.4 适用场景

- 低并发简单业务
- 对数据一致性要求不高的场景
- 不建议在高并发场景使用

---

### 3.3 分布式锁

#### 3.3.1 原理

```
1. 获取分布式锁
2. 读取DB最新数据
3. 写入缓存
4. 释放分布式锁
```

#### 3.3.2 代码实现

```java
/**
 * 用户Service - 分布式锁模式
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
     * 更新用户 - 分布式锁
     * 1. 获取分布式锁
     * 2. 更新DB
     * 3. 删除缓存
     * 4. 释放锁
     *
     * 适用场景：数据准确性要求极高的场景
     *
     * @param user 用户信息
     * @author lvdaxianer
     */
    @Transactional(rollbackFor = Exception.class)
    public void updateUser(User user) {
        String lockKey = LOCK_KEY_PREFIX + user.getId();
        RLock lock = redissonClient.getLock(lockKey);

        try {
            // 1. 获取锁（等待5秒，锁持有10秒）
            boolean acquired = lock.tryLock(5, 10, TimeUnit.SECONDS);
            if (!acquired) {
                throw new RuntimeException("获取锁失败，请稍后重试");
            }

            // 2. 更新DB
            userMapper.updateById(user);

            // 3. 删除缓存
            String cacheKey = CACHE_KEY_PREFIX + user.getId();
            redisTemplate.delete(cacheKey);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("系统繁忙", e);
        } finally {
            // 4. 释放锁
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
```

#### 3.3.3 优缺点

| 优点 | 缺点 |
|------|------|
| 强一致性 | 获取锁等待时间不可控 |
| 数据准确性高 | 高并发下性能急剧下降 |
| | 可能引发死锁、活锁 |
| | 实现复杂，需要考虑各种异常 |

#### 3.3.4 适用场景

- 金融、转账等强一致性要求场景
- 数据准确性要求极高的业务
- **不适合高并发普通业务场景**

---

### 3.4 Binlog同步

#### 3.4.1 原理

```
MySQL → Binlog → Canal → 消息队列 → 消费者 → Redis
```

完全异步解耦，业务线程只管写DB，缓存由独立程序保证一致。

#### 3.4.2 架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Binlog同步架构                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   业务线程                              后台同步线程                      │
│   ┌────────┐                          ┌────────────┐                   │
│   │ 写DB   │                          │  Canal      │                   │
│   └───┬────┘                          │  监听Binlog │                   │
│       │                               └──────┬─────┘                      │
│       │                                      │                            │
│       ▼                                      ▼                            │
│   ┌────────┐                          ┌────────────┐                   │
│   │ MySQL  │ ──Binlog──▶              │  RocketMQ  │                   │
│   └────────┘                          └──────┬─────┘                   │
│                                                 │                            │
│                                                 ▼                            │
│                                          ┌────────────┐                   │
│                                          │  消费者    │                   │
│                                          │  更新Redis │                   │
│                                          └────────────┘                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 3.4.3 代码实现

**Canal消费者（简化版）**：

```java
/**
 * Canal Binlog消费者
 * 监听MySQL变更，异步更新Redis缓存
 * @author lvdaxianer
 */
@Component
@Slf4j
public class CanalBinlogConsumer {

    @Autowired
    private StringRedisTemplate redisTemplate;

    private static final String CACHE_KEY_PREFIX = "user:";

    /**
     * 处理Canal消息
     * 1. 解析Binlog
     * 2. 判断操作类型
     * 3. 更新/删除缓存
     *
     * @param entry Binlog条目
     * @author lvdaxianer
     */
    public void process(CanalEntry.Entry entry) {
        // 1. 解析表名和操作类型
        String tableName = entry.getHeader().getTableName();
        CanalEntry.EventType eventType = entry.getHeader().getEventType();

        // 只处理user表
        if (!"user".equals(tableName)) {
            return;
        }

        // 2. 解析行数据
        CanalEntry.RowChange rowChange;
        try {
            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (InvalidProtocolBufferException e) {
            log.error("解析Binlog失败", e);
            return;
        }

        // 3. 处理每一行数据
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            switch (eventType) {
                case INSERT:
                case UPDATE:
                    // 获取更新后的数据
                    List<CanalEntry.Column> afterColumns = rowData.getAfterColumnsList();
                    Long userId = getColumnValue(afterColumns, "id");
                    String userData = JSON.toJSONString(parseUserData(afterColumns));

                    // 更新缓存
                    redisTemplate.opsForValue().set(
                            CACHE_KEY_PREFIX + userId,
                            userData,
                            3600,
                            TimeUnit.SECONDS
                    );
                    break;

                case DELETE:
                    // 获取删除前的数据
                    List<CanalEntry.Column> beforeColumns = rowData.getBeforeColumnsList();
                    Long deleteUserId = getColumnValue(beforeColumns, "id");

                    // 删除缓存
                    redisTemplate.delete(CACHE_KEY_PREFIX + deleteUserId);
                    break;

                default:
                    break;
            }
        }
    }

    /**
     * 获取列值
     *
     * @param columns 列列表
     * @param columnName 列名
     * @return 列值
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
     * 解析用户数据
     *
     * @param columns 列列表
     * @return 用户数据Map
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

#### 3.4.4 优缺点

| 优点 | 缺点 |
|------|------|
| 完全异步，无阻塞 | 实现复杂，需要搭建Canal |
| 业务代码无侵入 | 需要维护额外的同步服务 |
| 可靠性高 | 需要维护消息队列 |
| 适合大规模高并发 | |

#### 3.4.5 适用场景

- 大型高并发系统
- 多服务共享缓存场景
- 对数据一致性要求较高的业务
- **推荐阿里巴巴方案：Canal + RocketMQ**

---

### 3.5 消息队列（推荐高并发方案）

#### 3.5.1 原理

```
1. 业务线程写DB
2. 发送事务消息
3. 消费者异步更新缓存
```

#### 3.5.2 架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         消息队列同步架构                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   业务线程                              后台消费线程                      │
│   ┌────────┐                          ┌────────────┐                   │
│   │ 写DB   │                          │  消费者     │                   │
│   └───┬────┘                          │  更新缓存   │                   │
│       │                               └──────┬─────┘                      │
│       │                                      │                            │
│       ▼                                      │                            │
│   ┌────────┐                          ┌──────▼─────┐                     │
│   │ 事务消息│ ──异步──▶  RocketMQ  ──▶│  消费者    │ ──▶ Redis         │
│   └────────┘                          └────────────┘                     │
│                                                                         │
│   特点：                                                               │
│   - 事务消息保证DB与消息不丢                                           │
│   - 消费者保证消息幂等                                                 │
│   - 消息失败自动重试                                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 3.5.3 代码实现

**消息实体**：

```java
/**
 * 缓存同步消息
 * 用于DB变更后异步同步到Redis
 * @author lvdaxianer
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CacheSyncMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 实体类型（如：user, order, product）
     */
    private String entityType;

    /**
     * 实体ID
     */
    private Long entityId;

    /**
     * 操作类型
     */
    private OperationType operation;

    /**
     * 消息ID（用于幂等）
     */
    private String messageId;

    /**
     * 创建时间
     */
    private LocalDateTime createTime;

    /**
     * 操作类型枚举
     */
    @AllArgsConstructor
    public enum OperationType {
        CREATE("创建"),
        UPDATE("更新"),
        DELETE("删除");

        private final String description;
    }
}
```

**生产者（Service层）**：

```java
/**
 * 用户Service - 消息队列同步模式
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
     * 更新用户 - 事务消息
     * 1. 写DB
     * 2. 发送事务消息（保证DB与消息一致性）
     *
     * 优势：
     * - 不阻塞业务线程
     * - 消息可靠投递
     * - 异步更新缓存
     *
     * @param user 用户信息
     * @author lvdaxianer
     */
    @Transactional(rollbackFor = Exception.class)
    public void updateUser(User user) {
        // 1. 写DB
        userMapper.updateById(user);

        // 2. 发送事务消息
        CacheSyncMessage message = CacheSyncMessage.builder()
                .entityType("user")
                .entityId(user.getId())
                .operation(CacheSyncMessage.OperationType.UPDATE)
                .messageId(UUID.randomUUID().toString())
                .createTime(LocalDateTime.now())
                .build();

        // 发送事务消息
        Message<?> rocketMsg = MessageBuilder.withPayload(JSON.toJSONString(message))
                .build();

        rocketMQTemplate.sendMessageInTransaction(CACHE_TOPIC, rocketMsg, message);
    }

    /**
     * 删除用户
     *
     * @param userId 用户ID
     * @author lvdaxianer
     */
    @Transactional(rollbackFor = Exception.class)
    public void deleteUser(Long userId) {
        // 1. 删DB
        userMapper.deleteById(userId);

        // 2. 发送消息
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

**消费者**：

```java
/**
 * 缓存同步消息消费者
 * 消费消息，异步更新Redis缓存
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
     * 消费消息，更新缓存
     * 1. 幂等检查
     * 2. 解析消息
     * 3. 更新/删除缓存
     *
     * @param msgs 消息列表
     * @param context 消费上下文
     * @return 消费状态
     * @author lvdaxianer
     */
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                     ConsumeConcurrentlyContext context) {
        for (MessageExt msg : msgs) {
            try {
                // 1. 幂等检查
                if (isDuplicate(msg)) {
                    log.info("消息已处理，跳过: {}", msg.getMsgId());
                    continue;
                }

                // 2. 解析消息
                String body = new String(msg.getBody());
                CacheSyncMessage cacheMsg = JSON.parseObject(body, CacheSyncMessage.class);

                // 3. 处理消息
                handleMessage(cacheMsg);

                // 4. 记录幂等标记
                recordDuplicate(msg.getMsgId());

            } catch (Exception e) {
                log.error("处理消息失败, msgId: {}", msg.getMsgId(), e);
                // 消费失败，RocketMQ会自动重试
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    /**
     * 处理缓存同步消息
     *
     * @param message 缓存同步消息
     * @author lvdaxianer
     */
    private void handleMessage(CacheSyncMessage message) {
        String cacheKey = CACHE_KEY_PREFIX + message.getEntityId();

        switch (message.getOperation()) {
            case CREATE:
            case UPDATE:
                // 查询最新数据写入缓存
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
                // 删除缓存
                redisTemplate.delete(cacheKey);
                break;

            default:
                log.warn("未知的操作类型: {}", message.getOperation());
        }
    }

    /**
     * 幂等检查
     * 使用Redis记录已处理的messageId
     *
     * @param msg 消息
     * @return 是否已处理
     * @author lvdaxianer
     */
    private boolean isDuplicate(MessageExt msg) {
        String dedupKey = DUPLICATE_KEY_PREFIX + msg.getMsgId();
        return Boolean.TRUE.equals(duplicateCheckRedis.hasKey(dedupKey));
    }

    /**
     * 记录幂等标记
     *
     * @param messageId 消息ID
     * @author lvdaxianer
     */
    private void recordDuplicate(String messageId) {
        String dedupKey = DUPLICATE_KEY_PREFIX + messageId;
        duplicateCheckRedis.opsForValue().set(dedupKey, "1", 10, TimeUnit.MINUTES);
    }
}
```

#### 3.5.4 可靠性保障

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          可靠性保障机制                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. 事务消息                                                           │
│     ┌────────────┐    ┌────────────┐    ┌────────────┐               │
│     │  预发送消息  │ -> │  执行DB事务 │ -> │  提交消息   │               │
│     │  (HalfMsg) │    │            │    │  (可见)    │               │
│     └────────────┘    └────────────┘    └────────────┘               │
│          ↓                                                            │
│     如果DB失败 -> 消息被丢弃，不投递                                    │
│                                                                         │
│  2. 消息持久化                                                         │
│     - 消息刷盘到磁盘                                                   │
│     - Broker多副本存储                                                 │
│                                                                         │
│  3. 消费者ACK                                                         │
│     - 成功 -> CONSUME_SUCCESS                                          │
│     - 失败 -> RECONSUME_LATER (最多16次重试)                          │
│                                                                         │
│  4. 死信队列                                                           │
│     重试16次仍失败 -> 进入死信队列                                     │
│     需要人工介入处理                                                   │
│                                                                         │
│  5. 幂等性                                                             │
│     - messageId去重                                                    │
│     - 10分钟内不重复处理                                               │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 3.5.5 优缺点

| 优点 | 缺点 |
|------|------|
| 完全异步，不阻塞业务线程 | 需要维护消息队列 |
| 事务消息保证消息不丢 | 需要处理幂等 |
| 消息失败自动重试 | 实现相对复杂 |
| 性能高 | |
| 适合中高并发 | |

#### 3.5.6 适用场景

- 中高并发系统
- 对性能要求高的场景
- 允许短暂不一致的业务
- **推荐大多数高并发业务使用**

---

## 四、方案选型建议

### 4.1 选型决策树

```
                        ┌─────────────────┐
                        │ 业务场景是什么？ │
                        └────────┬────────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          │                      │                      │
          ▼                      ▼                      ▼
   ┌─────────────┐        ┌─────────────┐        ┌─────────────┐
   │  金融/转账   │        │  普通业务   │        │  简单查询   │
   │ 强一致性要求 │        │  最终一致即可│        │  读多写少   │
   └──────┬──────┘        └──────┬──────┘        └──────┬──────┘
          │                      │                      │
          ▼                      ▼                      ▼
   ┌─────────────┐        ┌─────────────┐        ┌─────────────┐
   │  分布式锁   │        │  消息队列    │        │  Cache Aside│
   │  或 Binlog │        │  (推荐)     │        │   (简单)    │
   └─────────────┘        └─────────────┘        └─────────────┘
```

### 4.2 场景推荐

| 场景 | 推荐方案 | 理由 |
|------|----------|------|
| 低并发简单业务 | Cache Aside | 实现简单，性能高 |
| 数据准确性要求极高 | 分布式锁 | 强一致性 |
| 大型高并发系统 | Binlog同步 | 无侵入，高可靠 |
| **中高并发系统（推荐）** | **消息队列** | **性能与一致性的平衡** |

---

## 五、总结

### 5.1 核心观点

1. **高并发场景下，延迟双删和分布式锁不是好选择**
   - 会阻塞业务线程
   - 影响系统吞吐量

2. **消息队列方案是最适合高并发场景的**
   - 完全异步，不阻塞
   - 事务消息保证可靠
   - 实现相对简单

3. **Binlog同步适合大型系统**
   - 完全解耦
   - 但复杂度高

### 5.2 阿里巴巴推荐

《Java开发手册》推荐： Canal + RocketMQ 方案

```
MySQL -> Binlog -> Canal -> RocketMQ -> 消费者 -> Redis
```

这是经过大规模验证的方案，可靠性高，适合高并发场景。

---

## 六、参考资料

- 阿里巴巴《Java开发手册》
- RocketMQ官方文档
- Canal官方文档
- Redis缓存设计最佳实践

# 技术方案文档

本文档记录了项目中各个技术方案的设计文档。

> 📖 **English Version**: [README-en.md](./README-en.md)

## 文档列表

| 文件名 | 说明 |
|--------|------|
| [seckill-system-design.md](./docs/seckill-system-design.md) | 秒杀系统技术方案（中文） |
| [seckill-system-design-en.md](./docs/seckill-system-design-en.md) | 秒杀系统技术方案（英文） |
| [cache-consistency-solution.md](./docs/cache-consistency-solution.md) | DB与缓存一致性方案（中文） |
| [cache-consistency-solution-en.md](./docs/cache-consistency-solution-en.md) | DB与缓存一致性方案（英文） |
| [skill-recommend-system.md](./docs/skill-recommend-system.md) | Skill特征提取与匹配系统（中文） |
| [skill-recommend-system-en.md](./docs/skill-recommend-system-en.md) | Skill特征提取与匹配系统（英文） |

## 文档说明

### 秒杀系统技术方案

该文档详细描述了秒杀场景下的技术方案，包括：

- **系统概述**：业务场景、核心目标、技术栈
- **系统架构**：整体流程、核心架构图
- **Redis Key设计**：各类Redis Key的用途
- **核心流程伪代码**：秒杀接口、库存扣减、限流防刷、MQ消费等
- **定时任务**：消息补偿、超时订单回滚、库存对账
- **Redis降级策略**：Redis故障时的降级方案
- **数据库设计**：订单表、本地消息表
- **核心场景解决方案**：
  - 超卖问题：多层防护（Redis Lua + MySQL乐观锁 + 库存对账）
  - 一人一单：Redis原子操作 + 数据库兜底
  - Redis与MySQL库存最终一致性：多阶段保障
  - 事务消息回滚：本地消息表 + 定时补偿
  - 防刷策略：多维度限流
  - Redis降级策略：降级查MySQL
- **确认清单**：技术方案配置确认

---

### DB与缓存一致性方案

该文档详细描述了高并发场景下DB与缓存数据一致性方案，包括：

- **问题背景**：经典一致性问题、一致性分类（强一致 vs 最终一致）
- **方案对比**：Cache Aside、延迟双删、分布式锁、Binlog同步、消息队列等多种方案
- **推荐方案**：采用 Canal 订阅 Binlog 实现缓存更新
- **方案详解**：Cache Aside 模式、延迟双删策略、分布式锁方案、异步更新策略（Canal + MQ）
- **代码实现**：各方案的伪代码示例
- **方案对比总结**：各方案的优缺点和适用场景

---

### Skill特征提取与匹配系统

该文档详细描述了基于 LangChain 的 Skill 特征提取与匹配推荐系统，包括：

- **问题背景**：用户上传 Skill 后，系统自动识别并推荐最相关的 Skill
- **输入类型**：skill描述、对话上下文、用户问题、技术文档
- **系统架构**：LangChain Chain + ChromaDB 向量存储 + FastAPI
- **核心模块**：特征提取（LLM）、向量存储（ChromaDB）、多路召回匹配（Jaccard + 向量）
- **技术栈**：langchain + langchain-openai + chromadb + fastapi
- **API 接口**：注册 Skill、推荐 Skill、健康检查
- **部署步骤**：依赖安装、环境配置、服务启动

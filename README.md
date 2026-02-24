# 技术方案文档

本文档记录了项目中各个技术方案的设计文档。

> 📖 **English Version**: [README-en.md](./README-en.md)

## 文档列表

| 文件名 | 说明 |
|--------|------|
| [seckill-system-design.md](./docs/seckill-system-design.md) | 秒杀系统技术方案（中文） |
| [seckill-system-design-en.md](./docs/seckill-system-design-en.md) | 秒杀系统技术方案（英文） |

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

# Technical Documentation

This documentation records the technical design documents for the project.

> 📖 **中文版本**: [README.md](./README.md)

## Document List

| Filename | Description |
|----------|-------------|
| [seckill-system-design.md](./docs/seckill-system-design.md) | Seckill System Design (Chinese) |
| [seckill-system-design-en.md](./docs/seckill-system-design-en.md) | Seckill System Design (English) |
| [cache-consistency-solution.md](./docs/cache-consistency-solution.md) | DB and Cache Consistency (Chinese) |
| [cache-consistency-solution-en.md](./docs/cache-consistency-solution-en.md) | DB and Cache Consistency (English) |
| [skill-recommend-system.md](./docs/skill-recommend-system.md) | Skill Feature Extraction and Match (Chinese) |
| [skill-recommend-system-en.md](./docs/skill-recommend-system-en.md) | Skill Feature Extraction and Match (English) |

## Document Description

### Seckill System Design

This document describes the technical solution for seckill scenarios in detail, including:

- **System Overview**: Business scenario, core goals, tech stack
- **System Architecture**: Overall process, core architecture diagram
- **Redis Key Design**: Various Redis key purposes
- **Core Process Pseudocode**: Seckill API, stock deduction, rate limiting, MQ consumption, etc.
- **Scheduled Tasks**: Message compensation, timeout order rollback, stock reconciliation
- **Redis Degradation Strategy**: Degradation plan when Redis fails
- **Database Design**: Order table, local message table
- **Core Scenario Solutions**:
  - Overselling prevention: Multi-layer protection (Redis Lua + MySQL optimistic locking + stock reconciliation)
  - One person one order: Redis atomic operations + database fallback
  - Redis and MySQL stock eventual consistency: Multi-stage guarantee
  - Transaction message rollback: Local message table + timed compensation
  - Anti-brush strategy: Multi-dimensional rate limiting
  - Redis degradation strategy: Degrade to query MySQL
- **Confirmation Checklist**: Technical solution configuration confirmation

---

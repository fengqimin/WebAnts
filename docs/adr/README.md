# 架构决策记录（ADR）

本目录记录 WebAnts 的架构级决策。每条 ADR 一经接受即冻结；后续变更通过新增 ADR 完成，不直接改写历史结论（勘误除外）。

## 状态说明

| 状态 | 含义 |
|------|------|
| Proposed | 已起草，待维护者评审 |
| Accepted | 已评审通过，作为实现与评审依据 |
| Superseded | 已被后续 ADR 取代 |
| Rejected | 已讨论但不采纳，保留理由 |

## 索引

| 编号 | 标题 | 状态 | 日期 | 关联问题 |
|------|------|------|------|----------|
| [ADR-0001](0001-callback-contract-and-retry-ownership.md) | 回调契约与重试归属 | Accepted | 2026-11-04 | P0-01、P1-03、P1-14、P1-19、P2-01 |
| [ADR-0002](0002-schedule-slot-ownership-and-request-lifecycle.md) | 调度槽位所有权与请求生命周期 | Accepted | 2026-11-04 | P1-01、P1-02、P1-04、P1-18、P1-20、P2-20、P2-23 |
| [ADR-0003](0003-shutdown-cancellation-and-loop-quiescence.md) | 关闭、取消与主循环收敛 | Accepted | 2026-11-04 | P1-02、P1-20、P2-23 |
| [ADR-0004](0004-data-correctness-link-filter-config-cache-signal.md) | 数据正确性契约：链接过滤、配置合并、解析缓存与信号顺序 | Accepted | 2026-11-05 | P1-05、P1-06、P1-07、P1-08、P1-15、P1-16、P1-17、P2-15、P2-16、P2-17 |

## 新增约定

- 文件名：`NNNN-<kebab-case-title>.md`，编号四位递增。
- 必填字段：状态、日期、基线、关联问题、决策、备选方案、影响、验收。
- 涉及破坏性变更时，必须在“影响与后果”中列出迁移方式。

# 待办（Backlog）

> 记录「已经做出决策、但尚未排期实施」的事项。每条待办必须有**来源、触发条件、最小范围、非目标与验收**；实施前先补 ADR，完成后回到对应 ADR / 问题清单更新状态。
>
> 与 [08 问题与建议](wiki/08-issues-and-recommendations.md) 的分工：08 记录已确认的缺陷与修复路线；本文件记录**依赖外部需求或后续决策**、当前不进入路线图的候选工作。

## 索引

| 编号 | 事项 | 状态 | 来源 | 目标 |
|------|------|------|------|------|
| BL-001 | 异步信号桥接（ADR-0006 候选） | 未排期 | [ADR-0005 §2.6.1](adr/0005-optional-layer-wiring-and-persistence-lifecycle.md)、08 P1-11 | 阶段 5 之后，待触发条件满足 |

---

## BL-001 异步信号桥接

| 项目 | 内容 |
|------|------|
| 状态 | **未排期（Not scheduled）** |
| 来源 | [ADR-0005 §2.6.1](adr/0005-optional-layer-wiring-and-persistence-lifecycle.md)、[08 P1-11](wiki/08-issues-and-recommendations.md)、[08 §8.10](wiki/08-issues-and-recommendations.md)「两套 Signal 合并」 |
| 关联决策 | [ADR-0003](adr/0003-shutdown-cancellation-and-loop-quiescence.md)（关闭收敛）、[ADR-0004](adr/0004-data-correctness-link-filter-config-cache-signal.md)（异步信号排序）、[ADR-0005](adr/0005-optional-layer-wiring-and-persistence-lifecycle.md)（可选层接入） |
| 决策归属 | 实施前必须新开 **ADR-0006** 并 Accepted；本待办只冻结「做什么」，不替代 ADR |
| 预期影响 | `webants/spider.py`、`webants/libs/signals.py`、文档；不改主链路默认行为 |

### 背景

ADR-0005 决定阶段 4 不桥接异步 `SignalManager`：Spider 生命周期只走同步 `Spider.signals`，异步总线保持实验性。该决定避免了热路径延迟、同步 done callback 无法 await、全局单例多 Spider 串扰，以及关闭终态新增排空条件四类风险。本待办记录「如果未来确实需要桥接，最低限度要满足什么」，避免届时重新论证。

### 触发条件（满足任一才排期）

1. 出现 **≥ 2 个真实的异步生命周期订阅者**，而不是测试或示例代码；
2. 有明确需求要在事件处理器里 `await` I/O（同步 `Signal` 做不到）；
3. 需要跨 Spider 的全局事件聚合（同步信号是实例级）；
4. 且已有性能预算或压测数据，证明事件投递不会成为抓取瓶颈。

未满足以上条件时，继续维持「同步总线负责生命周期、异步总线仅供可选层内部使用」的分工。

### 最小范围（不可裁剪）

1. **显式开关**：`Spider(emit_global_signals: bool = False)`，默认关闭；开启才投递异步事件。
2. **固定映射表**：见下方建议表，评审时冻结；未定义的事件不投递，避免语义膨胀。
3. **有界队列 + 单一 dispatcher**：同步上下文（`_on_task_done` 等）产生的事件不得内联 await，进入有界队列；队列满时有明确丢弃策略（建议丢弃并累加 `signal_dropped`），绝不阻塞抓取。
4. **关闭排空**：`close()` 顺序为「停止生产 → 关闭队列 → 等待 dispatcher 退出 → 发 `spider_closed` → `_closed=True`」；关闭后不得再有事件投递。
5. **多 Spider 隔离**：异步全局接收器按 `sender` 过滤；测试需覆盖两个 Spider 并发时事件不串扰。
6. **同步边界不变**：同步 `Spider.signals.connect` 已拒绝协程函数（ADR-0005 I13），桥接不得放宽这条守卫。
7. **异常策略**：单个异步处理器异常不得中断其他处理器或抓取；沿用 `Signal.send` 的逐接收器捕获与 `exception_tracker.track`。
8. **测试**：顺序/优先级、取消、多 Spider、队列满、关闭排空、处理器异常、开关关闭时零投递。
9. **文档**：明确两套总线的分工；若长期只保留一套，需在 ADR-0006 中给出弃用计划。

### 事件映射（建议，评审时冻结）

| 同步事件 / 时机 | 异步信号 | 触发点 | 备注 |
|-----------------|----------|--------|------|
| `spider_started` | `spider_opened` | `start()` 发同步信号之后 | 仅开关开启时投递 |
| `spider_stopped` | `spider_closed` | `close()` 排空完成后 | 必须最后投递 |
| `request_scheduled` | `request_scheduled` | `_schedule` 真正入队后 | 与同步信号同点 |
| 去重/队列满/限额/中间件过滤 | `request_dropped` / `request_reached` | 对应拒绝分支 | `reason` 取值固定 |
| 下载成功或缓存命中 | `response_received` | `process_response` 之前或之后 | 需明确缓存命中是否也算 |
| 回调产出 `Result` | `item_scraped` | 同步 `item_scraped` 之后 | 持久化失败另发 `item_error` |
| 持久化失败 | `item_error` | `persist_failures` 累加处 | 单个 Result 可能对应多个后端失败 |
| 请求最终失败 | **不映射** | — | 异步总线没有 `request_finished`；新增事件需另论证，避免与 `item_error` 混淆 |

### 非目标

- 不合并两套 `CircuitBreaker`；
- 不默认启用异步投递；
- 不改变同步 `Signal` 的「发送即执行完」语义；
- 不做无界队列、阻塞投递或「丢弃前重试」；
- 不在本待办中处理 P2-08（ujson）等无关清理项。

### 验收

- [ ] `Spider()` 默认不向 `signal_manager` 投递任何生命周期事件（spy 断言零次）。
- [ ] `Spider(emit_global_signals=True)` 的事件顺序符合映射表；`spider_closed` 是最后一个。
- [ ] 同步 done callback 产生的取消/逃逸异常事件经队列投递，且 `close()` 等其排空。
- [ ] 队列满时按既定策略丢弃并计数，抓取不阻塞。
- [ ] 两个 Spider 并发时，按 sender 过滤后各自只收到自己的事件。
- [ ] 处理器抛异常不影响其他处理器与抓取；`exception_tracker` 有记录。
- [ ] 关闭后不再有事件投递；`_closed=True` 之后调用 `send` 被拒绝或忽略并可观测。
- [ ] 文档与 ADR-0006 的状态、映射表、关闭顺序一致。

### 风险与开放问题

| # | 问题 | 建议默认值 |
|---|------|-----------|
| 1 | 是否只桥接生命周期事件（start/stop），不桥接按条/按页事件 | 先只桥接生命周期，降低热路径风险 |
| 2 | 队列容量与丢弃策略 | 容量 1024；满则丢弃并累加 `signal_dropped` |
| 3 | 缓存命中是否发 `response_received` | 发，并带 `from_cache=True` 语义（需新参数或 meta） |
| 4 | 是否最终弃用同步 `Spider.signals` | 不在 ADR-0006 中决定；另开迁移 ADR |
| 5 | dispatcher 是否单独 task | 是；由 `close()` 显式停止并 await |

### 参考

- [ADR-0005 §2.6.1](adr/0005-optional-layer-wiring-and-persistence-lifecycle.md) — 不桥接的四类硬约束与方案对比
- [ADR-0004 §2.4](adr/0004-data-correctness-link-filter-config-cache-signal.md) — 异步信号排序键 `(-priority, seq)`、`sender=None` 修正
- [ADR-0003](adr/0003-shutdown-cancellation-and-loop-quiescence.md) — 关闭收敛与 `close()` 状态机
- 代码位置：`webants/spider.py:26-69,503-522,584-633`、`webants/libs/signals.py:25-280`

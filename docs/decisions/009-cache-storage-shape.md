# 009 — CacheWriter 第一版存「原始报文」而不是结构化字段

> 编号 009。由 004（为什么 Cache 用批量写入）派生。
>
> 状态：已决定（第一版），**将来可能需要重新审视**。

---

## 问题

写 `CacheWriter` 时很自然地想设计这样一张表：

```sql
CREATE TABLE ticks (symbol TEXT, ts INTEGER, price REAL, volume REAL);
```

**但这四个字段在 `TickData` 里根本不存在。** 它实际是：

```cpp
struct TickData {
    uint64_t id;
    std::string topic;                              // "ws:kraken:ticker:BTC/USDT"
    std::string payload;                            // 原始报文（JSON 字符串）
    std::chrono::steady_clock::time_point received_at;
};
```

**`TickData` 是「原始报文容器」，不是「已解析的行情」。** 这个形态是 M0 定的，
整个 M2 也是按它设计的（解析发生在 worker 里，结果就地用掉）。

---

## 两个方案

| | A. 存原始形态 | B. 存结构化字段 |
|---|---|---|
| 表结构 | `msg_id / topic / payload / t0_ns / t4_ns` | `symbol / ts / price / volume` |
| 立刻可用 | ✅ | ❌ |
| SQL 查询 | ❌ 想算"BTC 均价"得先解析 JSON | ✅ 直接查 |
| 存储体积 | ❌ JSON 冗余 | ✅ 紧凑 |
| 代价 | 查询/聚合难 | **需要先把 payload 解析出来** |

## 选 A 的理由

**不是"图省事"，是因为 B 引出的问题在这一层解决不了。**

要让 `CacheWriter` 拿到结构化的 `symbol/price/volume`，就必须让**解析结果也流进管道** ——
也就是 `MessageQueue` 要承载领域类型（`TickerData` 之类）。

**那 `src/core/` 就得知道业务类型，直接违反依赖倒置**（PLAN 决策 002）。

这是**真实的架构张力**，不是实现细节：

```
现状：  WebSocket ─→ Queue<原始报文> ─→ worker 解析 ─→ 就地用掉
                                              │
                                              └─ 解析结果没有回传给管道

方案B： WebSocket ─→ Queue<原始报文> ─→ worker 解析 ─→ Queue<领域对象> ─→ CacheWriter
                                              ↑
                                    多一级队列，且 core 要认识领域类型
```

而且第一版的目标是**测出攒批的收益**（M3 的验收标准），不是设计最终存储格式。
**先跑通、先拿到数字，把张力记下来。**

---

## 顺带记下的两个小决定

**① 不建索引。**
本场景**读少写多**，索引会拖慢每一次 INSERT，而收益在查询侧。
（将来真要建，`topic` 上建一个就够。）

**② `t0_ns` / `t4_ns` 用 `steady_clock`（相对开机时刻）。**

- 好处：单调、不受系统时间调整影响，适合测延迟
- **代价：只在单次运行内有意义，跨进程比较没有价值**

要真正的墙上时间，得给 `TickData` 加一个 `system_clock` 字段 —— 那是另一件事。

---

## 什么时候该重新审视这条决策

出现下面任一情况，就该回来重做：

- [ ] 需要**按 symbol / 价格区间查询**历史数据（比如做回测取数）
- [ ] 存储体积成为问题（JSON 冗余 vs 二进制）
- [ ] 需要**跨进程/跨天**分析（那 `steady_clock` 就不够用了，要墙上时间）

**届时最可能的做法**：让 worker 解析后**同时**投递一份结构化结果给 CacheWriter，
而原始的 `MessageQueue` 保持只走原始报文 ——
相当于 `CacheWriter` 挂在一个"已解析"的旁路上，不污染 core。

---

## 待验证

- 存原始 JSON 的实际存储放大倍数是多少？（同一批数据，两种表结构各占多少磁盘）
- 从 JSON 里查一条历史数据的成本，是否真的不可接受？

# FinceptPipeline

> 一个实时行情异步处理管道。C++20 / STL / Qt6 / WebSocket / SQLite。
>
> 基于 [FinceptTerminal](https://github.com/Fincept-Corporation/FinceptTerminal)（C++20/Qt6
> 开源金融终端，2010 个源文件、16 个交易所）二次开发，**替换其原有的
> 「WebSocket → 主线程 → DataHub」链路**。

---

## 为什么这个项目值得看

### 一、抽象被三个真实实现验证过

核心只认两张窄接口（`IWebSocketClient` / `IPublisher`），实现是什么它不知道。
这两张名片先后被**三个完全不同的东西**实现：

| 实现 | 形态 |
|---|---|
| `MockWebSocketClient` | 自己的线程 + 条件变量 |
| `KrakenWebSocketClient` | Qt 事件循环 + 线程亲和性 + 真实网络 |
| `ws_stream.py` 子进程 | 跨进程 + QProcess 信号 |

**三次替换，`src/core/` 零改动。**

> 一个实现证明不了抽象是否成立。**三个可以。**

### 二、在真实系统里找到了瓶颈并消除它

在 FinceptTerminal 里定位到：行情报文的 **JSON 解析、对象构造、缓存更新全在主线程**。
原作者已经用「每轮最多 50 行 / 4 毫秒」的时间预算兜底 —— **这本身就是瓶颈的自证**，
代价是积压时数据滞后。

改造后，主线程只做一次字符串前缀判断 + 入队，解析/构造/发布全部移入 worker 池。
**真实行情端到端跑通：T0→T3 P50 = 92μs，双交易所并发无丢弃、P99 平稳。**

### 三、性能数字是测出来的，而且推翻过两次估算

| 实验 | 结果 |
|---|---|
| 批量写入 SQLite | **15.6× 加速**，拐点在 `batch_size = 1000` |
| 对照三种落盘策略 | 攒批加速 **16.4× ~ 165.3×**，且**抹平了策略差异** |
| 反直觉结论 | 攒批之后可以放心用 `WAL+FULL`（断电不丢）—— **不再需要为性能牺牲持久性** |

过程中**两处估算被自己的实测推翻**：fsync 代价高估 4 倍、单条 INSERT 高估 20 倍。
两处都写进了文档并标注修正。

---

## 架构

```
        ┌──────────────┐
行情源  │ Mock / Kraken│
        │ / ws_stream  │   ← 三个实现，同一张名片
        └──────┬───────┘
               │ raw 报文
        ┌──────▼───────┐
        │ AsyncReceiver│   盖 T0（收到时刻）
        └──────┬───────┘
               │
        ┌──────▼───────┐
        │ MessageQueue │   有界 + 3 种背压策略
        └──────┬───────┘
               │
        ┌──────▼───────┐
        │  ThreadPool  │   盖 T2/T3（取出 / 处理完）
        │   (workers)  │
        └──┬────────┬──┘
           │        │
   ┌───────▼──┐  ┌──▼──────────┐
   │CacheWriter│  │  Publisher  │
   │ 批量SQLite│  │  (接口)      │   ← 另一张名片
   │  (T4)     │  │   → DataHub  │
   └───────────┘  └─────────────┘
```

**依赖原则**：`src/core/` 只依赖 C++20 STL —— 不依赖 Qt、不依赖 Fincept。
所以核心可以独立编译、独立测试、独立 benchmark。

---

## 构建与运行

```bash
# 核心（不需要 Qt）
cd build && cmake .. -G Ninja && ninja && ctest --output-on-failure   # 12 个测试

# 需要 Qt 的部分（Kraken 适配器 + 基准工具）
cmake .. -G Ninja -DCMAKE_PREFIX_PATH=/opt/homebrew/opt/qt
ninja
./cache_bench 20000 batch     # batch_size 扫描
./cache_bench 20000 pragma    # 落盘策略对照
./test_kraken_live            # 真实行情联调（需 VPN）
```

---

## 实测数据

### M1 — 背压策略（9 组：3 速率 × 3 策略）

| 策略 @ 10000/s | 丢弃 | queue_wait P50 |
|---|---|---|
| Block | 0（生产被拖慢） | 226 ms |
| DropOldest | 54879 | **100 ms** |
| DropNewest | 54762 | 225 ms |

> **反直觉**：直觉认为「拒绝新数据 → 队列不满 → 等待短」，
> 实测 DropNewest 的等待反而是 Block 同级的 225ms。**丢谁不重要，关键是队列里剩的是谁。**

### M3 — 批量写入

| batch_size | 总耗时 | COMMIT 数 | 吞吐 |
|---|---|---|---|
| 1 | 0.532 s | 20000 | 37,605/s |
| 100 | 0.039 s | 200 | 518,212/s |
| **1000** | **0.034 s** | 20 | **581,745/s** |
| 10000 | 0.042 s | 2 | 473,903/s |

| 落盘策略 | 每次 COMMIT | 单条写 | 攒批(1000) | 加速比 |
|---|---|---|---|---|
| WAL + NORMAL | 27.8 μs | 0.555 s | 0.034 s | 16.4× |
| WAL + FULL | 58.3 μs | 1.166 s | 0.034 s | 34.5× |
| DELETE + FULL | 256.4 μs | 5.128 s | 0.031 s | **165.3×** |

---

## 设计决策日志

关键取舍都记在 [`docs/decisions/`](docs/decisions/)。其中三篇是完整的论证文档：

| 编号 | 决策 |
|---|---|
| [007](docs/decisions/007-cachewriter-thread-model.md) | CacheWriter 为什么用独立线程 + 自己的队列 |
| [008](docs/decisions/008-messagequeue-timed-dequeue.md) | 为什么给核心加"带超时的等待"（**主动打破零改动**） |
| [009](docs/decisions/009-cache-storage-shape.md) | 为什么第一版存原始报文而非结构化字段 |

001~006 是早期架构选型（一行能说清），索引见 [`PLAN.md`](PLAN.md) 第十节。

---

## 已知边界（诚实）

- **M2 那次改造的收益是推算的**：实测负载 145 msg/s vs 临界区间 1,120~11,250 msg/s
  —— 也就是说**在当前负载下这个优化几乎没用**。这是测量之后才知道的，也写进了文档。
- `CacheWriter` 的测试尚未覆盖"确定性地制造队列拥塞"这条路径。
- 尚未做多机 / 分布式场景。

---

## 文档

| 文件 | 内容 |
|---|---|
| [`PLAN.md`](PLAN.md) | 架构方案、路线图、决策索引 |
| [`LEARNING_LOG.md`](LEARNING_LOG.md) | 学习笔记、踩过的坑、面试自查 |
| [`HANDOFF.md`](HANDOFF.md) | 会话交接（进度、环境、已知边界） |

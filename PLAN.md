# FinceptPipeline — 实时行情异步处理框架

> 基于 [FinceptTerminal](https://github.com/Fincept-Corporation/FinceptTerminal)（C++20/Qt 6.8）二次开发的高性能实时数据处理 Pipeline。
>
> **目标岗位方向**：C++ 后端 / AI Infra

---

## 一、项目定位

不是在 FinceptTerminal 上"加功能"，而是在其之上**设计一个独立的异步数据处理子系统**：

- 从原有 WebSocket → 主线程 → DataHub 的链路中
- 抽出一条 **WebSocket → Async Receiver → Queue → Worker Pool → DataHub** 的异步链路
- 加上全链路延迟追踪和 benchmark

核心锻炼能力：**C++ 并发、系统设计、性能工程**（不涉及 UI、不涉及金融算法、不写 lock-free）。

---

## 二、分层架构（关键设计决策）

项目分为两层：**Pipeline Core（纯 C++）**和 **Fincept Adapter（Qt 桥接层）**。

```
              ╔══════════════════════════════╗
              ║     LatencyTracker          ║  ← 横切模块（非处理节点）
              ║  (全链路埋点 + P50/P95/P99)   ║     所有阶段打时间戳
              ╚══════════════════════════════╝
                        ↑ ↑ ↑ ↑ ↑
                        │ │ │ │ │
┌─────────────────────────────────────────────────────────────┐
│                    FinceptTerminal                          │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐      │
│  │WebSocket │    │  DataHub │    │  UI Subscribers  │      │
│  │ (券商行情) │    │ (pub/sub)│    │  (界面消费)       │      │
│  └────┬─────┘    └────▲─────┘    └──────────────────┘      │
│       │               │                                     │
├───────┼───────────────┼─────────────────────────────────────┤
│       │    Fincept Adapter (Qt 桥接层)                       │
│       │               │                                     │
│  ┌────┴───────────────┴──────┐                              │
│  │     FinceptPipelineBridge │  ← 唯一依赖 Qt 的模块         │
│  │  - AsyncReceiver (QObject)│                              │
│  │  - DataHubPublisher       │                              │
│  └───────────┬───────────────┘                              │
│              │                                               │
├──────────────┼──────────────────────────────────────────────┤
│              │    Pipeline Core（纯 C++，无 Qt 依赖）         │
│              ▼                                               │
│  ┌───────────────────────────┐                              │
│  │      MessageQueue<T>      │  ← std::mutex + queue + cv    │
│  │   (M0: 仅 Block 策略,      │                              │
│  │    M1: 扩展 Drop 策略)     │                              │
│  └───────────┬───────────────┘                              │
│              ▼                                               │
│  ┌───────────────────────────┐                              │
│  │    Worker Loop × N        │  ← dequeue → process 循环     │
│  │   (long-running workers,  │    非 submit/future 模式      │
│  │    ThreadPool 管理生命周期) │                              │
│  └───────┬───────────┬───────┘                              │
│          │           │                                       │
│          ▼           ▼                                       │
│  ┌────────────┐ ┌────────────┐                               │
│  │ CacheWriter│ │  Publisher │  ← 抽象接口，实现在 Adapter    │
│  │ (批量SQLite)│ │  (接口)     │                              │
│  └────────────┘ └────────────┘                               │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 依赖原则

```
pipeline/core/     → 只依赖 C++20 STL，不依赖 Qt，不依赖 Fincept
pipeline/adapters/ → 依赖 Qt + Fincept（DataHub, WebSocketClient）
```

这样做的好处：
- Pipeline Core 可以**独立编译、独立测试、独立 benchmark**
- 未来可以**单独拿出来**作为 `libpipeline` 复用
- 面试时清楚展示"哪些是我写的，哪些是 Fincept 原有的"

---

## 三、数据流 + 时间线

```
T0  WebSocket 收到原始 tick (网络线程)
T1  进入 MessageQueue, 记录 enqueue 时间戳
T2  Worker 取出, 记录 dequeue 时间戳
T3  Worker 处理完成（反序列化/转换/校验）
T4  CacheWriter 批量写入 SQLite
T5  DataHub::publish() 发布给原有订阅者
T6  UI Consumer 收到

统计指标：
  queue_wait_time    = T2 - T1   (排队等待延迟)
  processing_time    = T3 - T2   (业务处理延迟)
  cache_write_time   = T4 - T3   (存储延迟)
  publish_latency    = T5 - T4   (分发延迟)
  end_to_end_latency = T5 - T0   (端到端延迟)
```

注意：LatencyTracker 不是 Pipeline 中的一个处理节点，而是**横切模块（cross-cutting concern）**——在 T0-T5 的每个阶段打时间戳，不参与数据流向。

---

## 四、目录结构

```
fincept-qt/src/pipeline/
├── core/                       # 纯 C++，零外部依赖
│   ├── MessageQueue.h          # 模板类，有界线程安全队列
│   ├── ThreadPool.h            # 固定大小线程池（管理 Worker 生命周期）
│   ├── LatencyTracker.h        # 全链路延迟记录 + P50/P95/P99 统计
│   ├── LatencyTracker.cpp
│   └── Types.h                 # TickData, LatencySample 等通用类型
├── adapters/                   # Qt/Fincept 桥接层
│   ├── AsyncReceiver.h         # WebSocket 收包线程
│   ├── AsyncReceiver.cpp
│   ├── DataHubPublisher.h      # 对接 Fincept DataHub::publish()
│   ├── DataHubPublisher.cpp
│   ├── CacheWriter.h           # 对接 Fincept CacheManager, 批量写
│   └── CacheWriter.cpp
├── benchmark/
│   ├── MockDataGenerator.h     # 可控速率数据发生器
│   ├── MockDataGenerator.cpp
│   └── pipeline_bench.cpp      # 压力测试入口 (1000/5000/10000 msg/s)
├── tests/
│   ├── test_message_queue.cpp
│   ├── test_thread_pool.cpp
│   └── test_determinism.cpp    # 确定性测试：多线程输出 == 单线程输出
├── docs/
│   └── decisions/              # 设计决策记录 (Design Decision Log)
│       ├── 001-thread-model.md
│       ├── 002-no-lockfree-v1.md
│       └── 003-batch-write-strategy.md
├── CMakeLists.txt              # pipeline 模块独立构建
└── README.md
```

---

## 五、核心类接口（V0 确定，后续不轻易改接口签名）

### TickData — Pipeline 内部统一消息类型

```cpp
struct TickData {
    uint64_t id;                       // 单调递增序列号
    std::string topic;                 // 对应 DataHub topic，如 "market:quote:BTC-USD"
    std::string payload;               // [V0] JSON 字符串，模拟真实数据
                                       // [V1] 替换为结构化 Tick 对象，避免反复 parse
    std::chrono::steady_clock::time_point received_at;  // T0
};
```

### MessageQueue<T> — 有界阻塞队列

```cpp
template <typename T>
class MessageQueue {
public:
    // [V0] 仅实现 Block 策略 — 先验证 Pipeline 架构跑通
    // [M1] 扩展 DropOldest / DropNewest — 分别 benchmark 对比
    enum class BackpressurePolicy {
        Block,        // 队列满时阻塞等待
        DropOldest,   // 丢弃队首（最旧数据）— 适合行情场景
        DropNewest,   // 拒绝新数据，返回 false
    };

    explicit MessageQueue(size_t max_size = 100000);

    // 生产者调用
    bool enqueue(T item);            // policy=DropNewest 时可能返回 false
    bool try_enqueue(T item);        // 非阻塞，满则返回 false

    // 消费者调用
    T dequeue();                     // 空时阻塞等待
    bool try_dequeue(T& item);       // 非阻塞，空则返回 false

    // 生命周期
    void shutdown();                 // 唤醒所有阻塞线程
    bool is_shutdown() const;

    // 状态
    size_t size() const;
    size_t capacity() const;

private:
    std::queue<T> queue_;
    size_t max_size_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::atomic<bool> shutdown_{false};
};
```

### ThreadPool — 管理 Worker 生命周期

```cpp
// ThreadPool 负责创建/销毁 Worker 线程，但不采用 submit/future 模式。
// Worker 是 long-running loop：while(running) { msg = queue.dequeue(); process(msg); }
// ThreadPool 只管理线程生命周期（启动、优雅关闭），不做任务调度。

class ThreadPool {
public:
    explicit ThreadPool(size_t num_threads);
    ~ThreadPool();                       // 自动调用 shutdown()

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // 启动所有 Worker。每个 Worker 运行 worker_func，直到 stop_ 为 true。
    void start(std::function<void(std::stop_token)> worker_func);

    // 优雅关闭：设置 stop_ → 唤醒所有 Worker → join
    void shutdown();
    bool is_running() const;
    size_t worker_count() const;

private:
    std::vector<std::thread> workers_;
    std::atomic<bool> stop_{false};
};
```

### LatencyTracker — 全链路延迟统计（横切模块）

```cpp
// 单条消息的完整生命周期时间戳
struct TickLatency {
    std::chrono::steady_clock::time_point received_at;    // T0: WebSocket 收到
    std::chrono::steady_clock::time_point enqueued_at;    // T1: 入队
    std::chrono::steady_clock::time_point dequeued_at;    // T2: Worker 取出
    std::chrono::steady_clock::time_point processed_at;   // T3: 处理完成
    std::chrono::steady_clock::time_point cached_at;      // T4: Cache 写入
    std::chrono::steady_clock::time_point published_at;   // T5: DataHub 发布
};

// 聚合统计结果
struct LatencyStats {
    double queue_wait_p50_us, queue_wait_p95_us, queue_wait_p99_us;
    double processing_p50_us, processing_p95_us, processing_p99_us;
    double end_to_end_p50_us, end_to_end_p95_us, end_to_end_p99_us;
    double avg_us, max_us, min_us;
    size_t total_processed;
    double throughput_msg_per_sec;
};

class LatencyTracker {
public:
    void record(const TickLatency& sample);
    LatencyStats snapshot() const;   // 返回当前统计，不清空
    void reset();                     // 清空历史数据
    size_t sample_count() const;

    // [未来] 线上运行时可切换为采样模式（如 1%），避免 vector 无限膨胀
    void set_sampling_rate(double rate);  // 0.0 ~ 1.0, 1.0 = 全量

private:
    mutable std::mutex mutex_;
    std::vector<TickLatency> samples_;
    double sampling_rate_ = 1.0;  // M1: 全量; 后续可降为采样
};
```

### AsyncReceiver（V1，在 adapters/ 中）

```cpp
class AsyncReceiver : public QObject {
    Q_OBJECT
public:
    AsyncReceiver(std::shared_ptr<MessageQueue<TickData>> queue,
                  QObject* parent = nullptr);
    ~AsyncReceiver();

    void connect_to(const QString& ws_url);
    void disconnect();
    bool is_connected() const;

signals:
    void connection_state_changed(bool connected);
    void error_occurred(const QString& message);

private:
    void receiver_loop();   // 在独立线程中运行, 复用 Fincept WebSocketClient
};
```

### DataHubPublisher（V1，在 adapters/ 中）

```cpp
class DataHubPublisher {
public:
    // 把处理后的 TickData 发布到 Fincept DataHub
    void publish(const TickData& tick);
};
```

### CacheWriter（V3，在 adapters/ 中，后续细化）

```cpp
class CacheWriter {
public:
    // batch_size: 攒够多少条触发写入
    // flush_interval: 超过多久没写入触发强制刷新
    CacheWriter(size_t batch_size = 1000,
                std::chrono::milliseconds flush_interval = std::chrono::seconds(1));

    void write(const TickData& tick);
    void flush();   // 强制刷新
    void stop();

private:
    // [V3] 细化：内部 ring buffer 攒批、对接 Fincept CacheManager
};
```

---

## 六、Performance Goals（性能目标）

每次 Milestone 产出 benchmark 时，对比以下指标：

| 指标 | 测量方法 | 备注 |
|------|----------|------|
| **Throughput** | msg/s 处理能力 | 对比 Producer 速率 vs 实际处理速率 |
| **Queue Depth** | MessageQueue::size() 采样 | 观察是否积压 |
| **P50/P95/P99 latency** | LatencyTracker::snapshot() | 核心指标 |
| **CPU usage** | 系统工具 | 观察线程数是否合理 |
| **丢包率** | 发送总数 vs 接收总数 | DropOldest 模式下的数据完整性 |

### 对比基线

```
V0: 直接处理（无 Pipeline，单线程同步）
V1: 引入 Pipeline（有界队列 + 线程池）
V2: 扩展背压策略（Block vs DropOldest vs DropNewest）
V3: 批量写入 SQLite（单条 vs 100条/批 vs 1000条/批）
```

每个版本产出一份对比数据，最终形成**优化演进报告**，面试时直接展示。

---

## 七、开发 Milestones

### M0 — 纯 C++ Pipeline 核心（预期 ~2 周）

**依赖**：仅 C++20 STL + CMake

- [ ] 实现 `MessageQueue<T>`（M0 **仅 Block 策略**，Drop 策略延后到 M1）
- [ ] 实现 `ThreadPool`（start/shutdown，管理长生命周期 Worker）
- [ ] 实现 Worker Loop：`while(running) { msg = queue.dequeue(); process(msg); }`
- [ ] 实现 `MockDataGenerator`（可控制速率）
- [ ] 串联跑通：MockDataGenerator → MessageQueue → Worker Loop → 处理
- [ ] 确定性测试：N 条消息，多线程输出与单线程一致

**验收标准**：10000 msg/s 下不丢数据、不崩溃。

### M1 — 背压策略 + 延迟追踪（预期 ~1-2 周）

**依赖**：仅 M0

- [ ] MessageQueue 扩展 DropOldest / DropNewest 策略
- [ ] 实现 `LatencyTracker`（全链路埋点，P50/P95/P99 统计）
- [ ] 实现 `pipeline_bench`：3 档压力（1000/5000/10000 msg/s）× 3 种策略
- [ ] 产出第一版 benchmark 报告（对照第六章指标）

**验收标准**：对比 3 种策略在不同负载下的 P50/P95/P99/throughput。

### M2 — Fincept 接入（预期 ~2-3 周）

**依赖**：FinceptTerminal 编译通过（环境搭建可能需要额外时间）

- [ ] 实现 `AsyncReceiver`：持有 Fincept WebSocketClient，独立线程收包
- [ ] 实现 `DataHubPublisher`：处理后数据发布到 DataHub
- [ ] 选用一个数据源对接（Kraken WebSocket 行情）
- [ ] 真实数据流通测试

**验收标准**：真实 Kraken 行情经过 Pipeline 处理后出现在 DataHub。

### M3 — Cache 批量写入 ✅ 完成（2026-09-24）

- [x] 实现 `CacheWriter`（攒批策略：batch_size OR flush_interval 触发）
      —— 独立线程 + 自己的队列（决策 007）；顺带给 MessageQueue 加了 `dequeue_for`（决策 008）
- [ ] 对接 Fincept `CacheManager`
      —— **未做。且发现 PLAN 原本的措辞可能是个误解**：Fincept 的 `CacheManager`
      是**响应缓存**（key-value + TTL），不是时序存储，和 CacheWriter 不是一回事。
      详见决策 009。要不要接、接什么，等真需要时重新评估。
- [x] 性能对比：单条写 vs 批量写，并额外做了落盘策略对照

**验收标准**：批量写入延迟明显低于单条写入，找到最佳 batch_size。✅

**实测结果**（2 万条）：

| | 结果 |
|---|---|
| 攒批加速 | **15.6×**（WAL+NORMAL，batch_size=1000） |
| 最佳 batch_size | **1000**（10000 反而变慢） |
| 落盘策略影响 | 攒批加速 16.4× ~ **165.3×**，取决于 `synchronous` |
| **最大收获** | **攒批抹平了落盘策略差异 → 可以白拿持久性**（用 FULL 不再牺牲性能） |

**遗留**：CacheWriter 尚无单元测试（目前靠 benchmark 顺带验证行数）。

### M4 — 文档 + 简历（预期 ~0.5 周）

- [ ] 整理 Design Decision Log（**现已 9 篇**）
- [ ] 更新 README（架构图、构建说明、benchmark 结果）
- [ ] 撰写简历项目描述

---

## 八、测试策略

| 测试类型 | 内容 | Milestone |
|----------|------|-----------|
| 单元测试 | MessageQueue 线程安全、ThreadPool 生命周期 | M0 |
| 确定性测试 | N 条消息，多线程输出 = 单线程输出 | M0 |
| 压力测试 | 1000 / 5000 / 10000 msg/s × 3 种策略 | M1 |
| 延迟基准 | P50/P95/P99 报告 | M1 |
| 集成测试 | 真实 WebSocket 数据流通 | M2 |
| 性能对比 | 单条写 SQLite vs 批量写 | M3 |

---

## 九、不做的事情（明确排除）

- ❌ 不自己实现 lock-free queue（V0 用 std::mutex，后续可换 moodycamel::ConcurrentQueue）
- ❌ 不自己实现内存分配器（arena/slab）
- ❌ 不修改 Fincept 原有 UI
- ❌ 不新增金融算法/策略
- ❌ 不试图理解整个 FinceptTerminal 项目
- ❌ 不碰 CUDA/GPU

---

## 十、Design Decision Log（面试加分项）

**两类决策，不要混为一谈：**

| 类型 | 编号 | 形态 | 什么时候算够 |
|------|------|------|-------------|
| **选型决策** | 001~006 | 一行式（就在下面的表里） | 能说清「选了什么、为什么、排除了什么」 |
| **取舍决策** | 007~009 | [完整文档](docs/decisions/) | 有**代价**、有**反方论证**、有**被推翻的假设** |

**区分的理由**：早期架构选型是「在几个方案里挑一个」，一行能说清；
而 007~009 撞上的是**真实的架构张力**（fsync 该谁承受、core 该不该破例、存储形态无法两全）
—— 那种决策**必须有论证过程**，否则别人无法判断你想清楚了没有。

> **"知道哪些决策需要展开论证、哪些不需要"本身也是判断力。**
> 强行给 001~006 各写半页，反而是噪音。

---

| 编号 | 决策 | 要点 |
|------|------|------|
| 001 | 为什么第一版用 `std::mutex` 而不是 lock-free queue | 当前阶段首要目标是验证 Pipeline 架构正确性，不是极限吞吐。mutex 版可确定性测试，调通后替换为成熟库成本低 |
| 002 | 为什么 Pipeline Core 不依赖 Qt | 保证核心模块可独立编译/测试/benchmark；未来可单独作为 `libpipeline`；面试时清楚展示个人贡献边界 |
| 003 | 为什么 Worker 用 dequeue loop 而非 submit/future | 行情处理是持续流，不是离散任务。long-running worker + dequeue 比反复 submit 更符合流处理语义，避免 future 创建开销 |
| 004 | 为什么 Cache 用批量写入 | 单条写要付【每次事务的固定开销】；攒批把它摊薄。**实测（2026-09-24）**：每次 COMMIT 27.8μs(WAL+NORMAL) ~ 256.4μs(DELETE+FULL)，**攒批加速 16.4× ~ 165.3×**，拐点在 batch_size=1000。⚠️ 原写的"0.5-2ms"是高估 —— 现代 NVMe 快得多。详见 LEARNING_LOG 的 M3 段 |
| 005 | 为什么选用有界队列 + 背压策略 | 无界队列在消费者慢于生产者时会导致 OOM；有界队列 + DropOldest 适合行情场景（旧数据价值衰减快）；Block 策略适合对数据完整性有要求的场景 |
| 006 | 为什么 M0 只实现 Block 策略 | 减少首版复杂度，先验证 Pipeline 架构正确性；Drop 策略涉及丢弃逻辑和边界条件，在 M1 单独实现和 benchmark |
| 007 | 为什么 CacheWriter 用独立线程 + 自己的队列 | [全文](docs/decisions/007-cachewriter-thread-model.md)。**不能让慢路径拖住快路径** —— 写盘是"档案"性质、晚 50ms 无所谓，而 worker 的处理延迟不能被 fsync 污染（伤 P99）。独立线程承受 fsync；单一批填得更快、落盘延迟更低；flush 逻辑只有一份。⚠️ 原文「fsync 是毫秒级」已被实测修正为 **256μs**，但结论不变（见文档内的"实测修正"节） |
| 008 | 为什么给 MessageQueue 加带超时的等待（**主动打破 core 零改动**） | [全文](docs/decisions/008-messagequeue-timed-dequeue.md)。`flush_interval` 需要「等到有数据、或等够 T」；阻塞版会死锁在等数据上、非阻塞版退化成轮询。三条判据（通用性/兼容加法/换实现仍在）全过 → 该加。**「不改 core」是度量不是目标 —— 三次换实现已证明抽象成立，指标完成使命就该退场** |
| 009 | 为什么 CacheWriter 第一版存原始报文而非结构化字段 | [全文](docs/decisions/009-cache-storage-shape.md)。`TickData` 是「原始报文容器」（topic + payload），**没有 symbol/price/volume**。要存结构化字段，就得让解析结果也流进管道 → `MessageQueue` 要承载领域类型 → core 得认识业务类型（违反决策 002）。**这是真实架构张力，第一版先在存储侧妥协**。附：不建索引（读少写多）、时间戳用 steady_clock（只限单次运行内） |

---

## 十一、与 AI Infra 的思想共性

两者都是高并发事件处理系统，在调度、队列、延迟优化等方面存在共性：

| FinceptPipeline | 思想共性 |
|-----------------|----------|
| **攒批（size-or-time 双触发）** | **vLLM 的 continuous batching** — 都是「不等凑齐、按策略触发」的批处理调度。**本项目与 AI Infra 最硬的一条连接** |
| AsyncReceiver 接收行情 | 类似 API Server 接收推理请求 — 网络 IO 与业务处理解耦 |
| MessageQueue 缓冲 | 类似 Scheduler 请求队列 — 削峰填谷 |
| Worker Loop 并发处理 | 类似 Worker 并行推理 — 固定线程数处理持续流 |
| Backpressure 背压 | 类似请求限流 / 排队 — 过载保护 |
| LatencyTracker 全链路埋点 | 类似推理延迟监控 (TTFT / TPOT) — 分阶段定位瓶颈 |

> ⚠️ **曾写过一行错的**：「Cache 批量写入 ↔ KV Cache，用空间换时间」。
> KV Cache 缓存的是**已算出的中间结果**（避免重复计算），
> 而攒批合并的是 **I/O 事务**（延迟换吞吐）—— 共同点只有 "cache" 这个词。
> 已删除，换成上面第一行那个真正的对应。

---

## 十二、简历描述

### 版本 A — 简历用（紧凑，4 条）

> **FinceptPipeline — 实时行情异步处理管道** ｜ 个人项目
> `C++20 · 多线程 · Qt 6 · WebSocket · SQLite · CMake`
>
> 基于 FinceptTerminal（C++20/Qt6 开源金融终端，2010 个源文件）二次开发，
> 设计并实现独立的异步数据处理管道，替换其原有「WebSocket → 主线程 → DataHub」链路。
>
> - **抽象经三个真实实现验证**：以两张窄接口隔离核心与实现，先后被 Mock / Qt 真实网络 /
>   Python 子进程三种实现替换，**核心层零改动** —— 依赖倒置在真实替换中印证
> - **在真实系统中定位并消除主线程瓶颈**：在 2010 文件的开源终端里定位到行情解析
>   全部发生在主线程（原作者以"每轮 50 行 / 4ms"预算兜底），改造后全部移出；
>   真实行情端到端跑通，**T0→T3 延迟 P50 = 92μs**
> - **性能工程：实测而非估算**：批量写入 SQLite 实测 **15.6× 加速**、定位 `batch_size`
>   拐点（1000）；进一步对照落盘策略，得出反直觉结论 ——
>   **攒批抹平了 NORMAL 与 FULL 的差异，即不必再为性能牺牲断电安全**
> - **工程判断与文档**：以三条判据论证一次核心接口扩展的合理性，保持既有 10 个测试
>   零修改通过；产出 **9 篇设计决策日志**（含反方论证与被实测推翻的假设）

### 版本 B — 作品集 / README 用（完整）

> **FinceptPipeline — 实时行情异步处理管道**
>
> 基于 FinceptTerminal（C++20/Qt6 开源金融终端，2010 个源文件、16 个交易所）
> 二次开发，设计并实现独立的异步数据处理管道，替换其原有
> 「WebSocket → 主线程 → DataHub」链路。
>
> **数据流**：`WebSocket → AsyncReceiver(T0) → MessageQueue → Workers(T2/T3) → CacheWriter(T4) → DataHub(T5)`
>
> **一、抽象经三个真实实现验证**
> 以两张窄接口（`IWebSocketClient` / `IPublisher`）隔离核心与实现。该抽象先后被
> **Mock / Qt 真实网络 / Python 子进程**三种实现替换，**核心层零改动**。
> 一个实现证明不了抽象是否成立 —— 三个可以。
>
> **二、在真实系统中定位并消除主线程瓶颈**
> 在 2010 个源文件的开源终端里定位到：行情报文的 JSON 解析、对象构造、缓存更新
> 全部发生在主线程（原作者以「每轮 50 行 / 4ms」时间预算兜底，代价是积压时数据滞后）。
> 改造后主线程只做一次字符串前缀判断 + 入队，解析/构造/发布全部移入 worker 池。
> **真实行情端到端跑通：T0→T3 P50 = 92μs，双交易所并发无丢弃、P99 平稳。**
>
> **三、性能工程：实测而非估算**
> - 批量写入 SQLite：**15.6× 加速**，`batch_size` 拐点在 **1000**（10000 反而变慢）
> - 对照三种落盘策略后发现：攒批把策略差异**抹平**了（0.034 / 0.034 / 0.031 秒）
>   —— 意味着 `WAL+NORMAL`（为性能放弃 fsync）不再必要，
>   可以改用 `WAL+FULL` 拿到断电安全而性能不变
> - **过程中两处估算被自己的实测推翻**：fsync 代价高估 4 倍、单条 INSERT 高估 20 倍
>
> **四、全链路可观测性**
> 构建横切式延迟追踪（T0~T4 时间戳 + P50/P95/P99 + 吞吐，埋在 worker 热路径上、
> 用取模采样避免测量影响被测对象）。该系统此前**没有任何单条消息级的延迟数据**，
> 接入后首次暴露真实分布 —— 而这正是发现"改造收益有限"的原因。
>
> **五、工程判断**
> - 以三条判据（通用性 / 兼容性 / 可移植性）论证一次**核心接口扩展**的合理性，
>   并做成兼容加法，使既有 10 个测试**零修改**通过
> - 撞上架构张力时选择**先妥协并记录**（存储形态无法两全），列出"何时该回来重看"
> - 产出 **9 篇设计决策日志**，其中包含反方论证、被推翻的假设、以及"指标完成使命就该退场"这类取舍
>
> **已知边界（诚实）**：M2 那次改造的性能收益是推算的（145 msg/s vs 临界 1,120+），
> 未做改动前后的同时段对照；测试覆盖尚未覆盖全部组件。

---

## 十三、参考资料

- [FinceptTerminal GitHub](https://github.com/Fincept-Corporation/FinceptTerminal)
- [FinceptTerminal Architecture](https://github.com/Fincept-Corporation/FinceptTerminal/blob/main/docs/ARCHITECTURE.md)

---

> 最后更新：2026-08-07
> 方案基于 Claude Code + Codex 联合讨论已定稿，不再发散。
> 下一阶段：进入 V0 代码结构设计。

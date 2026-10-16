# FinceptPipeline 学习日志

> 每个知识点记录在此，不依赖聊天记录。
> PLAN.md = 架构方案；LEARNING_LOG.md = 学习笔记。

---

## 教学模式（与 Claude 的约定）

**目标**：通过 FinceptPipeline 培养 C++ 工程能力 → 进入 AI Infra

**原则**：
1. Claude 不当代码生成器，当导师 + Code Reviewer + 面试官
2. 我不直接给完整代码，先讲设计思想，引导你自己实现
3. 每个知识点必须联系项目：不是为了学 mutex，是为了实现线程安全队列所以需要 mutex
4. 每天结束进行代码 review + 面试模拟

**每次学习流程**：
```
Step 1  Claude 解释：为什么需要这个技术、底层原理、简单例子
Step 2  你自己实现
Step 3  Claude review 代码：规范、设计、潜在 bug
Step 4  总结面试问题，记入本日志
```

**不直接问的问题**：
- ❌ "帮我写 MessageQueue"
- ✅ "告诉我 Queue 需要解决哪些并发问题？设计合理吗？"

---

## 2026-08-10 — 项目启动

### MessageQueue 是什么

**问题**：两个线程要共享数据，如果不加任何保护，会出现：
- 竞态条件（读到写了一半的脏数据）
- 速度不匹配（生产者快、消费者慢 → 数据丢失）

**解决**：线程安全的有界队列（MessageQueue），充当 Producer 和 Consumer 之间的"传送带"。

### 三个核心基础件

| 元件 | 作用 | 一句话 |
|------|------|--------|
| `std::mutex` | 互斥锁，同一时刻只允许一个线程访问共享数据 | 锁 |
| `std::unique_lock<std::mutex>` | mutex 的 RAII 包装，构造时加锁，析构时解锁 | 不会忘释放的锁 |
| `std::condition_variable` | 线程间通知：队列空了 Consumer 睡觉等，Producer 放入后叫醒 | 线程间的信号 |
| `std::atomic<bool>` | 原子布尔，做"关机信号"，所有线程都能及时看到 | 优雅关闭的开关 |

### 类接口（初版）

```cpp
template <typename T>
class MessageQueue {
public:
    explicit MessageQueue(size_t max_size);
    bool enqueue(T item);       // Producer 用
    T dequeue();                // Consumer 用（阻塞）
    bool try_dequeue(T& item);  // Consumer 用（非阻塞）
    void shutdown();
private:
    std::queue<T> queue_;
    size_t max_size_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;  // 队列空 → Consumer 等
    std::condition_variable not_full_;   // 队列满 → Producer 等
    std::atomic<bool> shutdown_{false};
};
```

### 类比记忆

```
Producer（快递员）
    │
    ▼
MessageQueue（传送带，有容量上限）
    │
    ▼
Consumer（分拣员）
```

---

---

## 2026-08-11 — MessageQueue 完成

### 实现总结

| 方法 | 逻辑 |
|------|------|
| `enqueue(T item)` | 满时阻塞 → shutdown 返回 false → push → 通知 Consumer |
| `T dequeue()` | 空时阻塞 → shutdown+empty 返回 T{} → pop → 通知 Producer |
| `bool dequeue(T& item)` | 重载版：返回 true=拿到数据, false=关机。**一次锁内完成 wait+判断+取数，零竞态** |
| `try_dequeue(T& item)` | 非阻塞：空则返回 false |
| `shutdown()` | 设 flag + notify_all 两个 CV |

### 关键技术点

1. **两个条件变量**：`not_empty_`（Consumer 等数据）、`not_full_`（Producer 等空间）
2. **shutdown 退出通道**：每个 wait 条件里都有 `|| shutdown_`，防止线程永远卡死
3. **TOCTOU 竞态**：先检查再操作 vs 先操作再检查，都有间隙。根治方案是 `bool dequeue(T&)` ——全部在一次锁内完成
4. **旧版 dequeue 返回 T{}** 作为哨兵的问题：T 是 int 时 0 也是合法数据，分不清。`bool dequeue(T&)` 解决了这个问题

### 构建流程

```
cd build → cmake .. -G Ninja → ninja → ./test_xxx
```

- cmake: 读 CMakeLists.txt 生成 build.ninja
- ninja: 增量编译，只重新编译改过的文件
- assert 失败会输出文件名+行号+条件，`zsh: abort` 是正常行为

### 面试自查

- Q: 为什么用两个条件变量而不是一个？
- A: 精准唤醒 vs 全体唤醒。一个 CV + notify_all 也能工作，但会唤醒不需要的线程（Producer 叫醒 Producer → 白等）
- Q: dequeue 返回 T{} 有什么问题？
- A: 对 int 来说 0 是合法值，无法区分"真实数据 0" 和"关机哨兵 0"。用 `bool dequeue(T&)` 根治
- Q: 什么是 TOCTOU？
- A: Time-of-check to time-of-use。检查状态和操作之间有时间差，状态可能已经变了

---

---

## 2026-08-12 ~ 08-14 — M0 完成

### ThreadPool 要点

- `start(worker_func)` 创建 N 个线程执行同一个 worker 函数
- `shutdown()` 用 `compare_exchange_strong` 防止双重关闭，join 后 `workers_.clear()` 支持复用
- `is_running()` = 线程数 > 0 且 stop_ 为 false

### MockDataGenerator 要点

- **`std::this_thread::sleep_until(next_time)` 而不是 `sleep_for`**：每个周期独立计算目标时间，不累积误差
- 速率控制公式：`interval = 1s / rate`（如 5000 msg/s → 200us 间隔）
- 三测试：完整生成（不丢不重）、速率验证（±容差）、提前 stop()

### 🔴 M0 最重要的 bug：pool.shutdown() 中断 Worker 清空队列

**症状**：determinism 测试间歇性失败，`collected.size() < N`。

**根因**：
```cpp
while (!pool.should_stop()) {   // Worker 检查线程池状态
    if (!queue.dequeue(msg)) break;
    ...
}
// 主线程: queue.shutdown() → pool.shutdown()
// 竞态: pool.shutdown() 设 stop_=true 时队列还没清空
//       Worker 回到循环头看到 should_stop → 提前退出 → 剩余数据丢失
```

**修复**：Worker 的退出条件只由队列决定：
```cpp
while (true) {
    if (!queue.dequeue(msg)) break;   // 队列关且空 → 自然退出
    ...
}
// 顺序保证: queue.shutdown() 先 → Worker 取完剩余 → 自己退出
//           pool.shutdown() 后 → 只是 join 收尾
```

**教训**：模块的停止信号必须和它的数据源的生命周期对齐。谁的数据，谁说了算。

### M0 完成状态

```
MockDataGenerator ──→ MessageQueue ──→ ThreadPool(Worker Loop) ──→ 收集
     20000 msg/s           容量 200          4 workers
```

6 个测试套件 12 个测试全部通过。

### 面试自查

- Q: 为什么用 sleep_until 不用 sleep_for？
- A: sleep_for(interval) 每次调度误差会累积，N 次后总时长偏大；sleep_until(绝对时间点) 每周期独立，不累积
- Q: 为什么 Worker 退出条件只看队列不看线程池？
- A: 数据生命周期由队列管理。若看线程池状态，shutdown 可能发生在队列未清空时，导致剩余数据丢失
- Q: compare_exchange_strong 在 shutdown 里有什么用？
- A: 保证 shutdown 幂等——多个线程同时调用 shutdown 只有一个会执行 join，避免重复 join 崩溃

---

## 2026-08-15 ~ 17 — M0 总结 + 面试模拟 5 题

### 贯穿 M0 的元教训：检查与操作之间必有间隙

同一个思想在四个层面的四次应用：

| 场景 | 间隙 | 解法 |
|------|------|------|
| Consumer 外部判断 + dequeue | TOCTOU 竞态 | 判断搬进锁内（`bool dequeue(T&)`） |
| ThreadPool 判断 + 置位 | `if` 与 `=` 之间 | `compare_exchange_strong`（CPU 一条指令） |
| Worker should_stop + 取数 | 提前退场丢数据 | 退出权交给数据源（只查 dequeue 返回值） |
| sleep_for 累加 | 误差累积 | sleep_until 绝对时间锚定 |

**多线程 bug 最好的修法不是把竞态窗口调小，而是把它拆掉。**

### 面试 5 题核心答案

**Q1: 为什么 Worker Loop 不用 submit/future？**
- submit/future 适合离散任务（Web 请求、图像处理）；Worker Loop 适合持续流（行情、日志）
- 行情每秒上千条，每条 submit 一次 = 海量 future + 调度开销
- MessageQueue 已经是任务队列，池内再建队列是重复建设
- 附加收益：有界队列天然背压（submit 模式的池内队列常无界）

**Q2: 为什么用两个条件变量？**
- 一个 CV + notify_all 也能工作——predicate 保证正确性，notify 只是性能优化
- 但全体唤醒 = 唤醒风暴（10000 msg/s 下锁竞争吃吞吐）；notify_one 无法定向，会叫醒不该醒的人 → 卡顿
- 两个 CV：notify 方向与条件满足方向一一对应，每次唤醒精准命中
- 金句：正确性只是底线，两个 CV 是用实现复杂度换"每次唤醒都推进"

**Q3: shutdown 顺序反了会怎样（白板题）？**
- 正确顺序：queue.shutdown()（唤醒+给退出信号）→ Worker 自然退场 → pool.shutdown() 收尸
- 反了 = 循环等待死锁：Worker 等 queue.shutdown() 才能退出，主线程卡在 join() 里永远执行不到 queue.shutdown()
- 队列有数据：Worker 取完后卡在 dequeue 睡觉 → 死锁（数据没丢但系统挂）
- 队列已空：立刻死锁
- 旧版 Worker（带 should_stop）：反了变成数据丢失而非死锁——两个错误各有各的难看

**Q4: should_stop 竞态 bug 因果链（最值钱的教训）**
- 错因：两个权威来源的退出信号放同一循环，且线程池的信号检查在前
- 窗口：Worker 回到循环头检查 与 队列被取空 之间；主线程两行 shutdown 紧挨着，条件极易满足
- 时过时不过：主线程置 stop_（几微秒）vs 4 Worker 清空队列（几十微秒），调度器掷骰子 → Heisenbug
- 修复后 stop_ 的角色：队列消费型 Worker 里降级为无用信号；仍服务无数据源的 Worker（test_thread_pool）
- 原则：**退出条件的权威来源跟着"活"走——数据流的活由数据源宣布干完，线程池只管收尸**

**Q5: Producer 10 倍于 Consumer 的过载（设计题）**
- 有界队列：快速填满 → 背压传导到 Producer → 全系统降到 Consumer 速率；延迟 = 容量/消化速率（有上界）；内存安全
- 三策略：Block（保完整性，交易委托）vs DropNewest（保最新任务，批处理）vs DropOldest（保时效，行情）
- 行情选 DropOldest：旧价格时间价值衰减快、快照可自愈、P99 有界
- 完整性、时效性、资源占用三者不可兼得——M1 用 benchmark 数据说话

### 其他要点

- `std::jthread`（C++20）自带 auto-join + stop_token，ThreadPool 手写是刻意学习；M0 后可做重构练习
- `stop()` 语义是尽力而为：生成线程卡在 enqueue 时不能立刻生效
- rate=0 会除零 → UB，需 assert 挡住

---

## 终端命令速查卡

目录结构：`FinceptPipeline/` 下有 `CMakeLists.txt`、`src/`、`tests/`、`build/`（构建产物）。所有构建命令在 `build/` 里执行。

| 命令 | 作用 | 何时用 |
|------|------|--------|
| `cd ~/projects/FinceptPipeline/build` | 进入构建目录 | 每次开终端第一件事 |
| `cmake .. -G Ninja` | 生成构建配置（configure） | **改了 CMakeLists.txt 后**（如新增测试 target） |
| `ninja` 或 `cmake --build .` | 增量编译 | 改了 .h/.cpp 后 |
| `./test_xxx` | 跑单个测试 | 快速验证 |
| `ctest` | 跑全部注册测试 | 收尾验证 |
| `ctest --output-on-failure` | 全测+失败详情 | 推荐 |
| `ls` | 查看可执行文件 | 忘了测试名时 |

典型工作流：

```bash
# 改源码后
ninja && ./test_xxx

# 新增测试文件后（三步缺一不可）
# 1) CMakeLists.txt 加 add_executable + add_test
# 2) cmake .. -G Ninja
# 3) ninja && ctest
```

报错对照：

| 报错 | 原因 |
|------|------|
| `cd: no such file or directory: build` | 不在项目根目录 |
| `No tests were found` | 在错误目录跑 ctest |
| `Usage: cmake --build <dir>` | 忘了 `.` |
| `no rule to make target` | 改了 CMakeLists 没 reconfigure |
| `Assertion failed: ... file ..., line ...` | 测试失败，看行号定位 |

---

---

## 2026-08-24 ~ 09-02 — M1 完成：背压策略 + 延迟追踪 + Benchmark

### MessageQueue 背压策略

- `BackpressurePolicy { Block, DropOldest, DropNewest }`，构造时定死
- 满时行为：Block 等空位；DropOldest 弹队首放新；DropNewest 拒绝返回 false
- `dropped_count()`：atomic 计数，供 benchmark 算丢包率
- **语义区分**：shutdown 拒绝 ≠ 背压丢弃，shutdown 后不计数
- **YAGNI 决策**：砍掉了 PLAN 里计划的 `try_enqueue`——与 DropNewest 行为等价（2/3 策略组合冗余），且无"调用点级非阻塞"需求

### LatencyTracker（横切模块）

- `TickLatency` 6 个时间戳（T0~T5），M1 只用前 4 个
- **PipelineItem 重构**：TickData + TickLatency 成对走队列，业务类型不被监控污染
- 采样率：取模确定性采样（counter % N == 0），未命中不碰锁
- 分位数：朴素 sort + index = n×p/100（每指标只排一次序）
- throughput 除零 guard：所有样本同一时刻 → 0

### Benchmark 核心发现（M1 最有价值的数据）

9 组数据（3 速率 × 3 策略），消费速率 4444 msg/s（4 Worker × 900us 处理）：

| 策略 @ 10000/s | dropped | wall | queue_wait P50 |
|----------------|---------|------|----------------|
| Block | 0 | 22.65s（生产被拖慢） | 226ms |
| DropOldest | 54879 | 10.23s | **100ms** |
| DropNewest | 54762 | 10.23s | 225ms |

**🔴 反直觉发现：DropNewest 的 queue_wait 并不最小，DropOldest 才是。**
- 直觉假设："拒绝新数据 → 队列不满 → 等待短" → ❌ 被数据推翻
- 真实机制：DropNewest 的队列**常驻满状态**，被接受的幸运儿要排在 1000 条旧消息后面 → 225ms 排队高原
- DropOldest 每次入队弹队首，新消息前面的"人头"以 取走+淘汰 双倍速度消失 → 等待减半
- **丢谁不重要，关键是队列里剩的是谁**。DropNewest 剩旧消息堵路；DropOldest 队列里总是新消息

### 面试自查

- Q: 为什么轻载下三种策略无差异？
- A: 背压策略只在队列满时生效。rate < 消费速率时队列永远不满，策略分支根本不触发。**策略选型只对饱和场景有意义**
- Q: benchmark 为什么用忙等不用 sleep？
- A: sleep 引入调度抖动噪声；忙等占住 CPU 时间可控。benchmark 要测的是系统本身，不是调度器的脾气
- Q: 为什么 N 要跟 rate 成正比？
- A: 固定 N 会让低速率组跑 50s。固定时长（rate×10s）让每组负载量级可比，也是业界标准做法

---

## 2026-09-14 ~ 09-16 — M2 第一步：适配层（mock 先行）

### 做了什么

**依赖倒置落地**：Core 只认两张"名片"——`IWebSocketClient`（数据从哪来）和 `IPublisher`（数据往哪去）。Mock 只是名片的实现之一，M2 真接入时换成 Qt 版实现，**Core 一行不改**。

整条链路第一次全部接通：

```
MockWebSocketClient ──→ AsyncReceiver ──→ MessageQueue ──→ Workers ──→ MockPublisher
  (收包线程发回调)      (盖 T0 入队)                    (盖 T2/T3 → record → publish)
```

| 文件 | 内容 |
|------|------|
| `src/adapters/MockWebSocketClient.h` | 迷你 Producer-Consumer：`push_message` → `pending_` → 收包线程发回调 |
| `src/adapters/AsyncReceiver.h` | raw 报文 → 盖 T0 → 组装 `PipelineItem` → enqueue |
| `src/adapters/IWebSocketClient.h` / `src/core/IPublisher.h` | 两张窄接口名片（定稿未改） |
| `tests/test_adapter_e2e.cpp` | MockPublisher + 1000 条端到端 + 负向用例 |
| `CMakeLists.txt` | `AdapterE2E` target |

**10 个测试全绿。**

---

### 🔴 1. TOCTOU —— atomic 挡不住"检查 + 行动"

`push_message` 原本是「先检查 `accepting_`，再加锁 push」。`accepting_` 是 `atomic<bool>`，看着安全——**但 atomic 保证单个读写的原子性，不保证一串操作的原子性。**

窗口里的后果不是"多一条消息"这么轻，是**静默丢数据**：

```
t1  检查通过（accepting_ 还是 true）
t2  disconnect 把 accepting_ 设 false
t3  stopping_ = true
t4  notify → run() 醒来 → stopping_ 且 pending_ 空 → break，收包线程结束
t5  join() 返回
t6  push 才执行 → 消息进了 pending_
t7  return true              ← 谎报成功
```

消息躺在一个**已经没有消费者**的队列里，调用方拿到 `true`，毫不知情。

**修法两半缺一不可**：① 检查挪进临界区 ② **改 `accepting_` 的一方也持同一把锁**。
只做 ①，`disconnect()` 仍能在锁外写 `accepting_`——窗口变窄，没有消失。

> 对称性是关键：光保护"读"没用，得保护"读-判断-写"整段，**判断依据的状态也必须由同一把锁守护**。

### 🔴 2. 修 TOCTOU 时引入了双重加锁

加了外层 `lock_guard` 修 TOCTOU，**忘了删内层那个** → 同一线程对非递归 `std::mutex` 重复加锁 = 死锁 → 测试**无声挂死**，不报错不退出。

> **教训：修并发 bug 时"修一个引入一个"是常态**，因为动手的正是你最没把握的那段代码。改完要专门回扫一遍。

### 3. 回调必须在锁外调用

两个层次，严重程度不同：

- **硬死锁（回调重入）**：`run()` 持 `mutex_` → 调回调 → 回调里调 `push_message()`（"收到消息回一条"，完全合理）→ 同线程重入非递归锁 → 收包线程**在等自己释放锁，永远等不到**
- **活性问题（回调阻塞）**：回调里 `queue_.enqueue()` 在队列满时阻塞 → 锁被长期持有 → 主线程 `push_message` 和 `disconnect()` 全卡住。这条**能解开**，但响应性被回调绑架

**根因**：锁应该保护数据结构，不该保护"执行一段你不拥有的代码"。

**做法**：锁的作用域收窄到只包住"从 `pending_` 取出一条"，让锁在调回调之前自然释放。

### 4. 关闭顺序：上游先停 + 排空，再关下游入口

`AsyncReceiver::disconnect()` 必须是：

```
1. client_->disconnect()   ← 排空 pending_ + join 收包线程（写入全部结束）
2. queue_.shutdown()       ← 现在才能宣布终态
```

反过来写 → 收包线程还在派发 → 每条都撞上 `shutdown_ == true` → 数据丢掉。计数器能让你**看见**丢了多少，但救不回来。

同 M1「generator 放完 → queue.shutdown()」一课。

### 5. shutdown 后不能重试

`enqueue` 返回 `false` 的分支**不是"队列满"，是 `shutdown_ == true`**。队列满时 `not_full_.wait` 会阻塞等待然后继续 push，返回 `true`。

**shutdown 是终态，没有 reopen。** 所以"重试" = 收包线程卡在永不成功也永不超时的循环里 → `disconnect()` 的 `join()` 跟着一起死。

> **shutdown 之后只有两条路：丢掉，或者计入统计。没有第三条。**

### 6. `rejected_` 和 `dropped_` 必须分开计数

| | `dropped_`（MessageQueue） | `rejected_`（AsyncReceiver） |
|---|---|---|
| 含义 | **背压淘汰**：队列满，按策略主动丢 | **边界拒收**：管道正在关闭，数据没进去 |
| 触发 | 系统**过载**（还能继续跑） | 系统**正在停止**（终态） |
| 可恢复 | 可恢复——消费快一点就不丢 | 不可恢复——关了就是关了 |
| 该做什么 | 扩容 / 调策略 / 加消费者 | **查关闭时序是不是写错了** |

合并成一个数，你只知道"有数据没了"，**不知道该修哪儿**。这是 M1「shutdown 拒绝 ≠ 背压丢弃」在接收端的再次应用。

### 7. 语言/工具细节

- **header-only 里声明了析构就必须定义**，否则链接错误；且隐式析构遇到 joinable 的 `std::thread` 会 `std::terminate()`（进程当场死、无栈回溯、析构不执行）
- **`++volatile_int` 在 C++20 被弃用**（读-改-写对 volatile 几乎从来不是你要的），简单赋值 `x = x + 1;` 仍合法
- **缺 `#include <set>` 炸出 7 个 error**：编译器猜你想用 `std::get`，跑去实例化 variant 模板一路余震。**看编译错误只看第一条**
- **`cmake` ≠ 编译**：`cmake .. -G Ninja` 只画图纸（生成 `build.ninja`），`ninja` 才施工。漏掉 `ninja` 直接 `ctest` = 跑旧二进制 = **假绿**（比构建失败危险，因为它不报错）

---

### 工程原则（可复用）

1. **atomic 让每个动作安全，但不让动作的组合安全。** 检查-然后-行动必须整体加锁，且改状态方用同一把锁
2. **测试里的"等一等"不要用 `sleep`，要用真实的同步点。** `disconnect()` 的 `join()` 就是同步点；`sleep(100ms)` 是对调度器的赌博。flaky 测试的根源基本都是"我猜它应该处理完了"
3. **契约 + 观测互补**：文档写清"只有 X 能关队列"是契约；计数器是机制。你无法阻止别人误用，但可以让误用**留下痕迹**
4. **只交出对方需要的东西。** `publish` 只收 `TickData` 不收 `PipelineItem`——每多暴露一个字段，就多一份"别人依赖它"的风险
5. **负向用例也要写。** 正常路径的测试不会覆盖"关闭中"这类边界，而那正是幽灵 bug 的产地

---

### 面试自查

- **Q: `accepting_` 是 atomic，为什么"先检查再加锁 push"仍有竞态？**
- A: atomic 只保证单次读写原子。`if(!accepting_)` 和 `push` 是两个操作，中间有窗口——disconnect 可在窗口里关门并 join 掉收包线程，导致消息进了一个没有消费者的队列，而函数返回 `true`。**atomic 让每个动作安全，但不让动作的组合安全。**

- **Q: 只把检查挪进临界区，为什么不够？**
- A: 另一半是"改状态的一方也要持同一把锁"。否则 A 持锁期间 B 仍能在锁外写 `accepting_`，窗口只是变窄。**保护"读-判断-写"整段，判断依据的状态也必须由同一把锁守护。**

- **Q: 回调为什么必须在锁外？给一个具体死锁场景。**
- A: 硬死锁：`run()` 持 `mutex_` → 调回调 → 回调调 `push_message()` → 同线程重入非递归 mutex → **收包线程在等自己释放锁**。另一层是活性问题：回调阻塞（如 `enqueue` 遇队列满）会长期持锁，拖住 `push_message` 和 `disconnect`。根因：锁保护数据结构，不保护执行外部代码。

- **Q: `disconnect()` 为什么先 `client_->disconnect()` 再 `queue_.shutdown()`？**
- A: `on_raw_message` 只在收包线程执行、是写队列的一方。必须先确保写入全部结束（排空 + join），才能宣布队列终态。反了的话剩余消息全撞 shutdown 被丢掉。**关闭管道 = 上游先停+排空，再关下游入口。**

- **Q: shutdown 后 `enqueue` 返回 false，为什么不能重试？**
- A: 能走到 `return false` 的只有 shutdown（队列满会 `not_full_.wait` 阻塞后继续 push）。**shutdown 是终态、无 reopen**，重试 = 永不成功也永不超时的死循环 → `join()` 跟着卡死。只能丢掉或计数。

- **Q: `rejected_` 和 `dropped_` 为什么要分开？**
- A: 语义不同、**应对动作不同**。`dropped_` = 过载淘汰（可恢复 → 扩容/调策略）；`rejected_` = 关闭中拒收（不可恢复 → 查关闭时序 bug）。合并后只知"有数据没了"，不知该修哪儿。

- **Q: `publish` 为什么只传 `TickData` 不传 `PipelineItem`？**
- A: `PipelineItem` 含 `TickLatency`（T0~T5 管道内部记账）。传出去 = 监控字段变隐式 API，早晚被下游依赖，以后改 T0 语义要说服所有人。**窄接口不只体现在方法数量，更体现在参数里装了什么。**

---

## 2026-09-17 ~ 09-19 — M2 第二步：接真实数据源（Kraken + Qt）

### 做了什么

给管道换上**第二个实现**，验证"名片"设计：

```
MockWebSocketClient   →   KrakenWebSocketClient
（自己造的假数据）         （真 Qt、真网络、真线程模型）
```

| 文件 | 说明 |
|------|------|
| `src/adapters/KrakenWebSocketClient.h` | `IWebSocketClient` 的 Qt 实现 |
| `tests/test_kraken_live.cpp` | 真实行情联调（手动跑，不进 ctest） |
| `tests/probe_qt_ws.cpp` | 诊断探针（排障用） |

**🔴 验收结果：`src/core/` 零改动** —— 换一个完全不同的实现，核心一行没动，依赖倒置成立。

### Qt 三条规则（决定回调在哪个线程执行）

| 规则 | 含义 | 已有对应物 |
|------|------|-----------|
| 事件循环 | Qt 靠一个循环分发所有事件 | Mock 的 `run()` 里的 `while(true)` |
| **线程亲和性** | QObject 属于**创建它的那个线程**，只能在那碰 | ⚠️ **无——唯一全新的概念** |
| 连接类型 | 跨线程信号自动打包成事件排队 | `push_message` + `pending_` |

**推论**：在子线程 `new QWebSocket` + 那个线程跑 `exec()` → 回调在子线程执行。
契约「回调在收包线程上执行」由此**自动成立**，不是你手动实现的 —— 你做的是"配置好条件让它成立"。

### 🔴 最值钱的坑：子线程的事件派发器是 exec() 时才创建的

```
socket.open() 写在 loop.exec() 前面
→ 子线程还没有事件派发器 → socket 的 notifier 注册不上去
→ TCP 连接永远不推进
```

**症状：不报错、不崩溃、不超时**，程序看着一切正常，就是收不到数据。**极易误判成网络/VPN 问题。**

主线程**没有**这个问题 —— `QCoreApplication` 构造时就把主线程的派发器建好了。**这是子线程独有的坑。**

修法：`QTimer::singleShot(0, &socket, [...]{ socket.open(...); })` —— 投进事件队列，循环一跑起来就执行。

### 排空语义的 Qt 实现

| | Mock | Qt |
|---|---|---|
| 待处理数据在哪 | 自己的 `pending_` 队列 | 事件循环的任务队列 |
| 怎么保证排空 | 退出条件 = `stopping_ && pending_.empty()` | 把"退出"投到**队尾** |

```cpp
QMetaObject::invokeMethod(loop_ptr_, [lp]{ lp->quit(); }, Qt::QueuedConnection);
```

**`QueuedConnection` = 把调用打包成事件投到目标线程事件队列的队尾。** 队列 FIFO，轮到它时排前面的都跑完了。

直接 `lp->quit()` 是"立刻叫停"，队列里排着的事件全丢。
> **不是"等一下"，是"排我前面的先跑完"。**

### 悬垂指针防护

`loop_ptr_` 指向 `run()` 里的栈变量。`run()` 返回它就没了。

```cpp
{ lock; loop_ptr_ = &loop; }
loop.exec();
{ lock; loop_ptr_ = nullptr; }   // ← 必须在 loop 析构【之前】
```

加锁保证两种交错都安全：主线程要么读到 `nullptr`（什么都不做），要么读到的一定**还活着**（子线程要置空也得先拿这把锁）。

### 方法论：探针法（这次最该带走的）

主程序不工作时，写个最小程序去**证伪**：

```
curl 通                 → 网络没问题
TLS 预检：openssl 可用   → Qt 的 TLS 没问题
探针（主线程）通         → Qt + QWebSocket 没问题
        ↓ 排除法
问题在【子线程模型】→ 锁定根因
```

**没有探针的话**，那个"不报错、干等"的症状会让人一直怀疑网络和 VPN —— **方向完全错了**。

比"加打印"强在：打印是在**有问题的系统里观察**，探针是**换一个干净系统做对比**。

### 工程细节

- **前向声明只能用于指针/引用。** `QWebSocket socket;` 是栈上对象，需要完整定义 → 必须 `#include`
- **header-only 项目里声明了就必须定义**（老坑重演：`disconnect()` / `set_on_message()` / 析构）
- **cpptools 不自动读 `compile_commands.json`**（clangd 才会），必须在 `.vscode/c_cpp_properties.json` 显式配 `compileCommands`；工作区根目录 ≠ 项目目录时路径要算对
- **联网测试不注册为 ctest** —— 依赖 VPN、跑得慢、结果天然不确定（同 `pipeline_bench` 的理由）
- brew 的 Qt 不在 CMake 默认搜索路径，要给 `CMAKE_PREFIX_PATH` 提示

### 验收标准的写法

写「**`src/core/` 零改动**」，不写笼统的「Core 不改」。

因为 `AsyncReceiver` 在 `src/adapters/` —— 契约演进（比如将来加 `on_disconnect`）**只影响适配层，不渗进核心**，这才是"名片设计对了"的证据。标准写笼统了就测不出东西。

同理：`PLAN.md` 里「真实 **Kraken** 行情」也是把实现细节写进了验收标准 —— 数据源应该可替换。

### 面试自查

- **Q: `socket.open()` 为什么不能写在 `loop.exec()` 前面？**
- A: 子线程的事件派发器是 `exec()` 时才创建的。提前 `open()`，socket 的 notifier 注册不到派发器上 → 连接永远不推进。症状是**不报错、不崩溃、不超时，就是干等**。主线程无此问题（`QCoreApplication` 构造时就建好派发器）。

- **Q: `invokeMethod` + `QueuedConnection` 为什么能实现排空？**
- A: `QueuedConnection` 把调用打包成事件投到目标线程事件队列的**队尾**；队列 FIFO，轮到它时前面的都跑完了。直接 `quit()` 是立刻叫停，排队的事件全丢。**不是"等一下"，是"排我前面的先跑完"。**

- **Q: `loop_ptr_` 为什么要加锁 + 在析构前置空？**
- A: `loop` 是栈变量，`run()` 返回即销毁。不加锁，主线程可能读到指针后、使用前它已失效 → 野指针。加锁后两种交错都安全：要么读到 `nullptr`，要么读到的一定还活着。

- **Q: 为什么验收标准是「`src/core/` 零改动」而不是「Core 不改」？**
- A: `AsyncReceiver` 在 `src/adapters/`，不在 core。契约演进只影响适配层 —— **这正是依赖倒置成立的证据**。标准写笼统就测不出东西。

- **Q: 探针法什么时候用？比"加打印"强在哪？**
- A: 主程序不工作、不知哪一层坏时用。它是**排除法**：探针通 → 问题在你的代码；不通 → 在环境/库。强在"换一个干净系统做对比"，而打印是在有问题的系统里观察。

- **Q: 这个阶段所有坑的共同主题是什么？**
- A: **时序。** TOCTOU（检查与使用之间的窗口）、回调在锁外（谁在持锁）、排空语义（什么时候算结束）、`open` vs `exec`（派发器何时存在）、指针置空（何时失效）—— **并发代码的正确性往往不在于"做了什么"，而在于"什么时候做"和"谁来做"。**

### 遗留（待办）

- **`IWebSocketClient` 要不要加 `on_disconnect`？** 真实场景里连接会无声无息断掉（VPN 掉线、被踢、网络抖动），而 `AsyncReceiver` 还在傻等。加的话影响面全在适配层内，`src/core/` 仍不需要动。
- **真实的"排空"只能尽力而为。** Mock 里 `pending_` 看得见数得清；真实 socket 的数据可能还躺在 TCP 接收缓冲区里、**根本没变成事件**，`quit` 之前的任务排不到它前面。契约里"断开时已收到的消息会派发完"这句话，在真实实现上做不到 100%。

---

## 2026-09-19 — M2 收尾：真实数据流过完整管道（+ T1 事故）

### 做了什么

把 `KrakenWebSocketClient` 接进完整管道，端到端跑通真实行情：

```
Kraken ──→ AsyncReceiver ──→ MessageQueue ──→ Workers ──→ Publisher
(真实网络)  (盖 T0 入队)      (传送带)       (盖 T2/T3)   (收集)
```

`test_kraken_live.cpp` 从"直连打印"升级成完整管道测试。
结构和 `test_adapter_e2e.cpp` **一字未改**，只换了数据源 —— 这是"名片"设计的最终验证。

**结果**：30 条真实报文一条不丢，T0→T3 P50 = **31μs**。

### 🔴 T1 事故：一个"断言全绿但代码是错的"标本

**现象**：跑通后打印统计，`排队等待 P50 = 3.73705e+12 us`（≈ 43 天）。

**根因**：`AsyncReceiver::on_raw_message` 设了 T0 / payload / id / topic，**唯独漏了 T1**。
`steady_clock::time_point` 默认构造 = epoch(0)，于是 `queue_wait = T2 - T1 = 开机至今`。

**为什么没被发现** —— Mock 的 E2E 断言了这些，全绿：

```cpp
assert(received_at <= dequeued_at);   // T0 ≤ T2  ← 正好跳过 T1
assert(dequeued_at <= processed_at);  // T2 ≤ T3
assert(total_processed == N);         // 只是个计数
```

**`queue_wait` 这个统计量从来没被看过一眼。**

**⚠️ 关键纠正：不是"接真实数据源才发现的"。**
`test_adapter_e2e.cpp` 走的是**同一个** `AsyncReceiver::on_raw_message` —— **Mock 路径当时一样烂**。
真正抓到它的是 **"把完整统计打出来"** 这个动作。

### 修复

1. `AsyncReceiver::on_raw_message` 补 T1，位置在 `enqueue` **之前**
   —— `enqueue` 队列满时会阻塞（Block 策略），盖在前面 `queue_wait` 才包含被背压卡住的时间，那正是收包线程真实经历的
   （另一处可放 `enqueue` 内部，但 `MessageQueue` 是泛型 `template<typename T>`，不该知道 `TickLatency` 的存在 —— **在正确的层做正确的事**）
2. `test_adapter_e2e.cpp` 断言链补全：`T0 ≤ T1 ≤ T2 ≤ T3`

### 验证了断言真的有用

**没失败过的断言，不知道它有没有用。**
故意注释掉 T1 那行 → `Assertion failed: (received_at <= enqueued_at), line 134` → 恢复。

### 数字背后的故事

```
端到端 (T0→T3)  : 31 / 72 / 79 us
排队等待 (T1→T2) : 31 / 72 / 79 us     ← 几乎一样
```

拆开：`T3 - T0 = (T1-T0) + (T2-T1) + (T3-T2) = ≈0 + 31us + ≈0`

那 31μs **不是"队列堵了"** —— Worker 全在 `cv_.wait` 上睡觉，队里没有积压。
**它是线程唤醒延迟**：`enqueue` → `notify` → 操作系统唤醒 Worker 线程 → 才 dequeue。

> **这条管道自身几乎零开销；延迟主要是操作系统的调度成本。**
> （如果加了真实处理耗时，`T2→T3` 就会盖过它。）

### 可迁移的三条

1. **测试没覆盖到的地方 = 没测。** 断言写了三条不算完整，得问"我漏了什么"
2. **断言验终值，打印验过程。** 两者都要有
3. **没失败过的断言，不知道它有没有用。** 人为破坏一次，确认它真会响

### 面试自查

- **Q: 为什么"断言全绿"不等于"代码是对的"？**
- A: 断言只覆盖你**想到要查**的维度。T1 从来没被断言过，所以它一直是 epoch(0) 也没人发现。**覆盖不到的维度等于没测。**

- **Q: 换个数据源能发现代码里的 bug 吗？**
- A: 不能。Mock 和 Kraken 走的是**同一段** `AsyncReceiver::on_raw_message`，两条路径一样错。发现 bug 的是"打印中间量"这个动作，不是数据源的更换。

- **Q: 这条管道 31μs 的延迟花在哪？**
- A: 不在处理（T2→T3≈0），也不在排队拥堵（Worker 处于阻塞等待，无积压）。**花在 `enqueue` 的 notify 到 Worker 被操作系统唤醒之间** —— 是线程调度成本，不是管道自身的开销。

### 新增的待办

- **`on_disconnect` 的决策还没做**（见上一节的遗留），真实连接会无声断掉
- **`test_adapter_e2e` 的排空语义验证仍依赖时序**（非确定性设计）
- **测试用 `assert`，Release 构建下全部失效** —— 这次的 T1 bug 在 Release 下依然抓不到

---

## 2026-09-19 ~ 09-22 — M2 第三步：接进 FinceptTerminal（真实产品代码）

### 做了什么

把 FinceptPipeline 接进一个 **2010 个源文件、16 个交易所、真 Qt GUI 的第三方开源项目**，并改造了它的行情数据路径。

| 步骤 | 内容 |
|------|------|
| 编译 | FinceptTerminal（Qt 6.8.3 独立装在它自己的 `.qt/`，与 brew 的 6.11.2 隔离） |
| 构建集成 | 方案 C：`add_subdirectory`，暴露 `pipeline_core` / `fincept_pipeline` 两个 INTERFACE target |
| 新增 | `src/trading/pipeline/PipelineBridge.{h,cpp}` —— **复用 MessageQueue / ThreadPool / LatencyTracker** |
| 改造 | `ExchangeSession::handle_ws_line` 一拆为三 |

**结果**：Kraken 真实行情（K 线 / 盘口 / 自选列表）稳定流过新管道，日志零 ERROR。

### 改造前后的数据路径

```
之前：ws_stream.py ─QProcess 信号→ drain_ws_buffer() ─→ handle_ws_line()
                   (主线程)         (主线程 4ms/50行预算)  (主线程：解析 JSON +
                                                            构造 TickerData +
                                                            写缓存 + 发布)

之后：ws_stream.py ─→ handle_ws_line()   ← 主线程：一次前缀比较 + 入队，立刻返回
                         │
                         ↓
                   MessageQueue ─→ Workers ─→ 解析 + 构造 + 发布
```

**作者自己已经承认这条链路是瓶颈** —— `drain_ws_buffer` 里写着 `MAX_LINES_PER_CYCLE = 50` / `MAX_MS_PER_CYCLE = 4`，超了就 `singleShot(1)` 延后。代价是积压时数据滞后。

### 插入点：A vs B

Fincept 现有链路：`WebSocketClient → ExchangeSession(解析) → ExchangeSessionManager → DataHub`

| | 插入点 | 结论 |
|---|---|---|
| **A** | WebSocket 层（原始报文） | ✅ 选它 |
| B | SessionPublisher 层（已解析的类型） | ❌ 解析没被解耦 = pipeline 退化成"绕远路的转发" |

**选 A 的理由不是"更像 PLAN"，而是 B 的话整个 M2 建的东西只把一次函数调用推迟了几微秒。**

### 🔴 卡了两天的不是我们的代码：首次运行竞态

**症状**：CRYPTO 页面永远"离线"，价格全是 `--`。

**排查链**（每步排除一个变量）：

```
① 手动跑 ws_stream.py（stdin 保持打开）→ 数据正常   → 脚本没问题
② 换符号 XBT/USD · BTC/USD · BTC/USDT  → 都能推     → 不是符号问题
③ 检查 venv → ccxt 4.5.64 在                        → Python 环境没问题
④ 查应用日志 → "System Python found: python3"       → 用的竟是系统 python3！
⑤ 复现：系统 python3 跑它 → "ccxt.pro not available" ← 根因
```

**根因**：应用启动时（18:35）venv 还没建好（18:36~18:44 才建完）。`PythonRunner` 检测**一次**就把"用系统 python3"这个决定永久缓存了，之后再也不重试。所有 Python 子进程（WS / daemon）全部失败。

**修复：完全退出应用重新打开。** 就这么简单。

> **教训：「只检测一次」的初始化逻辑，遇到「被检测的东西稍后才就绪」就是必死竞态。**
> 和 M2 收尾那次的 T1 bug 是同一种病 —— **检查发生得太早，而且从不复查。**

### 六个设计决策

1. **分流判据用廉价前缀比较**（`line.startsWith("{\"type\":\"status\"")`），不做完整解析 —— 解析正是要挪走的成本。可信度来自 `ws_stream.py` 的序列化契约（紧凑无空格、`type` 首字段，核对过源码）
2. **控制消息（status / symbol_map）留主线程** —— 它们改会话状态且必须**保序**，而 worker 池是多线程不保序的。否则会出现"ticker 用着旧的符号表"
3. **`DropOldest` 而不是默认的 `Block`** —— `enqueue()` 在主线程的 WS 热路径上被调用，阻塞它 = 把 UI 卡回去 = 改造白做
4. **`bridge_` 声明在最后一个成员位置** —— 析构逆序，它最先析构 → 先 join 掉 worker → 之后才轮到 `mutex_` / `price_cache_`
5. **不实现"队列积压"读数** —— `MessageQueue::size()` 是无锁读，和并发写构成数据竞争（UB）。诊断读数不值得引入 UB
6. **worker 数 4 → 2** —— 16 个交易所 × 4 = 最坏 64 个线程

### 面试自查

- **Q: 为什么插入点选 WebSocket 层，而不是已经解析好的那一层？**
- A: 已解析的那层意味着解析仍在原线程做，pipeline 只把"发布"这一步挪后 —— 收益趋近于零。插在原始报文层，解析/构造/写缓存才真正离开主线程。

- **Q: 为什么控制消息不能进 worker 池？**
- A: 它们改会话状态，且 `symbol_map` 必须先于依赖它的 ticker 生效。worker 池是多线程、不保序的，混进去会导致"用旧符号表解析新数据"。**这是"哪些能异步、哪些不能"的分界线。**

- **Q: 为什么背压策略要换成 DropOldest？**
- A: `enqueue()` 在主线程的 WS 热路径上被调用。`Block` 在队列满时会阻塞它 —— 等于把主线程又卡回去，改造的意义就没了。实时行情宁可丢最旧的。

- **Q: "只检测一次"的初始化逻辑，风险是什么？**
- A: 如果被检测的东西稍后才就绪（这次 venv 比解释器检测晚 9 分钟建好），就会永远锁死在错误的回退分支上。**检查发生得太早、且从不复查** —— 和 T1 bug（检查漏了一个字段）是同一类错误。

- **Q: 这次排查为什么又用上了探针法？**
- A: 症状只是"界面没数据"，可能的原因有：网络 / VPN / 符号 / Python 环境 / 应用自身 / 我们的改动。**手动跑一次 `ws_stream.py` 就排除了前面四项。** 把最底层的组件单独拎出来跑，比在完整系统里猜快一个数量级。

### 🔴 性能实测：结论是「改造是对的，但这个场景用不上」

把统计接进日志后（每 5 秒一条），用真实行情做了负载实验。

**实测数据：**

| 交易所 | 速率 | 丢弃 | T0→T3 P50 | P99 |
|---|---|---|---|---|
| Kraken（5/25 个符号有效） | ~50 条/秒 | 0 | 110μs | 646μs（平稳） |
| OKX（符号全有效） | **~95 条/秒** | 0 | 92μs | 507μs（平稳） |
| **两个并发合计** | **~145 条/秒** | **0** | — | — |

**推算：**

```
单条处理成本（= 改造前主线程每条的开销）：  ≈ 89μs
主线程在 145 条/秒 下的开销：              145 × 89μs ≈ 1.3% 的一个核

要到 10% 占用 → 需要 ~1,120 条/秒
要到明显卡顿 → 需要 ~5,600 条/秒（交叉验证：原设计 4ms/45条 的预算 ⇒ 11,250 条/秒）

我们实测：145 条/秒。  差 8~40 倍。
```

**为什么够不到：** 自选列表硬编码 25 个符号；Kraken 只认其中 5 个；
最吵的 Binance **对本地区返回 HTTP 451 地理封锁**。

> **结论：优化本身是对的（单条主线程开销降了约 40 倍），但绝对值本来就微不足道。**
> 要让这个优化值回票价，需要 1000+ 符号同时推送，或一个 10 倍更吵的数据源。

**最有价值的产出其实是可观测性**：这个跑了 2.5 天的应用，之前**完全不知道**自己每条行情消息的端到端延迟。现在知道了 —— **92~110 微秒**。

### 本阶段尚未验证的

- **改动前后的直接对比**（A/B）没做：我们是靠"实测单条成本 × 实测速率"推算的，不是同一时段的对照
- `pipeline_stats_text()` 只进了日志，**没接到界面**
- `drain_ws_buffer` 的 4ms/50行预算**保留了**（安全网）—— 改造后它应该很少触发，但没有实测
- **多 session 并存已验证**：Kraken + OKX 同时跑，两条桥各自独立、都无丢弃、P99 平稳

### 教训（这次最该带走的一条）

> **优化之前先测量。否则你很可能在优化一个不重要的问题。**

我们花了两天做出一个**正确的、优雅的、但当前收益接近零**的改造。

**而这不是失败** —— 真正的失败是**做完之后不去测量，然后一直以为它快了 40 倍**。
现在我们诚实地知道：单条消息的主线程开销降了约 40 倍，但绝对值本来就微不足道。

这和 M1 那次 benchmark 的发现（"DropNewest 等待并不最短"）是同一个主题：
**直觉经常是错的，数字不会。**

---

## 2026-09-23 ~ 09-24 — M3：Cache 批量写入

### 做了什么

`CacheWriter` —— 把处理完的数据批量写进 SQLite。**独立线程 + 自己的队列**（决策 007），队列直接复用 `MessageQueue`。

顺带**主动打破了「`src/core/` 零改动」**：给 `MessageQueue` 加了 `dequeue_for`（带超时的等待），
因为 `flush_interval` 需要「等到有数据、或等够 T」。决策 008 记录了为什么这个破例是合理的。

### 🔴 性能实测（M3 的核心产出）

**实验设计**：同一个数据集（2 万条），每轮只改**一个**变量。

#### ① batch_size 扫描（固定 WAL + NORMAL）

| batch_size | 总耗时(s) | COMMIT 数 | 吞吐(条/秒) | 每次 COMMIT |
|---|---|---|---|---|
| 1 | 0.532 | 20000 | 37,605 | 26.6 μs |
| 10 | 0.085 | 2000 | 235,925 | 42.4 μs |
| 100 | 0.039 | 200 | 518,212 | 193.0 μs |
| **1000** | **0.034** | 20 | **581,745** | 1719.0 μs |
| 10000 | 0.042 | 2 | 473,903 | 21101.4 μs |

- 攒批有效：**15.6 倍**
- **拐点在 1000** —— 10000 反而变慢（批的 vector 扩容 + 单次事务的 WAL 增长）

#### ② 落盘策略扫描（batch_size ∈ {1, 1000}）

| 配置 | 每次 COMMIT | 单条写总耗时 | 攒批(1000) | **攒批加速比** |
|---|---|---|---|---|
| WAL + NORMAL | 27.8 μs | 0.555 s | 0.034 s | 16.4 × |
| WAL + FULL | 58.3 μs | 1.166 s | 0.034 s | 34.5 × |
| DELETE + FULL | 256.4 μs | 5.128 s | 0.031 s | **165.3 ×** |

### 🎯 三个结论

**① 攒批的收益 ≈ 「每次 COMMIT 的固定开销 × (N−1)」**
落盘策略越贵，攒批收益越大：16× → 34× → 165×。

**② 但攒批之后，三种策略的绝对性能被抹平了**（0.034 / 0.034 / 0.031 秒）
因为 COMMIT 次数从 20000 降到了 20 —— **单价差 9 倍，但只付 20 次**，差异淹没在别处。

**③ 于是「持久性 vs 性能」的取舍消失了** ← **本次最有价值的发现**

`WAL + NORMAL` 本来是为了性能**放弃 fsync**（代价：断电可能丢最近几条提交）。
现在可以放心换成 `WAL + FULL`（每次提交都 fsync、断电不丢）——
**性能完全一样，持久性是白拿的。**

### 🔴 修正一个估错的数字

007 号决策里写：「fsync 是毫秒级，所以攒批能差 1000 倍」。

| | 原来估的 | 实测 |
|---|---|---|
| 单次 fsync 代价 | 毫秒级 | **256 μs** |
| 攒批收益 | 10 ~ 1000 倍 | **16 ~ 165 倍** |

**方向对，量级估高了 4~6 倍。** 现代 NVMe 比想象中快得多。
（PLAN 决策 004 里「单条 SQLite INSERT 约 0.5-2ms」是同一类高估。）

> **教训：性能推理可以指导方向，但数字必须测。**
> 这已经是第三次了（M1 的 DropNewest、M2 第三步的负载、这里的 fsync）。

### 排空语义 —— 第三次遇到

| # | 场景 | "排空"的含义 |
|---|---|---|
| 1 | `MessageQueue::shutdown` | 退出条件 = 停止信号 **且** 队列已空 |
| 2 | `KrakenWebSocketClient::disconnect` | 把"退出"投到事件队列**队尾** |
| 3 | **`CacheWriter::shutdown`** | **flush 未满的最后一批** |

**同一个思想，三个场景、三套机制。** 这次的具体坑是「循环退出后那一批」——
`run()` 末尾加一行 `if (!batch_.empty()) write_batch();` 就解决了，但**不写就静默丢数据**。

### 面试自查

- **Q: 攒批为什么快？快在哪？**
- A: 消除的是**每次事务的固定开销**（实测 27.8 μs ~ 256.4 μs，取决于落盘策略），不是"写数据"本身。COMMIT 次数从 N 降到 N/batch_size，这个固定开销就被摊薄了。

- **Q: 为什么攒批之后，WAL+NORMAL 和 DELETE+FULL 性能一样？**
- A: 因为 COMMIT 次数从 20000 降到 20。**单价差 9 倍，但只付 20 次**，差异被别处的开销淹没了。

- **Q: 这个发现有什么实际价值？**
- A: **持久性可以白拿。** NORMAL 本来是为了性能放弃 fsync；攒批之后可以换成 FULL（断电不丢），性能不变。**攒批消解了一个本来必须做的取舍。**

- **Q: 为什么 batch_size 不是越大越好？**
- A: 实测拐点在 1000，10000 反而慢。批的 vector 扩容 + 单次事务的 WAL 增长 + 内存压力。（确切原因未查明。）

- **Q: 为什么给 core 加 `dequeue_for` 不算"污染"？**
- A: 三条判据全部通过：① **通用能力**（`std::condition_variable` 原生就有 `wait_for`）② **兼容加法**（老接口一字未改，10 个老测试零修改通过）③ **换实现依然存在**（Java 的 `BlockingQueue.poll(timeout)` 是同一件事）。**「不改 core」是度量，不是目标。**

### CacheWriter 的单元测试（补齐）

`CacheWriter` 的排空逻辑有两个**很容易悄悄写错、错了就静默丢数据**的地方，
所以补了 `tests/test_cache_writer.cpp`（4 个用例，进 ctest）：

| # | 用例 | 防的是什么 |
|---|------|-----------|
| ① | 写 1000 条 → shutdown → 库里 1000 条 | INSERT / 绑定 / 事务写错 |
| ② | 写 3 条、`batch_size=500` → **库里 3 条** | **`run()` 退出后那批**（最容易漏的一步） |
| ③ | 写 3 条、**不 shutdown**、等 250ms → 库里 3 条 | **`flush_interval` 按时间触发**（`dequeue_for` 存在的全部理由） |
| ④ | `written + dropped == 提交数` | 没有数据静默消失 |

**两个测试设计上的决定值得记：**

**① 期望值写成「守恒律」而不是「dropped > 0」**
`assert(written + dropped == 5000)` —— 它在"丢了"和"没丢"两种情况下都成立，所以**稳定**。
写 `assert(dropped > 0)` 会 flaky（取决于调度）。**测试要断言不变量，不是断言现象的巧合。**

**② 骨架故意返回失败，实现完才翻成 0**
一开始 `main()` 里 `return 1` + 一句"四个用例尚未实现 —— 故意让它失败，避免假绿"。
否则四个空用例也会让 ctest 报 `[Passed]` —— **那就是假绿**。

**而且验证了测试真的有用**（老规矩：没失败过的测试，你不知道它有没有用）：
故意注释掉 `run()` 末尾那句 flush → 测试②精确命中：

```
Assertion failed: (count_rows(path) == 3),
function test_flushes_partial_batch_on_shutdown, line 141
```

### 遗留

- 存的是**原始报文**（`payload`），不是结构化的 `symbol/price/volume` —— 见决策 009
- `batch_size=10000` 变慢的确切原因未查明
- ④ 的守恒律成立，但**没验证过"真的发生了丢弃"** —— 如果队列从没满过，
  这个用例会退化成"written == 5000"。想加强的话得想办法制造确定性的拥塞。

---

## 待记录

- [x] M1: benchmark 报告（docs/benchmark-report-m1.md，含反直觉发现）
- [x] M2 第一步：适配层（MockWebSocketClient + AsyncReceiver + E2E + 负向用例）
- [x] M2 第二步：接真实数据源（Kraken + Qt，验收 `src/core/` 零改动）
- [x] M2 收尾：真实数据流过完整管道（+ 修 T1 缺失与断言漏洞）
- [x] M2 第三步：接进 FinceptTerminal（`add_subdirectory` + 改造 handle_ws_line）
- [x] M2 性能实测：结论「改造是对的，但当前负载下收益可忽略」（实测 145 条/秒 vs 临界 1,120+）
- [x] M3: CacheWriter 批量写入（实测 15.6× 加速、拐点 batch_size=1000、"持久性可白拿"）
- [x] M3 收尾：CacheWriter 单元测试（4 个用例，已人工破坏验证过有效性）
- [ ] M4: 文档 + 简历

# HANDOFF — 会话交接文档

> 生成于 2026-09-12，更新于 2026-09-16。新会话**第一步必读**，然后读 PLAN.md 和 LEARNING_LOG.md。

---

## 一、项目是什么

FinceptPipeline：基于 FinceptTerminal 设计的实时行情异步处理框架（C++20 STL，纯 C++ core 层）。
学习目标：C++ 并发 + 系统设计 + 性能工程 → 进入 AI Infra。

- 架构与路线图：[PLAN.md](PLAN.md)
- 学习日志（所有知识点 + 教训 + 面试题）：[LEARNING_LOG.md](LEARNING_LOG.md)

## 二、教学模式约定（务必遵守）

来自 LEARNING_LOG.md 和用户多次明确反馈：

1. **不当代码生成器**——先讲设计思想，给骨架 + TODO 注释，让用户自己实现
2. 用户主动要求"直接修/直接写"时才可以写完整代码
3. 每阶段：解释 → 用户实现 → review（指出 bug 让用户修）→ 面试模拟 → 记入日志
4. 用户经常用**多个 AI 对照**（另一边的回答会贴过来要求审核）——审核时先判断"答的是不是被问的题"，再看事实

## 三、进度总览

```
M0 核心骨架        ✅ 完成（MessageQueue/ThreadPool/Worker Loop/MockDataGenerator）
M1 背压+延迟       ✅ 完成（3 背压策略 + LatencyTracker + benchmark + 报告）
M2 Fincept 接入    ✅ 三步全部完成（适配层 mock → Kraken+Qt → 接进 FinceptTerminal，真实行情稳定流过）
M3 批量写入        ✅ 完成（CacheWriter + 性能实测，见决策 007/008/009）
M4 文档+简历       ✅ 完成（README + 简历描述两版 + 决策日志分类）
```

**12 个测试全绿。项目五个阶段全部完成。**benchmark 报告：[docs/benchmark-report-m1.md](docs/benchmark-report-m1.md)（含"DropNewest 等待并不最短"的反直觉发现）。

## 四、当前状态：M3 完成 ✅

**总览：**

| 阶段 | 内容 | 状态 |
|------|------|------|
| M0 / M1 | MessageQueue / ThreadPool / LatencyTracker / 背压 / benchmark | ✅ |
| **M2** | 适配层 → 真实数据源（Kraken + Qt）→ 接进 FinceptTerminal，真实行情端到端流过 | ✅ |
| **M3** | CacheWriter 批量写入 SQLite + 性能实测 | ✅ |

**11 个测试全绿**（`ctest`）；另有 3 个手动运行的工具：`pipeline_bench` / `test_kraken_live` / `cache_bench`。

**M2 的设计要点（仍有效）：**

1. 数据源接口用**回调式**（`set_on_message`）——接口形状模仿真实 Qt 信号模型
2. 发布接口 `publish(const TickData&)`，**不传 PipelineItem**——监控时间戳不泄露给业务侧
3. 依赖倒置：Core 只认两张窄接口名片，Mock / Kraken 是它的两个实现

**文件状态：**

| 文件 | 状态 |
|------|------|
| `src/core/*` | ⚠️ **M2 全程零改动，但 M3 主动破了一次**（加 `dequeue_for`，见决策 008） |
| `src/core/MessageQueue.h` | ✅ 原有 3 个取法 + **新增 `dequeue_for`（带超时的等待）** |
| `src/adapters/IWebSocketClient.h` | ✅ 接口定稿（无需改动） |
| `src/adapters/MockWebSocketClient.h` | ✅ 收包线程 + push_message + disconnect 排空语义 + 析构 |
| `src/adapters/AsyncReceiver.h` | ✅ 回调翻译（盖 T0 + **T1**）+ reject 计数 |
| `src/adapters/KrakenWebSocketClient.h` | ✅ Qt 实现：子线程 + 事件循环 + 跨线程排空 |
| `src/adapters/CacheWriter.h` | ✅ **M3 新增**：独立线程 + 攒批 + SQLite（决策 007/009） |
| `tests/test_dequeue_for.cpp` | ✅ **M3 新增**（进 ctest） |
| `tests/test_adapter_e2e.cpp` | ✅ Mock 端到端 + 负向用例（进 ctest），断言链 T0≤T1≤T2≤T3 |
| `tests/test_kraken_live.cpp` | ✅ 真实行情 × 完整管道（**手动跑**） |
| `tests/probe_qt_ws.cpp` | 🔧 诊断探针（排障用，可删） |
| `benchmark/cache_bench.cpp` | ✅ **M3 新增**：batch_size × 落盘策略对照（**手动跑**） |

**决策日志 `docs/decisions/`（共 9 篇）**，M2/M3 阶段新增的三篇：

- **007** CacheWriter 线程模型（独立线程 + 自己的队列）
- **008** MessageQueue 加带超时的等待 —— **主动打破 core 零改动**
- **009** 存储形态（存原始报文而非结构化字段）

### M3 最重要的两个结果

1. **攒批 15.6× 加速，拐点在 `batch_size=1000`**（10000 反而变慢）
2. **攒批抹平了落盘策略差异 → 持久性可以白拿**：`WAL+NORMAL` 本来是为性能放弃 fsync，
   攒批之后可以换成 `WAL+FULL`（断电不丢）而**性能不变**（实测 0.034s vs 0.034s）

⚠️ 顺带修正了两个估错的数字：007 里的「fsync 毫秒级」、004 里的「单条 INSERT 0.5-2ms」，
**实测分别只有 256μs 和 27.8μs**。方向对，量级估高了 4~6 倍。现代 NVMe 比想象中快得多。

### 下一步：项目已完整 ✅ —— 可选增强

**M0~M4 五个阶段全部完成。** 12 个 ctest 测试全绿。

**M4 产出：**

- `README.md` —— 项目门面（158 行）：三个亮点 / 架构图 / 构建命令 / M1+M3 实测数据 / 决策索引 / **已知边界（诚实）**
- `PLAN.md` 第十二节 —— 简历描述**两版**：版本 A 简历用（4 条）、版本 B 作品集用（完整）
- `PLAN.md` 第十节 —— 决策日志区分「选型决策（001~006，一行式）」与「取舍决策（007~009，完整论证）」
- **修正一处错误**：原「Cache 批量写入 ↔ KV Cache」的比喻是错的（KV Cache 缓存中间结果避免重算，攒批合并 I/O 事务），已换成 **攒批 ↔ vLLM continuous batching**

**可选增强（都不是必做）：**

- M2 那次改造的性能前后对比（目前是推算，未做同时段对照）—— 成本高、收益低
- `CacheWriter` 测试的④ 尚未验证"确定性地制造队列拥塞"
- 分布式 / 多机场景未涉及

### FinceptTerminal 侧的关键信息（省得重新查）

- 位置：`/Users/kudoshinichi/projects/FinceptTerminal`（独立项目，和本仓库平级）
- 重编：`cd FinceptTerminal/fincept-qt && cmake --build --preset macos-release`
- Qt：6.8.3，装在 `FinceptTerminal/.qt/`（与 brew 的 6.11.2 隔离 —— 一个进程只能有一份 Qt）
- 运行日志：`~/Library/Application Support/com.fincept.terminal/logs/fincept.log`
- Fincept 自己的 Python 环境：`~/Library/Application Support/com.fincept.terminal/venv-numpy2`

**改动清单（我们碰过的 Fincept 文件）：**

| 文件 | 改动 |
|------|------|
| `fincept-qt/CMakeLists.txt` | `add_subdirectory` 接入 + 链接 `fincept_pipeline` |
| `src/trading/pipeline/PipelineBridge.{h,cpp}` | **新增**：复用 MessageQueue / ThreadPool / LatencyTracker |
| `src/trading/ExchangeSession.{h,cpp}` | `handle_ws_line` 拆成 分流/控制/数据 三段 + 周期性统计日志 |

**⚠️ 首次运行竞态（不是我们引入的，但极容易被误认为是我们搞坏了）：**

应用启动时如果 venv 还没建好，`PythonRunner` 会把"用系统 python3"这个决定**永久缓存**，
之后所有 Python 子进程（`ws_stream.py` / `ExchangeDaemonPool` / `yfinance`）全部失败
→ CRYPTO 页面显示"离线"、价格全是 `--`。
**修复：完全退出应用重新打开**（日志里会从 `System Python found: python3` 变成 `Using UV-managed venv-numpy2`）。

**插入点决策记录（已拍板选 A）：**

```
Fincept 现有链路：
  ws_stream.py → ExchangeSession(解析JSON) → ExchangeSessionManager → DataHub

A ✅ 插在 WebSocket 层（原始报文）—— 选它
   理由：B（插在已解析层）意味着解析仍在原线程做，pipeline 只把"发布"挪后，
        收益趋近于零。

关键接口事实：
- DataHub::instance().publish(topic, QVariant) 线程安全（内部 queued connection 编组到 hub 线程）
- topic 规范：fincept-qt/docs/DATAHUB_TOPICS.md，行情类 `ws:<exchange>:ticker:<pair>`
- ws_stream.py 输出格式：紧凑 JSON 无空格、`type` 是第一个字段（廉价判型依赖这个契约）
```

**已知边界（代码里已留注释，别当成 bug）：**

- 一次连接对应一个队列，断线重连需要新队列（见 `AsyncReceiver.h` 顶部）
- `MockWebSocketClient::connect()` 调两次会 `std::terminate()`（`KrakenWebSocketClient` 已加 joinable 防护）
- E2E 的排空语义验证依赖时序（N=1000 > 容量 100 保证有积压），**不是确定性设计**
- **测试用 `assert`，Release 构建（`-DNDEBUG`）下全部失效** —— T1 那类 bug 在 Release 下依然抓不到
- **真实 socket 的"排空"只能尽力而为**：Mock 的 `pending_` 看得见数得清，真实数据可能还在 TCP 缓冲区里没变成事件
- **`IWebSocketClient` 尚无 `on_disconnect`**：连接无声断掉时上游不知道（VPN 掉线 / 被踢 / 抖动）—— 这个决策还没做
- **性能实测已完成，结论是「改造对，但这个场景用不上」**：实测 145 条/秒（Kraken+OKX 并发），
  单条主线程开销约 89μs → 主线程占用约 1.3%，而临界区间在 1,120~11,250 条/秒。
  **差 8~40 倍。** 详见 LEARNING_LOG 的「性能实测」段
- **`drain_ws_buffer` 的 4ms/50行预算保留了**（作为安全网）。改造后它应该很少触发，但没实测
- **`pipeline_stats_text()` 只进了日志，没接界面**

**运行真实行情测试的前置条件：**

- VPN 必须开着（Kraken 在墙外）
- `cd build && ./test_kraken_live`（**不要**用 ctest，它不注册在 ctest 里）
- 约 30 秒收够 30 条后自动收尾；超时上限 120 秒

## 五、工程约定

- **远端仓库**：https://github.com/qiuku30/FinceptPipeline （`origin`，分支 `main`）
- **git 身份**：`qiuku30` / `265786174+qiuku30@users.noreply.github.com`（全局配置，隐私地址）
- **不提交的东西**（见 `.gitignore`）：`build/`、`compile_commands.json`（软链）、`.cache/`（clangd 索引）

- 构建：`cd build && cmake .. -G Ninja && ninja && ctest`
- **改了 CMakeLists 要重跑 cmake；只改 .h/.cpp 跑 ninja 就够**（历史事故：漏 ninja 直接 ctest = 跑旧二进制 = 假绿）
- IDE 报 `'core/xxx.h' file not found` 等一堆错误 = **IntelliSense 噪音，以 cmake 编译为准**。
  注意用户用的是 **cpptools 不是 clangd** —— cpptools **不会**自动读 `compile_commands.json`，
  已在 `/Users/kudoshinichi/projects/.vscode/c_cpp_properties.json` 里显式配了 `compileCommands`。
  ⚠️ VSCode 工作区根目录是 `projects/`，不是 `FinceptPipeline/`，路径要算对。
- Qt 装在 `/opt/homebrew/opt/qt`（brew 的 Qt 6），CMakeLists 里已内置 `CMAKE_PREFIX_PATH` 提示，`cmake ..` 直接可用
- 消息类型：队列里流通 `PipelineItem`（TickData + TickLatency 成对）
- Worker Loop 退出条件**只看 dequeue 返回值**（历史教训：should_stop 竞态）
- shutdown 顺序铁律：先 queue.shutdown()（或 gen/receiver 内部完成）再 pool.shutdown()

## 六、用户画像速览

- C++ 并发学习中，基础概念（mutex/CV/atomic/CAS）已过一遍但需反复巩固
- 术语过敏：新名词要先解释再用（"名片"=接口、依赖倒置等概念曾让用户蒙）
- 学习节奏：解释 → 动手 → review → 面试题，喜欢把结论记进 LEARNING_LOG
- 常用提问模式："检查一下"、"给出方案"、"最优答案"、"讲解一下"

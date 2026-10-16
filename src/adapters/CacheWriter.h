#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <sqlite3.h>

#include "core/MessageQueue.h"
#include "core/Types.h"

// ============================================================
// CacheWriter — 把处理完的数据【批量】写进 SQLite
//
// 设计决策见 docs/decisions/007-cachewriter-thread-model.md
//
// ── 为什么是独立线程 ────────────────────────────────────────
//
//   fsync 是毫秒级，而管道处理一条消息只要 ~92μs（M2 第三步实测）
//   —— 差一个数量级。如果让 worker 自己写盘：
//
//     · 那条消息的处理时间从 92μs 变成 ~1ms → P99 崩坏
//     · SQLite 同一时刻只允许一个写事务 → 多 worker 并发 COMMIT 互相阻塞
//
//   结论：**不能让慢路径拖住快路径。** 写盘由专职线程承受。
//
// ── 结构 ────────────────────────────────────────────────────
//
//   Workers ──→ MessageQueue<PipelineItem> ──→ CacheWriter 线程 ──→ SQLite
//              （复用 core 的队列）
//
//   和 Publisher 对称：都是 worker 的下游消费者，一个快一个慢。
//
// ── 攒批的两条触发路径 ──────────────────────────────────────
//
//   ① batch_.size() >= batch_size      攒够了 → 写
//   ② 距上次写 >= flush_interval        等太久了 → 写
//
//   为什么两条都要：
//     只留 ① → 低负载时批永远攒不满，数据永远不落盘（延迟无限）
//     只留 ② → 高负载时批会巨大，内存爆 + 单次写卡顿
//
// ============================================================
/// CacheWriter 的配置。
///
/// ⚠️ 故意放在【命名空间层】而不是嵌套在类里 —— C++ 的一条真实规则：
///    嵌套类的"默认成员初始化器"在外层类定义【完成之前】不可用，
///    而构造函数默认参数 `Options opts = {}` 正好在"类还没完"的位置上求值。
///    嵌套着写会编译失败：
///      error: default member initializer for 'batch_size' needed within
///             definition of enclosing class 'CacheWriter' outside of member functions
struct CacheWriterOptions {
    std::size_t batch_size = 500;                  // 攒够多少条写一次
    std::chrono::milliseconds flush_interval{200}; // 最多等多久
    std::size_t queue_capacity = 8192;             // 提交队列容量

    // ── SQLite 落盘策略 ────────────────────────────────────────
    // 它直接决定「每次 COMMIT 有多贵」，是攒批收益大小的关键变量：
    //
    //   WAL + NORMAL : 提交【不】fsync，只在 checkpoint 时刷盘
    //                  → 快；代价是断电可能丢最近若干条提交
    //   WAL + FULL   : 每次提交都 fsync
    //                  → 慢（毫秒级）；断电不丢已提交的数据
    //
    // 实测数据见 LEARNING_LOG 的 M3 段。
    std::string journal_mode = "WAL";
    std::string synchronous = "NORMAL";
};

class CacheWriter {
  public:
    struct Stats {
        std::uint64_t written = 0;   // 已落盘条数
        std::uint64_t flushes = 0;   // COMMIT 次数 ← 这个值就是 fsync 次数
        std::uint64_t dropped = 0;   // 队列满被丢的条数
    };

    // header-only 项目：声明了就必须定义，否则链接期 undefined reference。
    explicit CacheWriter(std::string db_path, CacheWriterOptions opts = {})
        : db_path_(std::move(db_path)), opts_(opts),
          // DropOldest：队列满时丢最旧的，绝不阻塞调用方（worker）
          queue_(opts.queue_capacity, MessageQueue<PipelineItem>::BackpressurePolicy::DropOldest) {
        // open_db 失败会打印错误并让 db_ 停在 nullptr —— 之后 write_batch 静默跳过。
        // 不在这里抛异常：构造失败让调用方处理，比半构造状态更难查。
        open_db();
        thread_ = std::thread([this] { run(); });
    }

    ~CacheWriter() { shutdown(); }

    CacheWriter(const CacheWriter&) = delete;
    CacheWriter& operator=(const CacheWriter&) = delete;

    // ------------------------------------------------------------
    void submit(PipelineItem item) {
        // 跑在 worker 线程上，绝不能阻塞它 —— 见 docs/decisions/007。
        // 队列满时 enqueue 按 DropOldest 丢最旧的，立刻返回。
        queue_.enqueue(std::move(item));
    }

    // ------------------------------------------------------------
    // shutdown — 【排空语义，第三次】
    //
    //   ① queue_.shutdown()     停收新数据，唤醒阻塞在 dequeue_for 上的写线程
    //   ② 写线程把队列【排空】    已提交但还没写的数据不能丢
    //   ③ flush 未满的最后一批    ← 最容易忘的一步（run() 退出后那一句）
    //   ④ join 线程
    //   ⑤ 关闭 SQLite 连接
    //
    // 漏掉 ③ 的话：应用关闭时那一批（可能几百条）静默消失。
    // ------------------------------------------------------------
    void shutdown() {
        // 幂等：第二次调用时 queue_ 已关、thread_ 已 join、db_ 已置空，全部跳过。

        // ① 停收新数据，唤醒可能阻塞在 dequeue_for 上的写线程
        queue_.shutdown();

        // ② 等写线程收工 —— 它会【排空队列】、写完最后一批、才退出
        if (thread_.joinable())
            thread_.join();

        // ③ 关连接。必须在 join 之后，否则写线程还在用它。
        if (db_) {
            sqlite3_close(db_);
            db_ = nullptr;
        }
    }

    Stats stats() const {
        return Stats{written_.load(std::memory_order_relaxed),
                     flushes_.load(std::memory_order_relaxed),
                     queue_.dropped_count()};
    }

    std::chrono::milliseconds last_flush_ago() const {
        const auto last = last_flush_ns_.load(std::memory_order_relaxed);
        if (last == 0)
            return std::chrono::milliseconds{-1}; // 还没 flush 过
        const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now().time_since_epoch())
                             .count();
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::nanoseconds{now - last});
    }

  private:
    // ------------------------------------------------------------
    // TODO 1: run — 写线程主函数
    //
    // 结构大致是：
    //
    //   while (真) {
    //       取一条（或超时）
    //       有 → 塞进 batch_；够了就 flush
    //       没有 → 看看 batch_ 是不是等太久了，是就 flush
    //       队列关了且取空了 → break
    //   }
    //
    // ⚠️ 【这里有个真坑，先想清楚再写】
    //
    //   MessageQueue 只有两种取法：
    //     bool dequeue(PipelineItem&)     阻塞，直到有数据或队列关闭
    //     bool try_dequeue(PipelineItem&) 非阻塞，立刻返回
    //
    //   而你需要的是第三种：**"等到有数据，或者等够 flush_interval"**。
    //
    //   用阻塞版 → 低负载时卡死在 dequeue 上，flush_interval 永远不会触发 ❌
    //   用非阻塞版 + sleep → 能work，但 sleep 多久？睡太久延迟高，
    //                        睡太短就是忙等烧 CPU
    //
    //   三条路，各自有代价：
    //     (a) try_dequeue + 短 sleep   —— 简单，但引入 sleep 粒度误差
    //     (b) 给 MessageQueue 加一个"带超时的 dequeue" ← 要动 core
    //     (c) 用独立定时器唤醒
    //
    //   选哪个？为什么？（这题没有标准答案，但你要能说清代价）
    // ------------------------------------------------------------
void run() {
    while (true) {
        PipelineItem item;

        // ★ 核心：带超时地等一条数据
        //   上限 = flush_interval，保证低负载时也能按时醒来
        bool got = queue_.dequeue_for(item, opts_.flush_interval);

        if (got) {
            // 有数据 → 塞进批
            batch_.push_back(std::move(item));

            // 触发路径 ①：攒够了就写
            if (batch_.size() >= opts_.batch_size) {
                write_batch(batch_);
            }
            continue;
        }

        // got == false：要么超时，要么队列关闭
        if (queue_.is_shutdown()) {
            break;   // 队列关了 → 退出循环
        }

        // 触发路径 ②：超时了，批非空就 flush
        if (!batch_.empty()) {
            write_batch(batch_);
        }
    }

    // ★ 退出后最后一批：最容易忘的一步
    //   循环里可能攒了几百条没到 batch_size，不写就丢了
    if (!batch_.empty()) {
        write_batch(batch_);
    }
}
    // ------------------------------------------------------------
    // TODO 2: open_db —— 建库建表
    //
    //   一次性做完：sqlite3_open + CREATE TABLE IF NOT EXISTS
    //
    //   表结构自己设计，但至少有：symbol / ts / price / volume
    //   想一想：要不要建索引？（读少写多 → 第一版建议不建，但要说得出理由）
    // ------------------------------------------------------------
    bool open_db()
    {
      // ① 打开（或创建）数据库文件
    int rc = sqlite3_open(db_path_.c_str(), &db_);
    if (rc != SQLITE_OK) {
        std::cerr << "[CacheWriter] 打开数据库失败: " << sqlite3_errmsg(db_) << "\n";
        sqlite3_close(db_);
        db_ = nullptr;
        return false;
    }

    // ② 建表（IF NOT EXISTS，可重复执行）
    //
    // ⚠️ 表结构必须匹配 TickData 的【实际形态】。
    //    TickData 是「原始报文容器」，不是已解析的行情：
    //        id / topic / payload(原始JSON) / received_at
    //    所以这里存 topic + payload，而不是 symbol/price/volume —— 后者根本不存在。
    //
    //    为什么第一版这么选：
    //      要存结构化的 symbol/price/volume，就必须让【解析结果也流进管道】，
    //      那 MessageQueue 就得承载领域类型，core 就得知道业务类型
    //      —— 违反依赖倒置。这是个真实的架构取舍，先不在这里解决。
    //
    //    不建索引：本场景读少写多，索引会拖慢每一次 INSERT。
    //    （将来真要建，topic 上建一个就够。）
    //
    //    t0_ns / t4_ns 用的是 steady_clock（相对开机时刻），
    //    **只在单次运行内有意义**，跨进程比较没有价值。
    //    要真正的墙上时间，得给 TickData 加一个 system_clock 字段 —— 那是另一件事。
    const char* create_sql = R"(
        CREATE TABLE IF NOT EXISTS ticks (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            msg_id      INTEGER NOT NULL,   -- TickData.id（管道内单调序号）
            topic       TEXT    NOT NULL,   -- 如 ws:kraken:ticker:BTC/USDT
            payload     TEXT    NOT NULL,   -- 原始报文
            t0_ns       INTEGER NOT NULL,   -- T0 received_at
            t4_ns       INTEGER NOT NULL    -- T4 cached_at（写入时盖）
        );
    )";

    char* errmsg = nullptr;
    rc = sqlite3_exec(db_, create_sql, nullptr, nullptr, &errmsg);
    if (rc != SQLITE_OK) {
        std::cerr << "[CacheWriter] 建表失败: " << (errmsg ? errmsg : "unknown") << "\n";
        sqlite3_free(errmsg);
        sqlite3_close(db_);
        db_ = nullptr;
        return false;
    }

    // ③ 落盘策略（见 CacheWriterOptions 里的说明）
    //    用 exec 而非 prepare —— PRAGMA 不需要参数绑定。
    //    失败不致命（比如旧版 SQLite 不认 WAL），忽略返回值即可。
    const std::string pragma_journal = "PRAGMA journal_mode=" + opts_.journal_mode + ";";
    const std::string pragma_sync = "PRAGMA synchronous=" + opts_.synchronous + ";";
    sqlite3_exec(db_, pragma_journal.c_str(), nullptr, nullptr, nullptr);
    sqlite3_exec(db_, pragma_sync.c_str(), nullptr, nullptr, nullptr);

    return true;
    }

    // ------------------------------------------------------------
    // TODO 3: write_batch —— 【M3 的核心，就这几行】
    //
    //   BEGIN;
    //   INSERT ...;   × N
    //   COMMIT;       ← 只有这一次 fsync
    //
    //   注意：
    //     · 用 prepared statement，不要每次拼 SQL 字符串
    //     · 事务里出错要能回滚，别把库写坏
    //     · 写完清空 batch_
    //
    //   这个函数的调用频率 = fsync 频率 = 性能的全部来源。
    // ------------------------------------------------------------
    void write_batch(std::vector<PipelineItem>& batch)
    {
      if (batch.empty() || db_ == nullptr) return;

    // ① BEGIN：开启事务
    char* errmsg = nullptr;
    if (sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, &errmsg) != SQLITE_OK) {
        std::cerr << "[CacheWriter] BEGIN 失败: " << (errmsg ? errmsg : "unknown") << "\n";
        sqlite3_free(errmsg);
        return;
    }

    // ② 准备语句（一次准备，N 次复用）
    sqlite3_stmt* stmt = nullptr;
    const char* insert_sql =
        "INSERT INTO ticks(msg_id, topic, payload, t0_ns, t4_ns) VALUES(?, ?, ?, ?, ?);";

    if (sqlite3_prepare_v2(db_, insert_sql, -1, &stmt, nullptr) != SQLITE_OK) {
        std::cerr << "[CacheWriter] prepare 失败: " << sqlite3_errmsg(db_) << "\n";
        sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
        return;
    }

    // steady_clock 的时间点 → 纳秒整数。
    // 必须先 duration_cast 到 nanoseconds 再取 count：
    // steady_clock 的原生周期在不同平台上不一样，直接取 count 会串味。
    const auto to_ns = [](std::chrono::steady_clock::time_point tp) -> sqlite3_int64 {
        return static_cast<sqlite3_int64>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(tp.time_since_epoch()).count());
    };

    // ③ 循环绑定 + 执行
    bool failed = false;
    for (auto& item : batch) {
        // ★ 盖 T4 —— 这个字段从 M1 起就留在 TickLatency 里，一直没人填
        item.latency.cached_at = std::chrono::steady_clock::now();

        sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(item.data.id));
        sqlite3_bind_text(stmt, 2, item.data.topic.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, item.data.payload.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 4, to_ns(item.latency.received_at)); // T0
        sqlite3_bind_int64(stmt, 5, to_ns(item.latency.cached_at));   // T4

        if (sqlite3_step(stmt) != SQLITE_DONE) {
            std::cerr << "[CacheWriter] step 失败: " << sqlite3_errmsg(db_) << "\n";
            failed = true;
            break;
        }

        sqlite3_reset(stmt);          // 重置语句，供下一次复用
        sqlite3_clear_bindings(stmt); // 清除上一次的绑定值
    }

    sqlite3_finalize(stmt);   // 释放 prepared statement

    // ④ COMMIT 或 ROLLBACK
    if (failed) {
        sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
        return;
    }

    if (sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, &errmsg) != SQLITE_OK) {
        std::cerr << "[CacheWriter] COMMIT 失败: " << (errmsg ? errmsg : "unknown") << "\n";
        sqlite3_free(errmsg);
        return;
    }

    // ⑤ 更新统计 + 清空批
    written_ += batch.size();
    flushes_ += 1;
    last_flush_ns_.store(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count(),
        std::memory_order_relaxed);
    batch.clear();
    }

    const std::string db_path_;
    const CacheWriterOptions opts_;

    // 复用 core 的队列。DropOldest：队列满时丢最旧的，不阻塞 worker。
    MessageQueue<PipelineItem> queue_;
    std::thread thread_;

    // 只有写线程碰这些
    sqlite3* db_ = nullptr;
    std::vector<PipelineItem> batch_;

    std::atomic<std::uint64_t> written_{0};
    std::atomic<std::uint64_t> flushes_{0};
    std::atomic<std::int64_t> last_flush_ns_{0}; // 供 last_flush_ago()
};

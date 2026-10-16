#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>

#include <sqlite3.h>

#include "adapters/CacheWriter.h"

// ============================================================
// CacheWriter 行为验证
//
// 它此前【不在 ctest 覆盖内】—— 只有 cache_bench 顺带数了行数。
// 而它里面有两个很容易悄悄写错、错了就【静默丢数据】的地方：
//   ① 关闭时未满的那一批（run() 循环退出后那一句）
//   ② 低负载时的按时间 flush（dequeue_for 存在的全部理由）
//
// 四个用例，按价值排序见下方各自的注释。
//
// 通用约定：
//   · 每个用例用独立的临时 db，跑完删掉（互不干扰）
//   · **shutdown 之后再查库**，才能验证"最后一批"
//   · 查库用独立连接（count_rows 自己开），不看 writer 自报的 stats
// ============================================================

namespace {

using namespace std::chrono;

/// 独立的临时数据库路径（每个用例一个，互不干扰）
std::string temp_db(const char* tag) {
    return std::string("/tmp/fincept_cachewriter_test_") + tag + ".db";
}

/// 删掉 db 及其附属文件（WAL / SHM / journal）
void remove_db(const std::string& path) {
    for (const char* suffix : {"", "-wal", "-shm", "-journal"})
        std::remove((path + suffix).c_str());
}

/// 独立开一个只读连接数行数 —— 不看 CacheWriter 自报的 written。
/// （cache_bench.cpp 里已经有一份同样的，可以直接搬）
std::uint64_t count_rows(const std::string& db_path) {
    sqlite3* db = nullptr;
    if (sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) {
        if (db)
            sqlite3_close(db);
        return 0;
    }
    std::uint64_t n = 0;
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM ticks;", -1, &stmt, nullptr) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW)
            n = static_cast<std::uint64_t>(sqlite3_column_int64(stmt, 0));
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return n;
}

/// 造一条有辨识度的测试数据
PipelineItem make_item(std::uint64_t id) {
    PipelineItem item;
    item.data.id = id;
    item.data.topic = "ws:test:ticker:BTC/USD";
    item.data.payload = R"({"last":85800.5,"n":)" + std::to_string(id) + "}";
    item.latency.received_at = steady_clock::now();
    return item;
}

} // namespace

// ============================================================
// ① 基本不丢（最简单，先跑通框架）
//
//   写 1000 条，batch_size = 100，然后 shutdown
//   期望：库里 1000 条
//
//   防的是 INSERT / 参数绑定 / 事务提交写错。
// ============================================================
void test_writes_all_rows() {
    // TODO:
    //   1. path = temp_db("basic"); remove_db(path);
    //   2. CacheWriterOptions opts; opts.batch_size = 100;
    //   3. 开一个 CacheWriter，submit 1000 条
    //   4. shutdown()          ← 必须在作用域结束前显式调，才能验证最后一批
    //   5. assert(count_rows(path) == 1000)
    //   6. remove_db(path)

    const std::string path = temp_db("basic");
    remove_db(path);

    CacheWriterOptions opts;
    opts.batch_size = 100;

    {
        CacheWriter writer(path, opts);
        for (std::uint64_t i = 0; i < 1000; ++i) {
            writer.submit(make_item(i));
        }
        writer.shutdown();   // ★ 必须在作用域结束前显式调
    }

    assert(count_rows(path) == 1000);
    remove_db(path);
    std::cout << "[PASS] ① 基本不丢（1000 条全部落盘）\n";
}

// ============================================================
// ② 关闭时不足一批也要落盘  ← 最容易漏的一步
//
//   写 3 条，batch_size = 500（永远攒不满），立刻 shutdown
//   期望：库里 3 条
//
//   防的是 run() 循环【退出后】那一批 ——
//   如果漏写末尾那句 `if (!batch_.empty()) write_batch();`，这个测试会挂。
//
//   （我手动验证过一次：submit 一条 → shutdown → 库里 1 条 ✓，
//     但那是临时探针，不算数。正式用例要有。）
// ============================================================
void test_flushes_partial_batch_on_shutdown() {
    // TODO: 结构同 ①，但 batch_size = 500、只写 3 条、立刻 shutdown
    //       断言库里恰好 3 条
    const std::string path = temp_db("partial");
    remove_db(path);

    CacheWriterOptions opts;
    opts.batch_size = 500;   // 故意设大，3 条永远攒不满

    {
        CacheWriter writer(path, opts);
        for (std::uint64_t i = 0; i < 3; ++i) {
            writer.submit(make_item(i));
        }
        writer.shutdown();
    }

    assert(count_rows(path) == 3);
    remove_db(path);
    std::cout << "[PASS] ② 关闭时排空最后一批（3 条不足一批也落盘）\n";
}

// ============================================================
// ③ flush_interval 真的会触发  ← 【最有价值的一个】
//
//   写 3 条，batch_size = 500，flush_interval = 100ms
//   【不调 shutdown】，等 250ms，然后查库
//   期望：库里已经有 3 条
//
//   为什么最有价值：它验证的是 M3 的核心设计 ——「低负载时也能按时落盘」。
//   如果没有 dequeue_for（用阻塞的 dequeue），写线程会卡死在等数据上，
//   flush_interval 永远不会触发，这个测试就会挂。
//
//   ⚠️ 两个要点：
//     · 查库必须在 shutdown 之前 —— 否则你验证的还是 ②（最后一批），不是 ③
//     · count_rows 会另开一个连接，所以和 writer 持有的连接不冲突
//     · 最后还是要 shutdown 一次，干净收尾
// ============================================================
void test_flush_interval_triggers_without_shutdown() {
    // TODO:
    //   1. flush_interval = 100ms，batch_size = 500
    //   2. 写 3 条
    //   3. std::this_thread::sleep_for(250ms)      ← 等它自然 flush
    //   4. assert(count_rows(path) == 3)           ← ★ shutdown 之前查！
    //   5. writer.shutdown()
    //   6. assert(count_rows(path) == 3)           ← 再查一次也该是 3（没重复写）

    const std::string path = temp_db("interval");
    remove_db(path);

    CacheWriterOptions opts;
    opts.batch_size = 500;
    opts.flush_interval = std::chrono::milliseconds(100);

    CacheWriter writer(path, opts);

    for (std::uint64_t i = 0; i < 3; ++i) {
        writer.submit(make_item(i));
    }

    // ★ 关键：不 shutdown，等它自然 flush
    std::this_thread::sleep_for(std::chrono::milliseconds(250));

    // ★ 在 shutdown 之前查库 —— 否则验证的是用例 ②，不是 ③
    assert(count_rows(path) == 3);

    // 干净收尾
    writer.shutdown();
    assert(count_rows(path) == 3);   // 再查一次：没重复写

    remove_db(path);
}

// ============================================================
// ④ 队列满时丢数据，但【总数守恒】
//
//   queue_capacity = 8（故意很小），batch_size = 10，
//   疯狂 submit 5000 条（比写入速度快得多）
//   期望：written + dropped == 5000
//
//   ⚠️ 期望值的形式很关键：
//      不要写 assert(dropped > 0) —— 那取决于调度，会 flaky。
//      写【守恒律】written + dropped == submitted —— 它在"丢了"和"没丢"
//      两种情况下都成立，所以稳定。
//      它验证的是：没有数据静默消失。
//
//   守恒律两边都用谁？
//      submitted  = 你循环 submit 的次数
//      written    = stats().written（CacheWriter 自报）
//      dropped    = stats().dropped（来自队列的 dropped_count）
//   注意 stats() 要在 shutdown 之后读，否则最后一批还没算进去。
// ============================================================
void test_conservation_under_drop_oldest() {
    // TODO:
    //   1. queue_capacity = 8, batch_size = 10
    //   2. 循环 submit 5000 条
    //   3. shutdown()
    //   4. const auto s = writer.stats();
    //      assert(s.written + s.dropped == 5000)
    //   5. 顺带：assert(count_rows(path) == s.written)   ← 自报数和库里的数要对得上

    const std::string path = temp_db("conservation");
    remove_db(path);

    CacheWriterOptions opts;
    opts.queue_capacity = 8;    // 故意很小，快速触发 DropOldest
    opts.batch_size = 10;
    opts.flush_interval = std::chrono::milliseconds(50);

    const std::uint64_t TOTAL = 5000;

    CacheWriter writer(path, opts);

    for (std::uint64_t i = 0; i < TOTAL; ++i) {
        writer.submit(make_item(i));
    }

    writer.shutdown();

    const auto s = writer.stats();
    assert(s.written + s.dropped == TOTAL);   // ★ 守恒律
    assert(count_rows(path) == s.written);    // ★ 自报数和库里对得上

    remove_db(path);
    std::cout << "[PASS] ④ DropOldest 下的守恒律（written + dropped == 提交数）\n";
}

int main() {
    test_writes_all_rows();
    test_flushes_partial_batch_on_shutdown();
    test_flush_interval_triggers_without_shutdown();
    test_conservation_under_drop_oldest();

    std::cout << "\nAll CacheWriter tests passed!\n";
    return 0;
}

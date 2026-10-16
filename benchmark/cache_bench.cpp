#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "adapters/CacheWriter.h"

// ============================================================
// M3 benchmark — 批量写入的两个变量
//
//   ./cache_bench <条数> batch   扫描 batch_size
//   ./cache_bench <条数> pragma  扫描 SQLite 落盘策略（默认）
//
// ── batch 模式回答 ──────────────────────────────────────────
//   batch_size 对性能的影响，拐点在哪
//
// ── pragma 模式回答 ─────────────────────────────────────────
//   攒批的收益到底来自哪？
//     如果来自 fsync        → WAL+FULL 那组应该慢得多、攒批收益巨大
//     如果来自事务固定开销   → 三组差距不大、攒批收益都差不多
//
//   两个模式都顺带验证正确性：写完直接查数据库行数，不信 CacheWriter 自报的数
// ============================================================

namespace {

using Clock = std::chrono::steady_clock;

struct Result {
    std::size_t batch_size = 0;
    std::string label;      // pragma 模式下用来说明配置
    double seconds = 0.0;
    std::uint64_t commits = 0;
    std::uint64_t dropped = 0;
    std::uint64_t rows_in_db = 0;
    double throughput = 0.0;
    double us_per_commit = 0.0;
};

/// 独立开一个只读连接数行数 —— 不看 CacheWriter 自报的 written
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

Result run_one(std::size_t batch_size, std::size_t total, const std::string& journal,
               const std::string& sync, const std::string& label) {
    Result r;
    r.batch_size = batch_size;
    r.label = label;

    const std::string db_path = "/tmp/fincept_cache_bench.db";
    for (const char* suffix : {"", "-wal", "-shm", "-journal"})
        std::remove((db_path + suffix).c_str());

    CacheWriterOptions opts;
    opts.batch_size = batch_size;
    // interval 设得很大 —— 只想测 batch_size / pragma，不让"按时间 flush"插进来干扰
    opts.flush_interval = std::chrono::seconds(60);
    opts.queue_capacity = total * 2 + 1024; // 足够大，绝不触发 DropOldest
    opts.journal_mode = journal;
    opts.synchronous = sync;

    const auto t0 = Clock::now();
    {
        CacheWriter writer(db_path, opts);

        for (std::size_t i = 0; i < total; ++i) {
            PipelineItem item;
            item.data.id = i;
            item.data.topic = "ws:bench:ticker:BTC/USD";
            item.data.payload = R"({"last":85800.5,"bid":85799.9,"ask":85801.0,"vol":12.34})";
            item.latency.received_at = Clock::now();
            writer.submit(std::move(item));
        }

        writer.shutdown(); // 排空队列 + flush 最后一批

        const auto s = writer.stats();
        r.commits = s.flushes;
        r.dropped = s.dropped;
    }
    const auto t1 = Clock::now();

    r.seconds = std::chrono::duration<double>(t1 - t0).count();
    r.throughput = static_cast<double>(total) / r.seconds;
    r.us_per_commit = r.commits ? (r.seconds * 1e6 / static_cast<double>(r.commits)) : 0.0;
    r.rows_in_db = count_rows(db_path);
    return r;
}

void print_header(const char* first_col) {
    std::cout << std::setw(22) << first_col << std::setw(10) << "批大小" << std::setw(12)
              << "总耗时(s)" << std::setw(11) << "COMMIT数" << std::setw(14) << "吞吐(条/秒)"
              << std::setw(16) << "每次COMMIT(μs)" << std::setw(10) << "行数" << "\n";
    std::cout << std::string(95, '-') << "\n";
}

void print_row(const Result& r, std::size_t total, bool show_label) {
    if (show_label)
        std::cout << std::setw(22) << r.label;
    std::cout << std::setw(10) << r.batch_size << std::setw(12) << std::fixed
              << std::setprecision(3) << r.seconds << std::setw(11) << r.commits << std::setw(14)
              << std::setprecision(0) << r.throughput << std::setw(16) << std::setprecision(1)
              << r.us_per_commit << std::setw(10) << r.rows_in_db;

    if (r.rows_in_db != total)
        std::cout << "  ❌ 丢数据！";
    else if (r.dropped != 0)
        std::cout << "  ⚠ 有丢弃";
    std::cout << "\n";
}

// ── 模式 1：扫描 batch_size（固定 WAL + NORMAL）────────────────
void bench_batch_sizes(std::size_t total) {
    std::cout << "\n【模式 batch】固定 WAL+NORMAL，只改 batch_size\n";
    std::cout << "每轮写入 " << total << " 条\n\n";
    print_header("落盘策略");

    run_one(1000, 1000, "WAL", "NORMAL", "预热"); // 预热，不打印

    std::vector<Result> rows;
    for (std::size_t bs : {1u, 10u, 100u, 1000u, 10000u})
        rows.push_back(run_one(bs, total, "WAL", "NORMAL", "WAL+NORMAL"));

    const Result& base = rows.front();
    for (const auto& r : rows)
        print_row(r, total, true);

    std::cout << std::string(95, '-') << "\n";
    std::cout << "\n相对单条写（batch_size=1）的加速比：\n";
    for (const auto& r : rows) {
        if (r.batch_size == 1)
            continue;
        std::cout << "  batch_size=" << std::setw(6) << r.batch_size << "  →  "
                  << std::fixed << std::setprecision(1) << (base.seconds / r.seconds) << " ×\n";
    }
}

// ── 模式 2：扫描落盘策略（固定 batch_size）────────────────────
void bench_pragmas(std::size_t total) {
    std::cout << "\n【模式 pragma】只改 SQLite 落盘策略，看攒批收益从哪来\n";
    std::cout << "每轮写入 " << total << " 条\n\n";
    print_header("落盘策略");

    run_one(1000, 1000, "WAL", "NORMAL", "预热"); // 预热

    struct Cfg {
        const char* journal;
        const char* sync;
        const char* label;
    };
    const std::vector<Cfg> cfgs = {
        {"WAL", "NORMAL", "WAL + NORMAL"},
        {"WAL", "FULL", "WAL + FULL"},
        {"DELETE", "FULL", "DELETE + FULL"},
    };

    for (const auto& c : cfgs) {
        const Result r1 = run_one(1, total, c.journal, c.sync, c.label);
        const Result r1000 = run_one(1000, total, c.journal, c.sync, c.label);
        print_row(r1, total, true);
        print_row(r1000, total, true);
        std::cout << std::setw(22) << "" << "    ↑ 上面两行：攒批加速比 = " << std::fixed
                  << std::setprecision(1) << (r1.seconds / r1000.seconds) << " ×\n\n";
    }
}

} // namespace

int main(int argc, char** argv) {
    const std::size_t total = (argc > 1) ? static_cast<std::size_t>(std::atoll(argv[1])) : 20000;
    const std::string mode = (argc > 2) ? argv[2] : "pragma";

    std::cout << "\nM3 — CacheWriter 批量写入 benchmark\n";
    std::cout << "(每轮都删库重建)\n";

    if (mode == "batch")
        bench_batch_sizes(total);
    else
        bench_pragmas(total);

    std::cout << "\n数据库留在 /tmp/fincept_cache_bench.db\n";
    return 0;
}

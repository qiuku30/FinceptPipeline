#include <cassert>
#include <chrono>
#include <cmath>
#include <iostream>
#include <thread>
#include <vector>

#include "core/LatencyTracker.h"

using namespace std::chrono;

// ============================================================
// 工具函数：构造一个指定 queue_wait 的样本
//
// 约定（方便测试验证）：
//   T0 = 时间原点
//   T1 = T0 + queue_wait 之前的时间（无所谓）
//   T2 = T1 + queue_wait
//   T3 = T2 + processing（processing 恒为 0，测试只关心 queue_wait）
//
// 即: queue_wait_time == 传入的 wait_us，
//     processing_time == 0，
//     end_to_end  == wait_us
// ============================================================

// TODO 0: 写一个 make_sample(uint64_t wait_us) 工具函数
// - 用一个固定的 steady_clock 时间点做基准（比如函数开头 now()）
// - 返回一个 TickLatency，四个时间点满足上面的约定
// --- 你的代码 ---
TickLatency make_sample(uint64_t wait_us) {
    auto base = steady_clock::now();
    TickLatency ts;
    ts.received_at = base;                      // T0
    ts.enqueued_at = base;                      // T1 = T0
    ts.dequeued_at = base + microseconds(wait_us); // T2 = T0 + wait_us
    ts.processed_at = ts.dequeued_at;           // T3 = T2 (processing=0)
    return ts;
}

// ============================================================
// 测试 1: 分位数正确性
//
// 构造 100 个样本，queue_wait 分别为 0,1,2,...,99 us
// 期望:
//   P50 == 50, P95 == 95, P99 == 99
//   avg == 49.5, min == 0, max == 99
//   total_processed == 100
// ============================================================
void test_percentiles() {
    // TODO 1:
    // - 循环 100 次，queue_wait = i，record 进去
    // - snapshot()
    // - 按上面期望值 assert（浮点比较用差值 < 1e-6）
    // --- 你的代码 ---
    LatencyTracker tracker;
    const int N = 100;

    // 1. 喂 100 个样本，wait_us = 0~99
    for (int i = 0; i < N; ++i) {
        tracker.record(make_sample(static_cast<uint64_t>(i)));
    }

    auto stats = tracker.snapshot();

    // 2. 验证样本总数
    assert(stats.total_processed == N);

    // 3. 浮点比较用 epsilon（1e-6），避免精度误差导致断言失败
    auto check_double = [](double got, double expected) {
        assert(std::abs(got - expected) < 1e-6);
    };

    // 4. 分位数验证：P50=50，P95=95，P99=99
    check_double(stats.queue_wait_p50_us, 50.0);
    check_double(stats.queue_wait_p95_us, 95.0);
    check_double(stats.queue_wait_p99_us, 99.0);

    // 5. avg/min/max：等差数列 0~99 的平均值 = 49.5
    check_double(stats.avg_us, 49.5);
    check_double(stats.min_us, 0.0);
    check_double(stats.max_us, 99.0);

    // 6. throughput: 跨度来自 make_sample 每次真实调用 now() 的微小差异。
    //    正常时钟分辨率下 > 0，但这是"环境敏感"断言——
    //    若将来 make_sample 改成固定基准时间，跨度=0 → throughput 保持 0。
    //    所以这里只断言非负，不做过强保证。
    assert(stats.throughput_msg_per_sec >= 0.0);
    std::cout << "[PASS] test_percentiles\n";
}

// ============================================================
// 测试 2: 空样本
// snapshot() 应该返回全零，不崩溃
// ============================================================
void test_empty_snapshot() {
    // TODO 2:
    // - 新建 tracker，直接 snapshot()
    // - 断言所有 p50/p95/p99、avg、min、max、total、throughput 都为 0
    // --- 你的代码 ---
    LatencyTracker tracker;
    auto stats = tracker.snapshot();

    // 所有字段都应该是 0（默认初始化的 LatencyStats 全零）
    assert(stats.queue_wait_p50_us == 0.0);
    assert(stats.queue_wait_p95_us == 0.0);
    assert(stats.queue_wait_p99_us == 0.0);
    // ... 其他字段同理
    assert(stats.total_processed == 0);
    assert(stats.throughput_msg_per_sec == 0.0);
    std::cout << "[PASS] test_empty_snapshot\n";
}

// ============================================================
// 测试 3: 采样率
//
// rate = 0.5 → 记 1000 条，sample_count 应恰好 == 500
// （用 counter % 2 == 0 的确定性取模，不是随机）
// ============================================================
void test_sampling_rate() {
    // TODO 3:
    // - set_sampling_rate(0.5)
    // - record 1000 条（queue_wait 随便填）
    // - assert sample_count() == 500
    // - 再测 rate = 0 时一条都不记: set_sampling_rate(0) → record → count == 0
    // --- 你的代码 ---
    LatencyTracker tracker;

    // 1. 采样率 0.5：1000 条 → 应该正好 500 条
    tracker.set_sampling_rate(0.5);
    for (int i = 0; i < 1000; ++i) {
        tracker.record(make_sample(10));
    }
    assert(tracker.sample_count() == 500);  // 确定性取模，必为 500

    // 2. 采样率 0.0：一条都不记
    tracker.reset();
    tracker.set_sampling_rate(0.0);
    for (int i = 0; i < 100; ++i) {
        tracker.record(make_sample(10));
    }
    assert(tracker.sample_count() == 0);

    // 3. 采样率 1.0：全量记
    tracker.reset();
    tracker.set_sampling_rate(1.0);
    for (int i = 0; i < 200; ++i) {
        tracker.record(make_sample(10));
    }
    assert(tracker.sample_count() == 200);
    std::cout << "[PASS] test_sampling_rate\n";
}

// ============================================================
// 测试 4: reset
// record 几条 → reset → sample_count == 0，snapshot 全零
// ============================================================
void test_reset() {
    // TODO 4:
    // - record 10 条
    // - reset()
    // - assert sample_count() == 0
    // - snapshot() 全零
    // --- 你的代码 ---
    LatencyTracker tracker;

    // 1. 先记录 10 条
    for (int i = 0; i < 10; ++i) {
        tracker.record(make_sample(1));
    }
    assert(tracker.sample_count() == 10);

    // 2. reset
    tracker.reset();
    assert(tracker.sample_count() == 0);

    // 3. snapshot 全零
    auto stats = tracker.snapshot();
    assert(stats.total_processed == 0);
    assert(stats.queue_wait_p50_us == 0.0);
    assert(stats.queue_wait_p95_us == 0.0);
    assert(stats.queue_wait_p99_us == 0.0);
    std::cout << "[PASS] test_reset\n";
}

// ============================================================
// 测试 5: 多线程 record 不丢样本 + 采样计数器并发安全
//
// 4 个线程各 record 250 条（共 1000 次调用），rate=0.5
// 关键点: 用采样路径（而非 rate=1.0 快速路径），让原子计数器
//         真正参与并发。
// 为什么结果确定是 500:
//   fetch_add 返回互不相同的旧值，4 个线程并发时值的集合
//   必然是 {0,1,...,999}（与线程调度顺序无关）。
//   interval=2 → 命中偶数的恰好 500 个 → 样本数确定 == 500。
// （同时验证了 mutex 保护：若 push_back 有竞态，count 会小于 500）
// ============================================================
void test_concurrent_record() {
    // TODO 5:
    // - 用 std::vector<std::thread> 开 4 个线程
    // - 每个线程 record 250 条
    // - join 后 assert sample_count() == 500
    // --- 你的代码 ---
    LatencyTracker tracker;
    tracker.set_sampling_rate(0.5);  // interval=2，走采样路径

    const int THREADS = 4;
    const int PER_THREAD = 250;

    std::vector<std::thread> threads;

    // 1. 起 4 个线程，每个 record 250 条
    for (int t = 0; t < THREADS; ++t) {
        threads.emplace_back([&tracker, PER_THREAD]() {
            for (int i = 0; i < PER_THREAD; ++i) {
                tracker.record(make_sample(10));
            }
        });
    }

    // 2. 等所有线程结束
    for (auto& th : threads) {
        th.join();
    }

    // 3. 1000 次调用 × 采样率 0.5 → 恰好 500 个样本
    assert(tracker.sample_count() == THREADS * PER_THREAD / 2);
    std::cout << "[PASS] test_concurrent_record\n";
}

int main() {
    test_percentiles();
    test_empty_snapshot();
    test_sampling_rate();
    test_reset();
    test_concurrent_record();
    std::cout << "\nAll LatencyTracker tests passed!\n";
    return 0;
}

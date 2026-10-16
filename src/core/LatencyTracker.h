#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <mutex>
#include <numeric>
#include <vector>

#include "core/Types.h"

// ============================================================
// LatencyTracker — 全链路延迟统计（横切模块）
//
// 不参与数据流向，只负责：
//   1. 收集每个 PipelineItem 的时间戳样本
//   2. 在 snapshot() 时聚合成 P50/P95/P99 等统计
//
// 指标计算（时间差 → 微秒 double）：
//   queue_wait_time    = T2 - T1
//   processing_time    = T3 - T2
//   end_to_end_latency = T3 - T0
//
// 线程安全约定：
//   record() 被 Worker 高频并发调用（内部加锁 + 原子计数）
//   snapshot()/reset()/sample_count() 内部加锁
//   set_sampling_rate() 应在 Worker 启动前调用一次
// ============================================================

// 聚合统计结果（snapshot() 的返回值）
struct LatencyStats {
    double queue_wait_p50_us, queue_wait_p95_us, queue_wait_p99_us;
    double processing_p50_us, processing_p95_us, processing_p99_us;
    double end_to_end_p50_us, end_to_end_p95_us, end_to_end_p99_us;
    double avg_us, max_us, min_us;     // 按 end_to_end 统计
    size_t total_processed;            // 样本总数
    double throughput_msg_per_sec;
};

class LatencyTracker {
public:
    LatencyTracker() = default;

    // ------------------------------------------------------------
    // record — Worker 高频调用，采集一条样本
    //
    // 采样策略（按 sampling_rate_ 分三种情况）：
    //   rate == 0.0 → 不记，直接返回
    //   rate == 1.0 → 全量记录（默认，快速路径）
    //   中间值      → 每 sample_interval_ 条记 1 条
    //                 例: rate=0.01 → interval=100 → counter%100==0 才记
    // 采样判断在加锁之前完成——未命中的样本根本不碰锁，
    // 降低高频采样场景下的锁争用。
    // ------------------------------------------------------------
    void record(const TickLatency& sample) {
        const double rate = sampling_rate_.load(std::memory_order_relaxed);

        // 情况 A：采样率 0 → 什么都不记
        if (rate == 0.0) {
            return;
        }

        // 情况 B：采样率 1.0 → 全量记录
        if (rate == 1.0) {
            std::lock_guard<std::mutex> lock(mutex_);
            samples_.push_back(sample);
            return;
        }

        // 情况 C：0 < rate < 1 → 按间隔取模采样
        // fetch_add 返回旧值，所以第一条的 count==0 必然被记录
        const uint64_t interval = sample_interval_.load(std::memory_order_relaxed);
        const uint64_t count = sample_counter_.fetch_add(1, std::memory_order_relaxed);
        if (count % interval != 0) {
            return;  // 未命中采样点，不记也不碰锁
        }

        std::lock_guard<std::mutex> lock(mutex_);
        samples_.push_back(sample);
    }

    // ------------------------------------------------------------
    // snapshot — 聚合统计，不清空历史
    //
    // 步骤：
    //   1. 锁内拷贝 samples_，锁外做所有计算（排序不占锁）
    //   2. 提取三个指标的 duration 数组（微秒 double）
    //   3. 每个数组排序一次，取 P50/P95/P99（index = n*x/100，clamp）
    //   4. avg/min/max 按 end_to_end
    //   5. throughput = n / 首尾样本 T0 的时间跨度
    //      边界: n < 2 或跨度 <= 0 → 0
    // 注意: 首尾样本按插入顺序取，隐含假设单个 Producer 下 T0 单调递增
    // ------------------------------------------------------------
    LatencyStats snapshot() const {
        // 1. 锁内拷贝，锁外计算
        std::vector<TickLatency> samples_copy;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            samples_copy = samples_;
        }

        LatencyStats stats{};  // 值初始化：全零
        const size_t n = samples_copy.size();
        if (n == 0) {
            return stats;  // 空样本 → 全零 stats
        }

        // time_point 相减 → 微秒数（int64_t → double）
        auto to_micro = [](const std::chrono::steady_clock::time_point& start,
                           const std::chrono::steady_clock::time_point& end) {
            return static_cast<double>(
                std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                    .count());
        };

        // 2. 提取三个指标的 duration 数组
        std::vector<double> queue_waits, processings, e2es;
        queue_waits.reserve(n);
        processings.reserve(n);
        e2es.reserve(n);
        for (const auto& s : samples_copy) {
            queue_waits.push_back(to_micro(s.enqueued_at, s.dequeued_at));   // T2 - T1
            processings.push_back(to_micro(s.dequeued_at, s.processed_at));  // T3 - T2
            e2es.push_back(to_micro(s.received_at, s.processed_at));         // T3 - T0
        }

        // 3. 每个数组只排序一次，然后取三个分位点
        // percentile 只读已排序数组，不再排序
        auto percentile_of_sorted = [](const std::vector<double>& sorted, double p) {
            size_t index = static_cast<size_t>(sorted.size() * p / 100.0);
            if (index >= sorted.size()) index = sorted.size() - 1;  // clamp
            return sorted[index];
        };

        std::sort(queue_waits.begin(), queue_waits.end());
        stats.queue_wait_p50_us = percentile_of_sorted(queue_waits, 50);
        stats.queue_wait_p95_us = percentile_of_sorted(queue_waits, 95);
        stats.queue_wait_p99_us = percentile_of_sorted(queue_waits, 99);

        std::sort(processings.begin(), processings.end());
        stats.processing_p50_us = percentile_of_sorted(processings, 50);
        stats.processing_p95_us = percentile_of_sorted(processings, 95);
        stats.processing_p99_us = percentile_of_sorted(processings, 99);

        std::sort(e2es.begin(), e2es.end());
        stats.end_to_end_p50_us = percentile_of_sorted(e2es, 50);
        stats.end_to_end_p95_us = percentile_of_sorted(e2es, 95);
        stats.end_to_end_p99_us = percentile_of_sorted(e2es, 99);

        stats.total_processed = n;

        // 4. avg/min/max 按 end_to_end，min/max 显式计算（不依赖排序后取首尾）
        const auto [min_it, max_it] = std::minmax_element(e2es.begin(), e2es.end());
        stats.min_us = *min_it;
        stats.max_us = *max_it;
        stats.avg_us = std::accumulate(e2es.begin(), e2es.end(), 0.0) / n;

        // 5. throughput：首尾样本 T0 的时间跨度
        if (n >= 2) {
            const auto first_t0 = samples_copy.front().received_at;
            const auto last_t0 = samples_copy.back().received_at;
            const double duration_sec =
                static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                        last_t0 - first_t0)
                                        .count()) /
                1'000'000.0;
            // duration_sec == 0（所有样本同一时刻，如测试用固定基准时间）→ 保持 0
            if (duration_sec > 0.0) {
                stats.throughput_msg_per_sec = static_cast<double>(n) / duration_sec;
            }
        }
        return stats;
    }

    // reset — 清空样本，同时重置采样计数器（采样模式重新开始）
    void reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        samples_.clear();
        sample_counter_.store(0, std::memory_order_relaxed);
    }

    // sample_count — 当前已采集样本数
    size_t sample_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return samples_.size();
    }

    // ------------------------------------------------------------
    // set_sampling_rate — 设置采样率并预计算采样间隔
    //
    // rate clamp 到 [0.0, 1.0]，interval = round(1/rate)
    //   rate=0.0 → interval 无意义（record 里直接 return）
    //   rate=1.0 → interval=1（record 走快速路径）
    // 约定：应在 Worker 启动前调用；与 record 并发调用视为未定义用法
    // ------------------------------------------------------------
    void set_sampling_rate(double rate) {
        if (rate < 0.0) rate = 0.0;
        if (rate > 1.0) rate = 1.0;
        sampling_rate_.store(rate, std::memory_order_relaxed);

        uint64_t interval = 1;
        if (rate == 0.0 || rate == 1.0) {
            interval = 1;  // record 的 A/B 快速路径会提前返回，用不到
        } else {
            interval = static_cast<uint64_t>(std::round(1.0 / rate));
            if (interval == 0) interval = 1;  // 防御浮点误差
        }
        sample_interval_.store(interval, std::memory_order_relaxed);
    }

private:
    mutable std::mutex mutex_;
    std::vector<TickLatency> samples_;
    std::atomic<double> sampling_rate_{1.0};       // 全量采集
    std::atomic<uint64_t> sample_counter_{0};      // 采样计数器（含未命中的调用）
    std::atomic<uint64_t> sample_interval_{1};     // 每 interval 条记 1 条
};

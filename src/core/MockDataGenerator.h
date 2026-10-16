#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

#include "core/MessageQueue.h"
#include "core/Types.h"

// ============================================================
// MockDataGenerator — 可控速率的行情数据源（模拟 WebSocket）
//
// 生产 PipelineItem 并盖 T0/T1 时间戳：
//   T0 received_at: 数据"到达"时刻（mock 里 = 构造时刻）
//   T1 enqueued_at: 入队时刻（mock 里紧邻 T0，近似相等）
//
// 行为约定：
//   - 放完 total 条后自动 queue.shutdown()（run() 末尾）
//   - stop() 是尽力而为的提前终止（卡在 enqueue 里时不立刻生效）
//   - rate <= 0 为非法参数（除零 → UB），调用方保证 > 0
// ============================================================
class MockDataGenerator {
public:
    // queue: 往哪个队列里放数据
    // total: 总共产生多少条（放完自动停）
    // rate:  每秒多少条。interval_ = 1s / rate
    explicit MockDataGenerator(MessageQueue<PipelineItem>& queue,
                               uint64_t total,
                               double rate_msg_per_sec)
        : queue_(queue)
        , total_(total)
        , interval_(std::chrono::nanoseconds(
              static_cast<uint64_t>(1'000'000'000.0 / rate_msg_per_sec))) {}

    ~MockDataGenerator() {
        if (thread_.joinable()) thread_.join();
    }

    // 禁止拷贝
    MockDataGenerator(const MockDataGenerator&) = delete;
    MockDataGenerator& operator=(const MockDataGenerator&) = delete;

    // 启动生成线程
    void start() {
        running_ = true;
        thread_ = std::thread(&MockDataGenerator::run, this);
    }

    // 提前终止（尽力而为：见 run() 里的检查点）
    void stop() {
        running_ = false;
    }

    // 等待生成线程结束
    void join() {
        if (thread_.joinable()) thread_.join();
    }

    bool is_running() const { return running_; }
    uint64_t generated_count() const { return generated_; }

private:
    // 线程主函数 — 按速率生产消息
    //
    // 用 sleep_until 而不是 sleep_for：
    //   sleep_for(200us) 每次误差几微秒，10000 次后累计可达几十毫秒
    //   sleep_until(next_time) 每个周期独立瞄准绝对时间点，误差不累积
    void run() {
        auto next_time = std::chrono::steady_clock::now();
        for (uint64_t i = 0; i < total_; ++i) {
            std::this_thread::sleep_until(next_time);
            if (!running_) break;

            // 构造消息并盖 T0/T1 时间戳
            PipelineItem item;
            item.data.id = i;
            item.latency.received_at = std::chrono::steady_clock::now();  // T0
            item.latency.enqueued_at  = item.latency.received_at;          // T1 ≈ T0

            queue_.enqueue(item);
            ++generated_;

            next_time += interval_;
        }
        queue_.shutdown();  // 放完了，通知 Consumer 取完就收工
        running_ = false;   // 正常完成也要改状态（is_running 不说谎）
    }

    MessageQueue<PipelineItem>& queue_;
    uint64_t total_;
    std::chrono::nanoseconds interval_;

    std::thread thread_;
    std::atomic<bool> running_{false};
    std::atomic<uint64_t> generated_{0};
};

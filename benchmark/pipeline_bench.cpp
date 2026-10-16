#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "core/LatencyTracker.h"
#include "core/MessageQueue.h"
#include "core/MockDataGenerator.h"
#include "core/ThreadPool.h"
#include "core/Types.h"

using namespace std::chrono;
using Policy = MessageQueue<PipelineItem>::BackpressurePolicy;

// ============================================================
// pipeline_bench — M1 压力测试工具
//
// 跑 3 档速率 × 3 种背压策略 = 9 组，每组输出一行：
//   policy | rate | wall_time | dropped | processed |
//   queue_wait P50/P95/P99 | processing P50/P95/P99 | e2e P50/P95/P99
//
// 预期假设（跑完用数据验证）：
//   Block:      dropped=0，queue_wait 随 rate 暴涨，wall_time 最长
//   DropOldest: dropped 中等，queue_wait 有界
//   DropNewest: dropped 最多，queue_wait 最小
// ============================================================

// 单组 benchmark 的结果
struct BenchResult {
    std::string policy_name;
    int rate;
    double wall_sec;
    uint64_t dropped;
    size_t processed;
    LatencyStats stats;
};

// ============================================================
// benchmark 全局参数（main 的报表头也要用，所以放在文件级）
//
// PROCESS_US 的取值依据:
//   消费速率 = NUM_WORKERS / PROCESS_US = 4 / 900us ≈ 4,444 msg/s
//   rate 1000  → 消费者绰绰有余，无背压
//   rate 5000  → 5000 > 4444，队列积压（背压开始）
//   rate 10000 → 严重积压（三种策略差异最大化）
// ============================================================
constexpr int NUM_WORKERS = 4;
constexpr int QUEUE_CAPACITY = 1000;
constexpr int PROCESS_US = 900;
constexpr int RUN_SECONDS = 10;  // 每档跑约 10 秒: N = rate × RUN_SECONDS

// ------------------------------------------------------------
// TODO 1: 模拟处理耗时（忙等 busy-wait ~PROCESS_US 微秒）
//
// 为什么忙等而不是 sleep_for：
//   sleep 会把线程交给调度器，唤醒时间有随机抖动 → 数据噪声大
//   忙等占住 CPU 转圈，耗时稳定 → benchmark 数据干净
//
// 写法提示:
//   auto deadline = steady_clock::now() + microseconds(PROCESS_US);
//   while (steady_clock::now() < deadline) { /* 空转 */ }
// ------------------------------------------------------------
// --- 你的代码 ---
static inline void busy_wait_micro(int us) {
    // 条件里调用 now()（系统时钟读取），编译器无法优化掉这个循环
    auto deadline = steady_clock::now() + microseconds(us);
    while (steady_clock::now() < deadline) { /* 空转 */ }
}

// 策略转字符串（辅助）
const char* policy_to_string(Policy p) {
    switch (p) {
        case Policy::Block:      return "Block";
        case Policy::DropOldest: return "DropOldest";
        case Policy::DropNewest: return "DropNewest";
        default:                 return "Unknown";
    }
}



// ------------------------------------------------------------
// TODO 2: run_benchmark(policy, rate) — 跑一组，返回 BenchResult
//
// 参数: N = rate × RUN_SECONDS（每档约 10 秒的数据量）
//       容量 1000、4 Workers、PROCESS_US 模拟处理（见文件级常量）
//
// 步骤:
//   1. 创建组件
//      MessageQueue<PipelineItem> queue(1000, policy);
//      ThreadPool pool(4);
//      LatencyTracker tracker;
//      MockDataGenerator gen(queue, N, rate);
//
//   2. Worker Loop（这次要打时间戳！）:
//      while (true) {
//          PipelineItem item;
//          if (!queue.dequeue(item)) break;
//          item.latency.dequeued_at = now();   // T2: 取出
//          模拟处理(PROCESS_US);
//          item.latency.processed_at = now();  // T3: 处理完
//          tracker.record(item.latency);
//      }
//
//   3. 启动与收尾（顺序你已经很熟）:
//      pool.start(worker) → gen.start()
//      gen.join() → pool.shutdown()
//
//   4. 计时: start() 前记 now()，pool.shutdown() 后再记 now()
//
//   5. 填 BenchResult:
//      wall_sec = 计时差值（秒）
//      dropped  = queue.dropped_count()
//      processed = tracker.sample_count()
//      stats    = tracker.snapshot()
// ------------------------------------------------------------
// --- 你的代码 ---
BenchResult run_benchmark(Policy policy, int rate) {
    const int N = rate * RUN_SECONDS;  // 每档固定约 10 秒的数据量

    // 1. 创建组件
    MessageQueue<PipelineItem> queue(QUEUE_CAPACITY, policy);
    ThreadPool pool(NUM_WORKERS);
    LatencyTracker tracker;
    MockDataGenerator gen(queue, N, rate);

    // 2. Worker Loop
    auto worker_func = [&] {
        while (true) {
            PipelineItem item;
            if (!queue.dequeue(item)) break;
            item.latency.dequeued_at = steady_clock::now();   // T2: 取出
            busy_wait_micro(PROCESS_US);                       // 模拟处理
            item.latency.processed_at = steady_clock::now();  // T3: 处理完
            tracker.record(item.latency);
        }
    };

    // 3. 启动与收尾
    auto start_time = steady_clock::now();
    pool.start(worker_func);
    gen.start();
    gen.join();
    // 注意: gen.join() 返回时 generator 的 run() 已经调过 queue.shutdown()，
    // 这里不需要重复调用
    pool.shutdown();
    auto end_time = steady_clock::now();

    // 4. 填 BenchResult
    BenchResult result;
    result.policy_name = policy_to_string(policy);
    result.rate = rate;
    result.wall_sec = duration<double>(end_time - start_time).count();
    result.dropped = queue.dropped_count();
    result.processed = tracker.sample_count();
    result.stats = tracker.snapshot();

    return result;
}

// ------------------------------------------------------------
// TODO 3: 打印一行报表
//
// 用 std::setw 对齐列宽，格式参考:
//   Block      1000    12.34s   dropped=0      processed=50000
//     queue_wait  P50=xx P95=xx P99=xx us
//     processing  P50=xx P95=xx P99=xx us
//     end_to_end  P50=xx P95=xx P99=xx us   throughput=xxx msg/s
// ------------------------------------------------------------
// --- 你的代码 ---
// ------------------------------------------------------------

void print_result(const BenchResult& r) {
    const auto& s = r.stats;

    // 第一行：策略名、速率、挂钟时间、丢弃数、处理数
    std::cout << std::fixed << std::setprecision(2);
    std::cout << std::left << std::setw(12) << r.policy_name
              << std::right << std::setw(6) << r.rate
              << "  " << std::setw(8) << r.wall_sec << "s"
              << "  dropped=" << std::setw(6) << r.dropped
              << "  processed=" << std::setw(6) << r.processed << "\n";

    // 第二行：排队延迟 P50/P95/P99
    std::cout << "    queue_wait  P50=" << std::setw(6) << s.queue_wait_p50_us
              << "  P95=" << std::setw(6) << s.queue_wait_p95_us
              << "  P99=" << std::setw(6) << s.queue_wait_p99_us << " us\n";

    // 第三行：处理延迟 P50/P95/P99
    std::cout << "    processing  P50=" << std::setw(6) << s.processing_p50_us
              << "  P95=" << std::setw(6) << s.processing_p95_us
              << "  P99=" << std::setw(6) << s.processing_p99_us << " us\n";

    // 第四行：端到端延迟 P50/P95/P99 和吞吐量
    std::cout << "    end_to_end  P50=" << std::setw(6) << s.end_to_end_p50_us
              << "  P95=" << std::setw(6) << s.end_to_end_p95_us
              << "  P99=" << std::setw(6) << s.end_to_end_p99_us << " us"
              << "  throughput=" << std::setprecision(0) << std::setw(8) << s.throughput_msg_per_sec << " msg/s\n";
}

int main() {
    // TODO 4: 3×3 双层循环
    // rates = {1000, 5000, 10000}
    // policies = {Block, DropOldest, DropNewest}
    // 每组调 run_benchmark 并打印
    // --- 你的代码 ---
    
    
    std::vector<int> rates = {1000, 5000, 10000};
    std::vector<Policy> policies = {
        Policy::Block,
        Policy::DropOldest,
        Policy::DropNewest
    };

    // 打印表头
    std::cout << "===== Pipeline Benchmark (M1) =====\n";
    std::cout << "Messages per run: rate x " << RUN_SECONDS << "s"
              << " (10000 / 50000 / 100000)\n";
    std::cout << "Queue capacity: " << QUEUE_CAPACITY << "\n";
    std::cout << "Workers: " << NUM_WORKERS << "\n";
    std::cout << "Simulated processing: " << PROCESS_US << " us\n";
    std::cout << "Consumer rate ≈ " << NUM_WORKERS << " / " << PROCESS_US
              << "us ≈ " << (NUM_WORKERS * 1'000'000 / PROCESS_US) << " msg/s\n\n";

    // 双层循环
    for (auto policy : policies) {
        for (int rate : rates) {
            BenchResult result = run_benchmark(policy, rate);
            print_result(result);
            std::cout << std::endl;   // 每组之间空一行
        }
    }

    std::cout << "Benchmark finished.\n";
    return 0;
}

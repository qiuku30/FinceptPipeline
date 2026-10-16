#include <cassert>
#include <chrono>
#include <iostream>
#include <set>
#include <thread>
#include <vector>

#include "core/MockDataGenerator.h"

// ============================================================
// 测试 1: 完整生成 — 不丢不重
//
// 数据流：
//   MockDataGenerator ──→ MessageQueue ──→ 主线程(Consumer)
//
// 验证：
//   1. generated_count() == N（生成器真的放了 N 条）
//   2. Consumer 收到的 id 数量 == N 且无重复
//   3. is_running() == false（正常完成也要把状态改掉）
// ============================================================
void test_full_generation() {
    constexpr uint64_t N = 1000;
    constexpr double RATE = 10000;  // 10000 msg/s → 1000 条约 0.1 秒跑完

    MessageQueue<PipelineItem> queue(50);  // 容量小于 N，制造阻塞
    MockDataGenerator gen(queue, N, RATE);

    // TODO 1: 启动生成器，检查 is_running()
    // --- 你的代码 ---
    gen.start();
    assert(gen.is_running());
    // ============================================================

    // TODO 2: 主线程当 Consumer
    // - 循环 dequeue（用 bool dequeue(T&) 模式）
    // - 收集所有 id 到 vector
    // - dequeue 返回 false 就退出（generator 放完后会 queue.shutdown()）
    // --- 你的代码 ---
    std::vector<uint64_t> collected;
    while (true) {
        PipelineItem item;
        if (!queue.dequeue(item)) break;
        collected.push_back(item.data.id);
    }
    // ============================================================

    // TODO 3: join 等生成线程结束，然后验证
    // - gen.join()
    // - generated_count() == N
    // - is_running() == false   ← 验证你刚修的 bug
    // - collected.size() == N，无重复（std::set）
    // - id 范围 [0, N)
    // --- 你的代码 ---
    gen.join();
    assert(gen.generated_count() == N);
    assert(!gen.is_running());
    assert(collected.size() == N);
    std::set<uint64_t> unique(collected.begin(), collected.end());
    assert(unique.size() == N);
    assert(*unique.begin() == 0);
    assert(*unique.rbegin() == N - 1);
    // ============================================================

    std::cout << "[PASS] test_full_generation (" << N << " msgs @ "
              << RATE << " msg/s)\n";
}

// ============================================================
// 测试 2: 速率基本正确
//
// 1000 条 @ 1000 msg/s → 理论上约 1 秒
// 实际耗时用 steady_clock 测，容差放宽到 [0.8s, 2.0s]
// ============================================================
void test_rate() {
    constexpr uint64_t N = 1000;
    constexpr double RATE = 1000;  // 1 条/ms → 总共约 1 秒

    MessageQueue<PipelineItem> queue(10000);  // 容量大，避免阻塞影响计时
    MockDataGenerator gen(queue, N, RATE);

    // TODO 4: 计时跑一次
    // - start() 前记录 steady_clock::now()
    // - gen.start()
    // - gen.join()   ← join 返回时生成线程已经结束
    // - 再记录 now()，算出耗时
    // - 验证 0.8s < 耗时 < 2.0s
    // 提示: duration_cast<std::chrono::milliseconds>(end - start).count()
    // --- 你的代码 ---
    auto start_time = std::chrono::steady_clock::now();
    gen.start();
    gen.join();
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          end_time - start_time)
                          .count();
    assert(elapsed_ms > 800 && elapsed_ms < 2000);
    // ============================================================

    std::cout << "[PASS] test_rate (" << elapsed_ms << "ms for " << N
              << " msgs @ " << RATE << " msg/s)\n";
}

// ============================================================
// 测试 3: 提前 stop()
//
// 100000 条 @ 10 msg/s → 跑完要 10000 秒，不可能等
// 跑 500ms 后 stop()，验证提前退出
// ============================================================
void test_early_stop() {
    constexpr uint64_t N = 100000;
    constexpr double RATE = 10;

    MessageQueue<PipelineItem> queue(100000);  // 容量足够大，生成线程不会被 enqueue 卡住
    MockDataGenerator gen(queue, N, RATE);

    // TODO 5: start → sleep 500ms → stop → join
    // - 验证 generated_count() < N
    // - 验证 is_running() == false
    //
    // 思考题: 这里为什么 queue 容量给 100000 而不是 50？
    //   如果队列满了，生成线程卡在 enqueue() 里，
    //   stop() 无法立刻生效（我们之前讨论过的边界问题），
    //   而且没人消费队列，join() 可能等很久。
    //   容量给满，生成线程永远不需要等 → stop() 生效快。
    // --- 你的代码 ---
    gen.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    gen.stop();
    gen.join();
    assert(gen.generated_count() < N);
    assert(!gen.is_running());
    // ============================================================

    std::cout << "[PASS] test_early_stop (generated "
              << gen.generated_count() << " / " << N << ")\n";
}

int main() {
    test_full_generation();
    test_rate();
    test_early_stop();
    std::cout << "\nAll MockDataGenerator tests passed!\n";
    return 0;
}

#include <cassert>
#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "core/ThreadPool.h"

// ============================================================
// 测试 1: 基本启停
// ============================================================
void test_start_and_shutdown() {
    ThreadPool pool(4);

    // 最简单的 worker: 检查 should_stop() 然后退出
    // 注意: 如果 worker 不检查 should_stop()，shutdown() 会永远等不到它退出
    auto worker = [&pool] {
        // TODO: 循环直到 pool.should_stop() 为 true
        // 提示: 不要空转（busy-wait），用 std::this_thread::sleep_for 让出 CPU
        // --- 你的代码 ---
        while (!pool.should_stop()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    };

    pool.start(worker);
    assert(pool.is_running());

    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // 让 Worker 跑一会儿
    pool.shutdown();
    assert(!pool.is_running());

    std::cout << "[PASS] test_start_and_shutdown\n";
}

// ============================================================
// 测试 2: Worker 确实在并发执行
// 用多个线程对一个计数器 +1，验证最终值 = 线程数 × 每线程加的次数
// ============================================================
void test_workers_run_concurrently() {
    ThreadPool pool(4);
    std::mutex mtx;
    int counter = 0;
    constexpr int increments_per_worker = 1000;

    auto worker = [&] {
        // TODO: 循环 increments_per_worker 次，每次 counter++
        // 用 mtx 保护 counter
        // 完成后等待 should_stop()
        for (int i = 0; i < increments_per_worker; ++i) {
            // --- 你的代码 ---
            // 提示: std::lock_guard<std::mutex> lock(mtx);
            std::lock_guard<std::mutex> lock(mtx);
            ++counter;
        }

        // 等待 ThreadPool 通知关闭
        while (!pool.should_stop()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    };

    pool.start(worker);
    assert(pool.is_running());

    std::this_thread::sleep_for(std::chrono::milliseconds(200));  // 等 Worker 完成
    pool.shutdown();
    assert(!pool.is_running());

    assert(counter == 4 * increments_per_worker);
    std::cout << "[PASS] test_workers_run_concurrently (counter=" << counter << ")\n";
}

int main() {
    test_start_and_shutdown();
    test_workers_run_concurrently();
    std::cout << "\nAll ThreadPool tests passed!\n";
    return 0;
}

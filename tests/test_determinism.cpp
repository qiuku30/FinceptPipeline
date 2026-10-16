#include <cassert>
#include <chrono>
#include <iostream>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "core/MessageQueue.h"
#include "core/ThreadPool.h"
#include "core/Types.h"

// ============================================================
// 确定性测试：多线程处理结果 == 单线程处理结果
//
// 数据流：
//   Producer ──→ MessageQueue<TickData> ──→ ThreadPool(Worker Loop) ──→ collected
//
// Worker Loop 做的事（这就是"Worker Loop"）：
//   while (!pool.should_stop()) {
//       TickData msg;
//       if (!queue.dequeue(msg)) break;  // 队列关了就退出
//       // 处理 msg...
//   }
//
// 验证：
//   1. 收集到的 id 数量 == N（不丢）
//   2. 收集到的 id 无重复（不重）
//   3. 所有 id 都在 [0, N) 范围内（没有脏数据）
// ============================================================

void test_determinism() {
    constexpr int N = 10000;
    constexpr int NUM_WORKERS = 4;

    // ============================================================
    // TODO 1: 创建组件
    // - MessageQueue<TickData>（容量自定，比如 100）
    // - ThreadPool(NUM_WORKERS)
    // - 用于收集结果的容器（vector + mutex）
    // ============================================================
    // --- 你的代码 ---
    MessageQueue<PipelineItem> queue(100);
    ThreadPool pool(NUM_WORKERS);

    std::mutex collected_mutex;
    std::vector<uint64_t> collected;
    // ============================================================

    // ============================================================
    // TODO 2: 定义 Worker Loop
    //
    // 这是整个 M0 最核心的一段代码。
    // 每个 Worker 线程跑这个函数：
    //
    //   while (!pool.should_stop()) {
    //       TickData msg;
    //       if (!queue.dequeue(msg)) break;   // shutdown → 退出
    //       // 记下 msg.id（加锁保护 collected）
    //   }
    //
    // 提示：
    // - dequeue(msg) 返回 false = 队列已关且空，安全退出
    // - 写 collected 时用 std::lock_guard/std::unique_lock
    // ============================================================
    auto worker_func = [&] {
        // --- 你的代码 ---
        while (true) {
            PipelineItem item;
            if (!queue.dequeue(item)) break;
            // TODO: 把 item.data.id 记到 collected 里（加锁）
            std::lock_guard<std::mutex> lock(collected_mutex);
            collected.push_back(item.data.id);
        }
    };
    // ============================================================

    // ============================================================
    // TODO 3: 启动 Worker 们
    // - pool.start(worker_func)
    // - 检查 pool.is_running()
    // ============================================================
    // --- 你的代码 ---
    pool.start(worker_func);
    assert(pool.is_running());
    // ============================================================

    // ============================================================
    // TODO 4: Producer — 放 N 条 TickData，然后 shutdown 队列
    //
    // TickData 只需要填 id（0 到 N-1），其他字段不管。
    //
    // 注意顺序：
    //   1. 放完 N 条
    //   2. queue.shutdown()  ← 告诉 Worker "没数据了，取完就收工"
    //   3. pool.shutdown()    ← 告诉 Worker "你们也该停了"
    //
    // 为什么先关队列再关线程池？
    //   如果先关线程池，Worker 可能正卡在 dequeue() 里等待，
    //   而队列里还有没取完的数据 → 丢消息。
    //   先关队列：Worker 取完所有数据后，dequeue 返回 false → 退出循环，
    //   pool.shutdown() 只是收个尾。
    // ============================================================
    // --- 你的代码 ---
    for (int i = 0; i < N; ++i) {
        PipelineItem item;
        item.data.id = i;
        queue.enqueue(item);
    }
    queue.shutdown();
    // ============================================================

    // ============================================================
    // TODO 5: 关闭 ThreadPool，等所有 Worker 退出
    // ============================================================
    // --- 你的代码 ---
    pool.shutdown();
    // ============================================================

    // ============================================================
    // TODO 6: 验证结果（和 test_message_queue_mt 一样）
    // - collected.size() == N
    // - 用 std::set 检查无重复
    // - 检查 id 范围 [0, N)
    // ============================================================
    // --- 你的代码 ---
    assert(collected.size() == N);
    std::set<uint64_t> unique(collected.begin(), collected.end());
    assert(unique.size() == N);
    assert(*unique.begin() == 0);
    assert(*unique.rbegin() == N - 1);
    // ============================================================

    std::cout << "[PASS] test_determinism ("
              << N << " messages, " << NUM_WORKERS << " workers)\n";
}

int main() {
    test_determinism();
    std::cout << "\nAll determinism tests passed!\n";
    return 0;
}

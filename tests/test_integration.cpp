#include <cassert>
#include <iostream>
#include <mutex>
#include <set>
#include <vector>

#include "core/MessageQueue.h"
#include "core/MockDataGenerator.h"
#include "core/ThreadPool.h"
#include "core/Types.h"

// ============================================================
// M0 最终串联测试
//
// 数据流（第一次把三个模块完整接起来）：
//   MockDataGenerator ──→ MessageQueue ──→ ThreadPool(Worker Loop) ──→ collected
//
// 验证：N 条消息从生成到处理，不丢、不重、无脏数据
// ============================================================

void test_full_pipeline() {
    // ============================================================
    // TODO 1: 创建组件
    // - MessageQueue<TickData>，容量 200（故意小于 N，制造背压）
    // - ThreadPool，4 个 worker
    // - MockDataGenerator：N=10000 条，速率 20000 msg/s
    // - 收集容器：vector<uint64_t> + mutex
    // ============================================================
    // --- 你的代码 ---
    MessageQueue<PipelineItem> queue(200);
    ThreadPool pool(4);
    MockDataGenerator gen(queue, 10000, 20000.0);
    std::mutex collected_mutex;
    std::vector<uint64_t> collected;

    // ============================================================
    // TODO 2: Worker Loop（跟 test_determinism 一样的模式）
    // while (!pool.should_stop()) {
    //     TickData msg;
    //     if (!queue.dequeue(msg)) break;
    //     // 加锁把 msg.id 放进 collected
    // }
    // ============================================================
    // --- 你的代码 ---
    auto worker_func=[&]{
        while ( true )
        {
            PipelineItem item;
            if(!queue.dequeue(item)) break;
            std::lock_guard<std::mutex> lock(collected_mutex);
            collected.push_back(item.data.id);
        }
    };

    // ============================================================
    // TODO 3: 启动顺序
    // 1. pool.start(worker)   ← Worker 先就位
    // 2. gen.start()          ← 开始生产
    //
    // 想一想：为什么先 start Worker 再 start Generator？
    // ============================================================
    // --- 你的代码 ---
    pool.start(worker_func);
    gen.start();

    // ============================================================
    // TODO 4: 收尾顺序
    // 1. gen.join()      ← 等生产结束
    //                      （generator 的 run() 末尾会自动 queue.shutdown()）
    // 2. pool.shutdown() ← Worker 取完剩余数据后自然退出，这里收尾
    //
    // 想一想：如果反过来先 pool.shutdown() 再 gen.join() 会怎样？
    // ============================================================
    // --- 你的代码 ---
    gen.join();
    pool.shutdown();

    // ============================================================
    // TODO 5: 验证
    // - gen.generated_count() == 10000
    // - collected.size() == 10000
    // - 无重复（std::set）
    // - id 范围 [0, 10000)
    // - !gen.is_running() 和 !pool.is_running()
    // ============================================================
    // --- 你的代码 ---
    assert(gen.generated_count() == 10000);
    assert(collected.size() == 10000);
    std::set<uint64_t> unique_ids(collected.begin(), collected.end());
    assert(unique_ids.size() == 10000);
    for (const auto& id : collected) {
        assert(id >= 0 && id < 10000);
    }
    assert(!gen.is_running());
    assert(!pool.is_running());

    std::cout << "[PASS] test_full_pipeline (MockDataGenerator → "
              << "MessageQueue → ThreadPool)\n";
}

int main() {
    test_full_pipeline();
    std::cout << "\nAll integration tests passed!\n";
    return 0;
}

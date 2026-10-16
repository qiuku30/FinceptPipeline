#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>

#include "core/MessageQueue.h"

// ============================================================
// dequeue_for 行为验证
//
// 它存在的理由：CacheWriter 需要「等到有数据、或等够 flush_interval」。
// 所以测试必须覆盖"没数据时会超时返回"，而不只是"有数据能取到"。
//
// 四个用例：
//   ① 有数据        → 立刻返回 true（不该傻等满 timeout）
//   ② 空队列        → 等够 timeout 后返回 false，且 is_shutdown() 为假
//   ③ 等待中途来数据 → 提前被唤醒（不该死等到超时）
//   ④ 已关闭        → 立刻返回 false，且 is_shutdown() 为真
//
// ③ 是最重要的一条：它证明这是"等待"，不是"睡死过去"。
// ============================================================

using namespace std::chrono;

namespace {

void test_returns_immediately_when_data_available() {
    MessageQueue<int> q(10);
    q.enqueue(42);

    int v = 0;
    const auto t0 = steady_clock::now();
    const bool ok = q.dequeue_for(v, milliseconds(500));
    const auto elapsed = duration_cast<microseconds>(steady_clock::now() - t0);

    assert(ok);
    assert(v == 42);
    assert(elapsed < milliseconds(100)); // 数据已在，不该等满 500ms

    std::cout << "[PASS] ① 有数据立刻返回 (" << elapsed.count() << " μs)\n";
}

void test_times_out_when_empty() {
    MessageQueue<int> q(10);

    int v = 0;
    const auto t0 = steady_clock::now();
    const bool ok = q.dequeue_for(v, milliseconds(200));
    const auto elapsed = duration_cast<milliseconds>(steady_clock::now() - t0);

    assert(!ok);                  // 没取到
    assert(!q.is_shutdown());     // 不是因为关机 —— 是超时
    assert(elapsed >= milliseconds(190)); // 真的等够了
    assert(elapsed < milliseconds(500));  // 也没等过头

    std::cout << "[PASS] ② 空队列超时返回 (" << elapsed.count() << " ms)\n";
}

void test_wakes_up_early_when_data_arrives() {
    MessageQueue<int> q(10);

    int v = 0;
    bool ok = false;
    const auto t0 = steady_clock::now();

    std::thread consumer([&] { ok = q.dequeue_for(v, milliseconds(2000)); });

    std::this_thread::sleep_for(milliseconds(100));
    q.enqueue(7); // 100ms 时喂数据

    consumer.join();
    const auto elapsed = duration_cast<milliseconds>(steady_clock::now() - t0);

    assert(ok);
    assert(v == 7);
    assert(elapsed < milliseconds(1000)); // 提前唤醒，没死等到 2 秒

    std::cout << "[PASS] ③ 数据提前到达即被唤醒 (" << elapsed.count() << " ms)\n";
}

void test_shutdown_returns_false() {
    MessageQueue<int> q(10);
    q.shutdown();

    int v = 0;
    const auto t0 = steady_clock::now();
    const bool ok = q.dequeue_for(v, milliseconds(500));
    const auto elapsed = duration_cast<microseconds>(steady_clock::now() - t0);

    assert(!ok);
    assert(q.is_shutdown()); // ← 调用方靠这个区分「超时」和「关了」
    assert(elapsed < milliseconds(100)); // 关机应立刻返回

    std::cout << "[PASS] ④ 已关闭立刻返回，且 is_shutdown() 可区分\n";
}

} // namespace

int main() {
    test_returns_immediately_when_data_available();
    test_times_out_when_empty();
    test_wakes_up_early_when_data_arrives();
    test_shutdown_returns_false();
    std::cout << "\nAll dequeue_for tests passed!\n";
    return 0;
}

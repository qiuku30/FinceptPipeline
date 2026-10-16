#include <cassert>
#include <iostream>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "core/MessageQueue.h"

using Policy = MessageQueue<int>::BackpressurePolicy;

void smoke_test(Policy policy) {
    constexpr int PRODUCERS = 3;
    constexpr int CONSUMERS = 2;
    constexpr int PER_PRODUCER = 20000;
    constexpr int TOTAL = PRODUCERS * PER_PRODUCER;   // 60000
    constexpr size_t CAPACITY = 100;

    MessageQueue<int> queue(CAPACITY, policy);

    uint64_t dequeued = 0;  // 只在 collected_mutex 锁内修改，锁内读取
    std::mutex collected_mutex;
    std::vector<int> collected;
    collected.reserve(TOTAL);   // 预分配，减少重分配

    auto producer_func = [&](int start_id) {
        for (int i = 0; i < PER_PRODUCER; ++i) {
            int id = start_id + i;
            bool ok = queue.enqueue(id);

            if (policy == Policy::Block || policy == Policy::DropOldest) {
                assert(ok);
            }
        }
    };

    auto consumer_func = [&] {
        while (true) {
            int val;
            if (!queue.dequeue(val)) break;

            std::lock_guard<std::mutex> lock(collected_mutex);
            collected.push_back(val);
            ++dequeued;
        }
    };

    // 先启动 Consumer 就位，再启动 Producer（消费端先就绪）
    std::vector<std::thread> consumers;
    for (int i = 0; i < CONSUMERS; ++i) {
        consumers.emplace_back(consumer_func);
    }

    std::vector<std::thread> producers;
    for (int i = 0; i < PRODUCERS; ++i) {
        producers.emplace_back(producer_func, i * PER_PRODUCER);
    }

    for (auto& t : producers) t.join();
    queue.shutdown();  // 通知消费者没数据了
    for (auto& t : consumers) t.join();

    uint64_t dropped = queue.dropped_count();
    uint64_t total_dequeued = dequeued;
    std::cout << "  dropped=" << dropped << ", dequeued=" << total_dequeued
              << ", total=" << TOTAL << std::endl;

    // ① 守恒：入队总数（TOTAL） = 取走 + 丢弃
    assert(total_dequeued + dropped == TOTAL);

    // ② 无重复：collected 中 id 必须全局唯一
    std::set<int> unique_set(collected.begin(), collected.end());
    assert(unique_set.size() == collected.size());

    // ③ 策略特定断言
    if (policy == Policy::Block) {
        assert(dropped == 0);
    } else {  // DropOldest 或 DropNewest
        assert(dropped > 0);
    }

    std::cout << "[PASS] smoke_test for "
              << (policy == Policy::Block    ? "Block" :
                  policy == Policy::DropOldest ? "DropOldest" : "DropNewest")
              << std::endl;

}

int main() {
    std::cout << "=== Starting multi-threaded backpressure smoke tests ===\n";
    smoke_test(Policy::Block);
    smoke_test(Policy::DropOldest);
    smoke_test(Policy::DropNewest);
    std::cout << "\nAll smoke tests passed!\n";
    return 0;
}
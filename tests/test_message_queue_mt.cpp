#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
#include <mutex>
#include <set>

#include "core/MessageQueue.h"

// ============================================================
// 测试 1: 单 Producer 单 Consumer
// 目标: 验证基本线程安全 — N 条消息不丢不重
// ============================================================
void test_single_producer_single_consumer() {
    constexpr int N = 10000;
    MessageQueue<int> queue(100);  // 队列容量小于 N，测试阻塞逻辑

    // TODO 1: Consumer 线程
    // - 循环 dequeue()
    // - 把取到的数字 push_back 到 collected 里
    // - 退出条件: shutdown 来了 且 队列空
    // 提示: shutdown 之后，dequeue 返回值是什么？你需要检查一下
    std::vector<int> collected;
    std::thread consumer([&] {
        // --- 你的代码 ---
        // while (true) {
        //     int val = queue.dequeue();
        //     注意: shutdown 后 dequeue 返回 T()
        //     怎么区分"拿到真数据"和"shutdown 了"?
        //     一种方式: 先判断 is_shutdown() && queue.size()==0 → break
        // }
        while (true) {
            int val;
            if (!queue.dequeue(val)) break;
            collected.push_back(val);
        }
    });

    // Producer 线程
    std::thread producer([&] {
        for (int i = 0; i < N; ++i) {
            queue.enqueue(i);
        }
        queue.shutdown();  // 放完了，通知 Consumer 收工
    });

    producer.join();
    consumer.join();

    // TODO 2: 验证结果
    // - collected.size() 应该 == N
    // - 检查有没有重复数字（用 std::set）
    // - 检查数字范围（0 到 N-1）
    // --- 你的代码 ---

    assert(collected.size()==N);
    std::set<int> unique(collected.begin(),collected.end());
    assert(unique.size()==N);
    assert(*unique.begin()==0);
    assert(*unique.rbegin()==N-1);
    std::cout << "[PASS] test_single_producer_single_consumer ("
              << N << " messages)\n";
}

// ============================================================
// 测试 2: 多 Producer 多 Consumer
// 目标: 验证并发下无竞态、无数据丢失
// ============================================================
void test_multi_producer_multi_consumer() {
    constexpr int NUM_PRODUCERS = 3;
    constexpr int MSG_PER_PRODUCER = 5000;
    constexpr int TOTAL = NUM_PRODUCERS * MSG_PER_PRODUCER;
    constexpr int NUM_CONSUMERS = 2;

    MessageQueue<int> queue(500);

    // 提示: 多个 Consumer 共享一个 collected vector，需要锁保护
    std::mutex collected_mutex;
    std::vector<int> collected;

    // TODO 3: 启动 Consumer 线程们
    // - 每个 Consumer 做的事情和测试 1 一样
    // - 但往 collected 写时要用 collected_mutex 保护
    // 提示: 用 std::vector<std::thread> 管理线程
    std::vector<std::thread> consumers;
    // --- 你的代码 ---
    for(int i=0;i<NUM_CONSUMERS;i++)
    {
        consumers.emplace_back([&]{
            while (true) {
                int val;
                if (!queue.dequeue(val)) break;
                std::unique_lock<std::mutex> lock(collected_mutex);
                collected.push_back(val);
            }
        });
    }

    // TODO 4: 启动 Producer 线程们
    // - 每个 Producer 放 MSG_PER_PRODUCER 条
    // - 放完最后一个 Producer 调用 shutdown()
    // 小心: 不能每个 Producer 都 shutdown ! 只能最后一个 shutdown
    // --- 你的代码 ---
    std::vector<std::thread> producers;
    // --- 你的代码 ---
    for(int i=0;i<NUM_PRODUCERS;i++)
    {
        producers.emplace_back([&,i]{
            for(int j=0;j<MSG_PER_PRODUCER;j++)
                queue.enqueue(i*MSG_PER_PRODUCER+j);
        });
    }
    for (auto& t : producers) t.join();  // 等所有 Producer 都放完
    queue.shutdown();


    // 等待所有线程
    for (auto& t : consumers) t.join();

    // TODO 5: 验证
    // - collected.size() 应该 == TOTAL
    // - 无重复
    // --- 你的代码 ---
    assert(collected.size()==TOTAL);
    std::set<int> unique(collected.begin(),collected.end());
    assert(unique.size()==TOTAL);
    assert(*unique.begin()==0);
    assert(*unique.rbegin()==TOTAL-1);

    std::cout << "[PASS] test_multi_producer_multi_consumer ("
              << TOTAL << " messages, "
              << NUM_PRODUCERS << "P + " << NUM_CONSUMERS << "C)\n";
}

int main() {
    test_single_producer_single_consumer();
    test_multi_producer_multi_consumer();
    std::cout << "\nAll multi-threaded tests passed!\n";
    return 0;
}

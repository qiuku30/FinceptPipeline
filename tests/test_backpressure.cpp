#include <cassert>
#include <iostream>
#include <vector>

#include "core/MessageQueue.h"

// ------------------------------------------------------------
// 测试 1：DropOldest —— 丢最旧的
// 容量 10，塞 0~99，预期队列剩 90~99，dropped_count == 90
// ------------------------------------------------------------
void test_drop_oldest() 
{
    const size_t CAPACITY=10;
    const size_t N=100;

    MessageQueue<int> queue(CAPACITY,MessageQueue<int>::BackpressurePolicy::DropOldest);

    for(int i=0;i<N;i++)
    {
        bool ok=queue.enqueue(i);
        assert(ok);
    }

    assert(queue.dropped_count()==N-CAPACITY);

    std::vector<int> check;
    int val;
    while(queue.try_dequeue(val))
    {
        check.push_back(val);
    }

    assert(check.size()==CAPACITY);
    for(size_t i=0;i<CAPACITY;++i)
    {
        assert(check[i]==static_cast<int> (90+i));
    }

    std::cout << "[PASS] test_drop_oldest\n";

}

// ------------------------------------------------------------
// 测试 2：DropNewest —— 丢最新的
// 容量 10，塞 0~99，预期队列剩 0~9，dropped_count == 90
// ------------------------------------------------------------
void test_drop_newest() {
    const size_t CAPACITY = 10;
    const int N = 100;

    MessageQueue<int> queue(CAPACITY,
                            MessageQueue<int>::BackpressurePolicy::DropNewest);

    int drop_expect=0;
    for (int i=0;i<N;i++)
    {
        bool ok=queue.enqueue(i);
        if(!ok) drop_expect++;
    }

    assert(queue.dropped_count()==static_cast<uint64_t>(drop_expect));
    assert(drop_expect==N-CAPACITY);

    std::vector<int> check;
    int val;
    while(queue.try_dequeue(val))
    {
        check.push_back(val);
    }

    assert(check.size()==CAPACITY);
    for(size_t i=0;i<CAPACITY;++i)
    {
        assert(check[i]==static_cast<int> (i));
    }

    std::cout << "[PASS] test_drop_newest\n";


    }

// ------------------------------------------------------------
// 测试 3：shutdown 语义
// shutdown 后 enqueue 返回 false，且 dropped_count 不再增长
// ------------------------------------------------------------
void test_shutdown_semantics() {
    const size_t CAPACITY = 5;

    MessageQueue<int> queue(CAPACITY,
                            MessageQueue<int>::BackpressurePolicy::DropOldest);

    // 塞满队列
    for (int i = 0; i < static_cast<int>(CAPACITY); ++i) {
        assert(queue.enqueue(i));
    }
    assert(queue.size() == CAPACITY);

    uint64_t before = queue.dropped_count();
    assert(before == 0);

    // shutdown
    queue.shutdown();

    // 再 enqueue —— 返回 false，且 dropped_count 不变
    bool ok = queue.enqueue(999);
    assert(!ok);
    assert(queue.dropped_count() == before);

    // 队列里的元素不变（0~4）
    std::vector<int> check;
    int val;
    while (queue.try_dequeue(val)) {
        check.push_back(val);
    }
    assert(check.size() == CAPACITY);
    for (size_t i = 0; i < CAPACITY; ++i) {
        assert(check[i] == static_cast<int>(i));
    }

    std::cout << "[PASS] test_shutdown_semantics\n";
}

// ------------------------------------------------------------
// main
// ------------------------------------------------------------
int main() {
    test_drop_oldest();
    test_drop_newest();
    test_shutdown_semantics();

    std::cout << "\nAll backpressure tests passed!\n";
    return 0;
}


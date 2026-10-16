#include <cassert>
#include <iostream>
#include <thread>

#include "core/MessageQueue.h"

void test_create_and_push() {
    MessageQueue<int> queue(10);

    assert(queue.size() == 0);
    assert(queue.capacity() == 10);

    queue.enqueue(42);
    assert(queue.size() == 1);

    int val = queue.dequeue();
    assert(val == 42);
    assert(queue.size() == 0);

    std::cout << "[PASS] test_create_and_push\n";
}

void test_try_dequeue_empty() {
    MessageQueue<int> queue(10);
    int val = -1;
    bool ok = queue.try_dequeue(val);
    assert(!ok);          // 队列为空应该返回 false
    assert(val == -1);     // val 不应被修改
    std::cout << "[PASS] test_try_dequeue_empty\n";
}

int main() {
    test_create_and_push();
    test_try_dequeue_empty();
    std::cout << "\nAll tests passed!\n";
    return 0;
}

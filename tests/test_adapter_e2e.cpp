#include <cassert>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <set>

#include "adapters/AsyncReceiver.h"
#include "adapters/MockWebSocketClient.h"
#include "core/IPublisher.h"
#include "core/LatencyTracker.h"
#include "core/MessageQueue.h"
#include "core/ThreadPool.h"
#include "core/Types.h"

// ============================================================
// 端到端 mock 测试 — M2 第一步的验收
//
// 完整数据流（所有组件第一次全部接上）:
//   MockWebSocketClient ──→ AsyncReceiver ──→ MessageQueue
//     (收包线程发回调)      (盖 T0 入队)        ↓
//                            MockPublisher ←─ ThreadPool Workers
//                           (收集发布结果)    (盖 T2/T3 → record → publish)
//
// 验证:
//   1. N 条消息一条不丢（publisher 收到 N 条）
//   2. payload 内容正确（"tick i" 对应 id i）
//   3. 时间戳完整（T0 ≤ T2 ≤ T3）
//   4. disconnect 后正在途中的消息也被处理完（Mock 的排空语义）
// ============================================================

// ------------------------------------------------------------
// TODO 1: 测试本地 MockPublisher
//
// 实现 IPublisher，publish() 把 TickData 收集进 vector（加锁）。
// 同时把 TickData 的 id/payload 记录下来方便断言。
// ------------------------------------------------------------
// --- 你的代码 ---
class MockPublisher : public IPublisher {
public:
    void publish(const TickData& data) override {
        std::lock_guard<std::mutex> lock(mutex_);
        collected_.push_back(data);
    }

    std::vector<TickData> snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return collected_;   // 返回拷贝，调用者拿到的是安全快照
    }

    size_t count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return collected_.size();
    }

private:
    mutable std::mutex mutex_;
    std::vector<TickData> collected_;
};

// ------------------------------------------------------------
// TODO 2: 端到端测试主流程
//
// 1. 创建组件:
//    MessageQueue<PipelineItem> queue(100);
//    auto client = make_shared<MockWebSocketClient>();
//    AsyncReceiver receiver(client, queue);
//    ThreadPool pool(4);
//    LatencyTracker tracker;
//    MockPublisher publisher;
//
// 2. Worker Loop（升级版——拆包发布，不再是收集 id）:
//    while (true) {
//        PipelineItem item;
//        if (!queue.dequeue(item)) break;
//        item.latency.dequeued_at  = now();   // T2
//        busy_wait 或 sleep 模拟处理;
//        item.latency.processed_at = now();   // T3
//        tracker.record(item.latency);
//        publisher.publish(item.data);        // ← 只有数据走出去
//    }
//
// 3. 启动顺序: pool.start → receiver.connect_to("mock://test")
//    （想一想: 为什么 Worker 先于 receiver 就位？）
//
// 4. 注入 N 条: client->push_message("tick i")
//
// 5. 收尾顺序: receiver.disconnect() → pool.shutdown()
//    （receiver.disconnect 内部会 queue.shutdown，
//     所以这里不需要手动 shutdown 队列）
//
// 6. 验证（TODO 3）
// ------------------------------------------------------------
void test_adapter_e2e() {
    // --- 你的代码 ---
    constexpr int N = 1000;
    constexpr size_t QUEUE_CAP = 100;

    // ---------- 1. 创建组件 ----------
    MessageQueue<PipelineItem> queue(QUEUE_CAP);
    auto client = std::make_shared<MockWebSocketClient>();
    AsyncReceiver receiver(client, queue);
    ThreadPool pool(4);
    LatencyTracker tracker;
    MockPublisher publisher;

    // ---------- 2. Worker Loop ----------
    auto worker_func = [&]() {
        while (true) {
            PipelineItem item;
            if (!queue.dequeue(item)) break;   // 队列关闭且空 → 退出

            // T2: 出队时间
            item.latency.dequeued_at = std::chrono::steady_clock::now();

            // 模拟处理：忙等 10us（避免 sleep 的调度抖动）
            auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::microseconds(10);
            volatile int dummy = 0;
            while (std::chrono::steady_clock::now() < deadline) { dummy=dummy+1; }
            (void)dummy;

            // T3: 处理完成
            item.latency.processed_at = std::chrono::steady_clock::now();

            // 时间戳完整性断言：整条链 T0 ≤ T1 ≤ T2 ≤ T3
            //
            // ⚠️ 链必须【完整】。历史事故：T1 曾经完全没被赋值
            //    （steady_clock::time_point 默认构造 = epoch 0），
            //    而当时只断言了 T0≤T2≤T3，正好跳过 T1 —— bug 一直没被发现，
            //    queue_wait 统计出来是"开机至今"（约 43 天）。
            //    直到接真实数据源时把完整统计打出来才暴露。
            assert(item.latency.received_at <= item.latency.enqueued_at);   // T0 ≤ T1
            assert(item.latency.enqueued_at <= item.latency.dequeued_at);   // T1 ≤ T2
            assert(item.latency.dequeued_at <= item.latency.processed_at);  // T2 ≤ T3

            // 上报延迟 + 发布数据
            tracker.record(item.latency);
            publisher.publish(item.data);
        }
    };

    // ---------- 3. 启动顺序：Worker 先就位 ----------
    pool.start(worker_func);
    receiver.connect_to("mock://test");

    // ---------- 4. 注入 N 条消息 ----------
    for (int i = 0; i < N; ++i) {
        bool ok = client->push_message("tick " + std::to_string(i));
        assert(ok);   // 未 disconnect，必须全部接受
    }

    // ---------- 5. 收尾 ----------
    receiver.disconnect();   // 内部会 client_->disconnect() + queue_.shutdown()
    pool.shutdown();

    // ---------- 6. 验证 ----------
    auto collected = publisher.snapshot();

    // ① 不丢：publisher 收到 N 条
    assert(collected.size() == N);

    // ② 内容正确 + 无重复 id
    std::set<uint64_t> ids;
    for (const auto& data : collected) {
        ids.insert(data.id);
        // payload 应该与 id 对应："tick <id>"
        std::string expected = "tick " + std::to_string(data.id);
        assert(data.payload == expected);
    }
    assert(ids.size() == N);   // id 全局唯一

    // ③ 时间戳完整（tracker 记录了 N 条）
    assert(tracker.snapshot().total_processed == N);

    std::cout << "[PASS] test_adapter_e2e\n";
}

// ------------------------------------------------------------
// TODO 3: 验证逻辑（写进 TODO 2 的末尾）
//
// - publisher 收到 N 条（不丢）
// - 每条 payload 与 id 对得上（内容正确，且顺序可能乱——并发处理）
//   提示: 用 std::set 或排序后比较，别假设顺序
// - 每条时间戳 T0 ≤ T2 ≤ T3（可选加强验证）
// - tracker.snapshot().total_processed == N（时间戳没丢）
// ------------------------------------------------------------



// ------------------------------------------------------------
// 负向用例：队列先关，再推消息 → 计数器应该 +1
//
// 注意：这个场景在当前 E2E 拓扑下永远不会发生
//（因为 disconnect 保证先 join 再 shutdown），
// 所以必须人为制造"队列先关"的顺序来验证计数器。
// ------------------------------------------------------------
void test_rejected_when_queue_closed() {
    MessageQueue<PipelineItem> queue(100);
    auto client = std::make_shared<MockWebSocketClient>();
    AsyncReceiver receiver(client, queue);

    // ① 启动收包线程（此时队列还没关）
    receiver.connect_to("mock://test");

    // ② 人为制造"队列先关"：手动关掉队列
    //    注意：这不影响 client 的 accepting_，
    //    所以下一步 push_message 仍然能推进 pending_
    queue.shutdown();

    // ③ 推一条消息（accepting_ 还是 true，能进 pending_）
    bool ok = client->push_message("tick-rejected");
    assert(ok);

    // ④ 用 disconnect 的 join 作为同步点
    //    disconnect 内部会 client_->disconnect() → join 收包线程
    //    join 返回时保证所有 on_raw_message 已执行完毕
    receiver.disconnect();

    // ⑤ 断言：这条消息因为队列已关闭而被拒绝
    assert(receiver.rejected_count() == 1);

    std::cout << "[PASS] test_rejected_when_queue_closed\n";
}

int main() {
    test_adapter_e2e();
    test_rejected_when_queue_closed();
    std::cout << "\nAll adapter E2E tests passed!\n";
    return 0;
}

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>

#include <QCoreApplication>

#include "adapters/AsyncReceiver.h"
#include "adapters/KrakenWebSocketClient.h"
#include "core/IPublisher.h"
#include "core/LatencyTracker.h"
#include "core/MessageQueue.h"
#include "core/ThreadPool.h"
#include "core/Types.h"

// ============================================================
// 真实行情 × 完整管道 —— M2 的端到端验收
//
//   Kraken ──→ AsyncReceiver ──→ MessageQueue ──→ Workers ──→ Publisher
//   (真实网络)  (盖 T0 入队)      (传送带)       (盖 T2/T3)   (收集)
//
// 和 test_adapter_e2e.cpp 的**唯一区别**：数据源从 Mock 换成 Kraken。
// 其余结构一字未改 —— 这正是"名片"设计的验证。
//
// 【运行】需要 VPN 开着；cd build && ./test_kraken_live
//         （故意不注册进 ctest：依赖网络、耗时长、结果不确定）
//
// ── 验收 ────────────────────────────────────────────────────
//   1. 真实行情流过整条管道（publisher 收到 N 条）
//   2. T0~T3 时间戳齐全、单调
//   3. src/core/ 零改动
// ============================================================

namespace {

// 收集发布结果 —— 和 test_adapter_e2e.cpp 里的 MockPublisher 同一套
class CollectingPublisher : public IPublisher {
public:
    void publish(const TickData& tick) override {
        std::lock_guard<std::mutex> lock(mutex_);
        ++count_;
        last_payload_ = tick.payload;
    }

    size_t count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return count_;
    }

    std::string last_payload() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return last_payload_;
    }

private:
    mutable std::mutex mutex_;
    size_t count_ = 0;
    std::string last_payload_;
};

}  // namespace

int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);

    constexpr size_t TARGET    = 30;                    // 收够这么多条就收工
    constexpr size_t WORKERS   = 4;
    constexpr size_t QUEUE_CAP = 1000;
    constexpr auto   MAX_WAIT  = std::chrono::seconds(120);

    // ── 组件：和 test_adapter_e2e 一一对应，只换了数据源 ──
    MessageQueue<PipelineItem> queue(QUEUE_CAP);
    auto client = std::make_shared<KrakenWebSocketClient>();
    AsyncReceiver receiver(client, queue);
    ThreadPool pool(WORKERS);
    LatencyTracker tracker;
    CollectingPublisher publisher;

    // 主线程等"收够了"用的同步点
    std::mutex mtx;
    std::condition_variable cv;

    // ── Worker Loop ──
    //    跑在 ThreadPool 的工作线程上，和 Mock 版完全一样：
    //    dequeue → 盖 T2 → 处理 → 盖 T3 → 上报 → 发布
    auto worker = [&]() {
        while (true) {
            PipelineItem item;
            if (!queue.dequeue(item)) break;              // 队列关了且取空 → 收工

            item.latency.dequeued_at  = std::chrono::steady_clock::now();   // T2
            item.latency.processed_at = std::chrono::steady_clock::now();   // T3

            tracker.record(item.latency);
            publisher.publish(item.data);

            if (publisher.count() >= TARGET) {
                cv.notify_one();                          // 叫醒主线程
            }
        }
    };

    // ── 启动顺序：Worker 先就位，再连数据源 ──
    //    （和 Mock 版同一个理由：避免消息先涌进队列却没人消费）
    pool.start(worker);
    std::cout << "[main] 正在连接 wss://ws.kraken.com ...\n";
    receiver.connect_to("wss://ws.kraken.com");

    // ── 等收够 TARGET 条 ──
    //
    // ★ 顺带注意：这个 lambda 由 AsyncReceiver 转发，
    //   跑在 KrakenWebSocketClient 的【收包线程】上，不是主线程。
    //   所以 AsyncReceiver::on_raw_message 里的 queue_.enqueue
    //   是真正的跨线程入队 —— 这正是 M2 要验的东西。
    {
        std::unique_lock<std::mutex> lock(mtx);
        const bool enough = cv.wait_for(lock, MAX_WAIT, [&] {
            return publisher.count() >= TARGET;
        });
        if (!enough) {
            std::cout << "[main] ⚠️ 超时（" << MAX_WAIT.count() << "s），只收到 "
                      << publisher.count() << " 条\n";
        }
    }
    std::cout << "[main] 已收 " << publisher.count() << " 条，开始收尾\n";

    // ── 收尾顺序（和 Mock 版一字不差）──
    //    receiver.disconnect() 内部：client_->disconnect() 排空 + join
    //                                然后才 queue_.shutdown()
    //    再 pool.shutdown() 让 Worker 把队列取空后退出
    receiver.disconnect();
    pool.shutdown();
    std::cout << "[main] 管道已干净关闭\n";

    // ── 统计 ──
    const auto s = tracker.snapshot();
    std::cout << "\n═══════════ 结果 ═══════════\n";
    std::cout << "  发布总数        : " << publisher.count() << "\n";
    std::cout << "  tracker 样本数  : " << s.total_processed << "\n";
    std::cout << "  ── 端到端 (T0→T3) ──\n";
    std::cout << "    P50 / P95 / P99 : " << s.end_to_end_p50_us << " / "
              << s.end_to_end_p95_us << " / " << s.end_to_end_p99_us << " us\n";
    std::cout << "    min / avg / max : " << s.min_us << " / " << s.avg_us
              << " / " << s.max_us << " us\n";
    std::cout << "  ── 排队等待 (T1→T2) ──\n";
    std::cout << "    P50 / P95 / P99 : " << s.queue_wait_p50_us << " / "
              << s.queue_wait_p95_us << " / " << s.queue_wait_p99_us << " us\n";
    std::cout << "  最后一条报文     : "
              << publisher.last_payload().substr(0, 100) << " ...\n";
    std::cout << "════════════════════════════\n";

    return 0;
}

#pragma once

#include <atomic>
#include <memory>
#include <string>

#include "adapters/IWebSocketClient.h"
#include "core/MessageQueue.h"
#include "core/Types.h"

// ============================================================
// AsyncReceiver — 收包端适配器
//
// 职责：把 WebSocket 的原始消息翻译成 PipelineItem 送入队列。
// 回调在客户端的收包线程上执行（Qt 模型同理），
// 因此 enqueue 是跨线程的——MessageQueue 的线程安全在此发挥作用。
//
// 数据流：
//   WebSocket 收到 "raw" ──→ 回调 ──→ 盖 T0 ──→ enqueue
//
// 注意（M2 范围的已知边界）：
//   当前 disconnect() 会 queue.shutdown()，意味着"一次连接对应一个队列"。
//   断线重连需要新队列——M2 真接入时再处理。
// ============================================================
class AsyncReceiver {
public:
    AsyncReceiver(std::shared_ptr<IWebSocketClient> client,
                  MessageQueue<PipelineItem>& queue)
        : client_(client), queue_(queue) {}

    // 禁止拷贝
    AsyncReceiver(const AsyncReceiver&) = delete;
    AsyncReceiver& operator=(const AsyncReceiver&) = delete;

    // ------------------------------------------------------------
    // TODO 1: connect_to
    // 1. 设置回调（核心翻译逻辑，见 TODO 2）
    // 2. client_->connect(url)
    // 3. connected_ = true
    // ------------------------------------------------------------
    void connect_to(const std::string& url)
    {
        client_->set_on_message([this](const std::string& raw) {
            on_raw_message(raw);
        });
        client_->connect(url);
        connected_ = true;
    }

    // ------------------------------------------------------------
    // TODO 3: disconnect
    // 顺序:
    //   1. client_->disconnect()   ← 停止收包（Mock 保证已注入的消息派发完）
    //   2. queue_.shutdown()       ← 宣布"不再有新数据"，Worker 取完收工
    //   3. connected_ = false
    // 想一想: 为什么是这个顺序？(和 generator 放完→queue.shutdown 同一课)
    // ------------------------------------------------------------
    void disconnect()
    {
        client_->disconnect();
        queue_.shutdown();
        connected_ = false;
    }

    bool is_connected() const { return connected_; }

    uint64_t rejected_count() const { return rejected_.load(std::memory_order_relaxed); }

private:
    // ------------------------------------------------------------
    // TODO 2: 回调逻辑（connect_to 里设置，在收包线程上执行）
    //
    // void on_raw_message(const std::string& raw):
    //   1. 构造 PipelineItem
    //   2. item.data.payload = raw        ← 原始报文放 payload
    //      item.data.id = ?（提示: 接收方自增序号，用成员原子计数器）
    //      item.data.topic = ?（mock 阶段没有真实 topic，填空串）
    //   3. item.latency.received_at = now()   ← T0 在这盖
    //   4. queue_.enqueue(item)
    // ------------------------------------------------------------
    void on_raw_message(const std::string& raw)
    {
        PipelineItem item;
        item.data.payload = raw;
        item.data.id = next_id_.fetch_add(1, std::memory_order_relaxed);
        item.data.topic = "";
        item.latency.received_at = std::chrono::steady_clock::now();   // T0
        // T1 必须在这里盖，而且要在 enqueue 【之前】：
        //   enqueue 在队列满时会阻塞（Block 策略），
        //   盖在前面，"排队等待"才包含被背压卡住的时间——
        //   那正是收包线程真实经历的东西。
        item.latency.enqueued_at = std::chrono::steady_clock::now();   // T1
        // ✅ 接住返回值，失败时计数
        if (!queue_.enqueue(std::move(item))) {
        rejected_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    std::shared_ptr<IWebSocketClient> client_;
    MessageQueue<PipelineItem>& queue_;
    std::atomic<bool> connected_{false};
    std::atomic<uint64_t> next_id_{0};   // 接收序号
    std::atomic<uint64_t> rejected_{0};  // enqueue 失败计数
};

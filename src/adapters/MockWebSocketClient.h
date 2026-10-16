#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

#include "adapters/IWebSocketClient.h"

// ============================================================
// MockWebSocketClient — 模拟真实 WebSocket 客户端
//
// 行为模型（模仿真实 Qt 客户端）：
//   1. connect() 之后内部启动一个"收包线程"
//   2. 测试用 push_message() 注入消息（线程安全）
//   3. 收包线程把消息一条条派发给回调（在收包线程上执行！）
//   4. disconnect() 停止派发
//
// 这本身就是一个迷你 Producer-Consumer：
//   测试线程 push_message  →  内部 pending_ 队列  →  收包线程取出发回调
// ============================================================
class MockWebSocketClient : public IWebSocketClient {
public:
    MockWebSocketClient() = default;
    ~MockWebSocketClient()
    {
        // 确保析构前停止收包线程
    if (thread_.joinable()) {
        disconnect();   // disconnect 内部会 join 线程
    }
    }

    // TODO 1: set_on_message — 存下回调
    // 注意: 回调要在"收包线程"上被调用，所以这里只保存
    void set_on_message(std::function<void(const std::string&)> cb) override
    {
        on_message_ = std::move(cb);
    }


    // TODO 2: connect — 记录 URL，启动收包线程
    // 线程执行 run()（见下）
    void connect(const std::string& url) override
    {
        url_ = url;
        thread_ = std::thread([this]() { run(); });
    }

    // ------------------------------------------------------------
    // TODO 3: disconnect — 优雅停止
    //
    // 关键语义（和 queue.shutdown 同一课）：
    //   "断开"之后，已经注入但还没派发的消息怎么办？
    //   真实 WebSocket 断开时，缓冲里的数据也不会凭空消失。
    //   所以: 先停止接收新消息，再把 pending_ 里剩余的全部派发完，
    //   最后才结束线程。
    //
    // 实现提示:
    //   1. accepting_ = false（停止接收新 push）
    //   2. 设置停止信号并 notify 收包线程
    //   3. join 收包线程
    //   收包线程的退出条件要写成:
    //     "没有新消息可收了 且 pending_ 已空" → 退出
    // ============================================================
    void disconnect() override
    {
        {
        std::lock_guard<std::mutex> lock(mutex_);
        accepting_ = false;   // ① 先关入口：push_message 立刻返回 false
        stopping_ = true;     // ② 再发停止信号：收包线程可以准备退出了
        }
        cv_.notify_one();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    // 测试专用：注入一条消息（线程安全，任意线程可调）
    // TODO 4: push_message
    // - 若 accepting_ == false 返回 false（断开后拒绝注入）
    // - 加锁 push 进 pending_，notify 收包线程，返回 true
    bool push_message(const std::string& msg)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(!accepting_) {
            return false;
        }
    
        pending_.push(msg);
        cv_.notify_one();
        return true;
    }

private:
    // TODO 5: run — 收包线程主函数
    //
    // while (true) {
    //     加锁等待: pending_ 非空 或 该收工了
    //     if (该收工了 且 pending_ 空) break;
    //     取出队首（在锁外调回调！回调里可能再调用本对象的方法）
    //     调用 on_message_(msg)
    // }
    //
    // 注意: 回调必须在锁外调用，否则回调里调用 push_message 会死锁
    void run()
    {
        while (true) {
            std::string msg;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this]() { return !pending_.empty() || stopping_; });

                if (stopping_ && pending_.empty()) {
                    break;
                }
                

                msg = std::move(pending_.front());
                pending_.pop();
            }

            if (on_message_) {
                on_message_(msg);
            }
        }
    }

    std::function<void(const std::string&)> on_message_;  // 回调
    std::queue<std::string> pending_;                     // 待派发的消息
    std::mutex mutex_;
    std::condition_variable cv_;
    std::thread thread_;
    std::atomic<bool> accepting_{true};   // disconnect 后拒绝新注入
    std::atomic<bool> stopping_{false};   // 收工信号
    std::string url_;
};

#pragma once

#include <atomic>
#include <functional>
#include <thread>
#include <vector>

class ThreadPool {
public:
    // TODO 1: 构造函数
    // - 存下线程数量
    // - stop_ 初始化为 false
    explicit ThreadPool(size_t num_threads)
        : num_threads_(num_threads) {
        // --- 你的代码 ---
        stop_ = false;
    }

    // TODO 2: 析构函数
    // - 如果还在运行，调用 shutdown()
    ~ThreadPool() {
        // --- 你的代码 ---
        if (is_running()) {
            shutdown();
        }
    }

    // 禁止拷贝（线程不可复制）
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // TODO 3: 启动所有 Worker
    // - 创建 num_threads_ 个 std::thread
    // - 每个线程执行 worker_func（一个无限循环，直到 stop_ 为 true）
    // - 存到 workers_ 里
    // 提示: 用 workers_.emplace_back([this, worker_func] { ... })
    void start(std::function<void()> worker_func) {
        // --- 你的代码 ---
        // for (size_t i = 0; i < num_threads_; ++i) {
        //     workers_.emplace_back([this, worker_func] {
        //         worker_func();   // ThreadPool 不关心具体做什么，
        //                           // 只负责启动线程
        //     });
        // }
        for(size_t i=0;i<num_threads_;i++)
        {
            workers_.emplace_back([this, worker_func] {
                worker_func();
            });
        }
    }

    // TODO 4: 优雅关闭
    // - 设 stop_ = true
    // - 等所有线程结束（join）
    // 提示: Worker 函数里需要自己检查 stop_ 并退出，
    //       ThreadPool 只负责发信号 + 等它们结束
    void shutdown() {
        // --- 你的代码 ---
        bool expected = false;
        if(!stop_.compare_exchange_strong(expected,true))
            return;
        for(auto& t:workers_)
        {
            t.join();
        }
        workers_.clear();
    }

    // TODO 5: 状态查询
    // 提示: "正在运行" = 线程数 > 0 且 stop_ 为 false
    bool is_running() const {
        // --- 你的代码 ---
        return workers_.size() > 0 && !stop_;
    }

    size_t worker_count() const {
        return workers_.size();
    }

    // 暴露给 Worker 函数用的：检查是否该停了
    bool should_stop() const {
        return stop_;
    }

private:
    std::vector<std::thread> workers_;
    std::atomic<bool> stop_{false};
    size_t num_threads_;
};

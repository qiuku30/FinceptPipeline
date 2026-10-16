#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>

template <typename T>
class MessageQueue {
public:
    enum class BackpressurePolicy
    {
        Block,  // 队列满时阻塞等待
        DropOldest,  // 队列满时丢弃最旧的元素
        DropNewest   // 队列满时丢弃新来的元素
    };

    explicit MessageQueue(size_t max_size = 100000, BackpressurePolicy policy = BackpressurePolicy::Block)
        : max_size_(max_size) ,policy_(policy){}

    // Producer 调用：放入一个元素
    // TODO: 实现线程安全 + 队列满时阻塞
    bool enqueue(T item) {
        std::unique_lock<std::mutex> lock(mutex_);
        if(shutdown_) return false;

        switch (policy_)
        {
        case BackpressurePolicy :: Block:
        {
            not_full_.wait(lock,[&]{return queue_.size()<max_size_ || shutdown_;});
            if(shutdown_) return false;
            queue_.push(std::move(item));
            not_empty_.notify_one();
            return true;
        }


        case BackpressurePolicy::DropNewest:
        {
            if(queue_.size()>=max_size_)
            {
                ++dropped_;
                return false;
            }
            queue_.push(std::move(item));
            not_empty_.notify_one();
            return true;
        }
        
        case BackpressurePolicy::DropOldest:
        {
            if(queue_.size()>=max_size_)
            {
                queue_.pop();
                ++dropped_;
            }
            queue_.push(std::move(item));
            not_empty_.notify_one();
            return true;
        }

        default:
            break;
        }

        return false;

    }

    // Consumer 调用：取出一个元素（队列空时阻塞）
    // TODO: 实现线程安全 + 队列空时阻塞等待
    T dequeue() {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock,[&]{return !queue_.empty() || shutdown_;});
        if(shutdown_ && queue_.empty()) return T();
        T item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return item;
    }

    // 带输出参数的重载：返回值 false 表示关机且队列空，true 表示成功取到数据
    // 数据写入 item，只在返回 true 时有效
    bool dequeue(T& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [&]{ return !queue_.empty() || shutdown_; });
        if (shutdown_ && queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return true;
    }

    // ------------------------------------------------------------
    // dequeue_for — 带超时的取元素
    //
    // 返回值：true  = 取到了
    //         false = 没取到（超时 或 队列已关闭）
    //
    // 要区分这两种情况，查 is_shutdown()（已存在、且单调，无竞态）：
    //
    //     if (queue_.dequeue_for(item, interval)) {
    //         处理 item
    //     } else if (queue_.is_shutdown()) {
    //         break;                 // 队列关了 → 收工
    //     } else {
    //         超时了 → 做点别的（比如把攒着的批 flush 掉）
    //     }
    //
    // 为什么需要它：见 docs/decisions/008-messagequeue-timed-dequeue.md
    //   「等到有数据、或等够 T」—— 阻塞版会卡死在等数据上（低负载时
    //   永远等不到），非阻塞版退化成轮询。这是 core 本来就该有的通用能力。
    //
    // ⚠️ 判空条件与 dequeue 不同，这是有意的：
    //   dequeue      : shutdown_ && empty → false  （只有关机才 false）
    //   dequeue_for  : empty → false               （超时也会队列空）
    //   排空语义两者一致：只要还有数据就取走，不管关没关。
    // ------------------------------------------------------------
    bool dequeue_for(T& item, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait_for(lock, timeout, [&]{ return !queue_.empty() || shutdown_; });
        if (queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one(); // 腾出一个空位 → 叫醒可能阻塞的生产者
        return true;
    }

    // Consumer 调用：非阻塞取出（队列空时返回 false）
    // TODO: 实现线程安全
    bool try_dequeue(T& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return true;
    }

    // 优雅关闭：唤醒所有等待中的线程
    // TODO: 实现
    void shutdown() {
        shutdown_ = true;
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    size_t size() const { return queue_.size(); }
    size_t capacity() const { return max_size_; }
    bool is_shutdown() const { return shutdown_; }



    uint64_t dropped_count() const {return dropped_;}

private:
    std::queue<T> queue_;
    size_t max_size_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::atomic<bool> shutdown_{false};
    BackpressurePolicy policy_;
    std::atomic<uint64_t> dropped_{0};
};

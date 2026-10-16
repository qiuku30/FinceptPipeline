#pragma once

#include <chrono>
#include <cstdint>
#include <string>

// Pipeline 内部统一消息类型
// 目前只用 id 做确定性验证，topic/payload/received_at 后续阶段再用
struct TickData {
    uint64_t id;                                       // 单调递增序列号，测试用
    std::string topic;                                 // 例如 "market:quote:BTC-USD"
    std::string payload;                               // [V0] JSON 字符串
    std::chrono::steady_clock::time_point received_at; // T0: 收到时间
};

// 单条消息的完整生命周期时间戳（横切模块用的类型）
// M1 只用前四个字段（T0~T3），后两个 M2/M3 再填
struct TickLatency {
    std::chrono::steady_clock::time_point received_at;    // T0: 收到
    std::chrono::steady_clock::time_point enqueued_at;    // T1: 入队
    std::chrono::steady_clock::time_point dequeued_at;    // T2: Worker 取出
    std::chrono::steady_clock::time_point processed_at;   // T3: 处理完成
    std::chrono::steady_clock::time_point cached_at;      // T4: Cache 写入（预留）
    std::chrono::steady_clock::time_point published_at;   // T5: 发布（预留）
};

// 队列里流通的单元：业务数据 + 它的时间戳，成对传递不会错配
struct PipelineItem {
    TickData data;
    TickLatency latency;
};

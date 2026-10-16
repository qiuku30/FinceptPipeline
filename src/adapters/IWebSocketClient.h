#pragma once

#include <functional>
#include <string>

// ============================================================
// IWebSocketClient — 数据源侧窄接口（名片 1）
//
// Pipeline Core 只认识这张名片：设置回调 → 连接 → 数据到了回调被调用。
//
// 实现者：
//   MockWebSocketClient（M2 第一步，模拟消息到达）
//   FinceptWebSocketClient（M2 真接入，内部翻译 Qt 信号为回调）
//
// 约定：回调在客户端自己的收包线程上执行（Qt 模型同理：
//   信号在 QObject 所在线程的事件循环里触发）
// ============================================================
class IWebSocketClient {
public:
    // 设置消息回调：每次收到一条消息，调用一次 cb（原文字符串）
    virtual void set_on_message(std::function<void(const std::string&)> cb) = 0;

    // 连接（mock 里只记录 URL 并启动内部线程）
    virtual void connect(const std::string& url) = 0;

    // 断开：停止回调（已收到的消息仍要派发完，见 Mock 实现）
    virtual void disconnect() = 0;

    virtual ~IWebSocketClient() = default;
};

#pragma once

#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

// Qt 类型：头文件里用到什么就 include 什么。
// 前向声明（class QWebSocket;）只能用于指针/引用——
// 而这里要 `QWebSocket socket;`（栈上对象），编译器必须知道它的完整定义。
#include <QAbstractSocket>
#include <QEventLoop>
#include <QMetaObject>
#include <QObject>
#include <QString>
#include <QTimer>
#include <QUrl>
#include <QWebSocket>

#include "adapters/IWebSocketClient.h"

// ============================================================
// KrakenWebSocketClient — 真实 WebSocket 数据源（Qt 实现）
//
// IWebSocketClient 的第二个实现。它存在的意义只有一个：
//   **验证"名片"设计对不对** —— 换一个和 Mock 完全不同的实现，
//   看 src/core/ 需不需要改。不需要改 = 依赖倒置成立。
//
// ── 与 Mock 的结构对应 ──────────────────────────────────────
//
//   MockWebSocketClient            KrakenWebSocketClient
//   ───────────────────────        ─────────────────────────
//   run() 里的 while(true)   →     QEventLoop::exec()
//   自己维护的 pending_      →     Qt 的事件队列（内置，你看不见）
//   cv_.notify_one()         →     投递事件（QueuedConnection）
//   回调在收包线程执行        →     ← 同样要求
//
// ── 线程模型（本类唯一的难点）───────────────────────────────
//
//   Qt 的三条规则，共同决定了回调在哪个线程执行：
//
//     ① 线程亲和性：QWebSocket 属于"创建它的那个线程"
//     ② 事件循环：  那个线程必须跑着 exec()，socket 才收得到数据
//     ③ 连接类型：  跨线程的信号自动排队（QueuedConnection）
//
//   推论：在子线程里 new QWebSocket + 那个线程跑 exec()
//         → 信号在子线程触发 → 回调在子线程执行 ✓
//
//   **契约「回调在收包线程上执行」由此自动成立。**
//
// ── 前置条件 ────────────────────────────────────────────────
//
//   test 的 main() 里必须存在 QCoreApplication 实例。
// ============================================================
class KrakenWebSocketClient : public IWebSocketClient {
public:
    KrakenWebSocketClient() = default;

    // 析构必须保证线程收干净：
    // std::thread 析构时若仍 joinable → std::terminate()（进程当场死）
    ~KrakenWebSocketClient() override
    {
        disconnect();
    }

    void set_on_message(std::function<void(const std::string&)> cb) override
    {
        on_message_ = std::move(cb);
    }

    void connect(const std::string& url) override
    {
        // 二次 connect 直接拒绝。
        // （Mock 里那个版本会 std::terminate —— 这里是修掉的那个坑）
        if (thread_.joinable()) {
            return;
        }
        thread_ = std::thread([this, url]() { run(url); });
    }

    // ------------------------------------------------------------
    // disconnect — 排空后优雅退出
    //
    // 核心机制：QMetaObject::invokeMethod + QueuedConnection
    //
    //   为什么不能直接 loop->quit()？
    //     主线程拿到 loop 指针直接调 quit()，确实能让 exec() 返回，
    //     但那是"立刻叫停" —— 排在事件队列里还没跑的事件全丢 ❌
    //
    //   为什么要走 invokeMethod？
    //     invokeMethod(loop_ptr_, lambda, Qt::QueuedConnection) 会把
    //     lambda **打包成一个事件，投进 loop_ptr_ 所在线程的事件队列尾部**。
    //     事件队列 FIFO —— 轮到 lambda 时，排在它前面的都跑完了。
    //
    //   ★ 这就是"排空语义"：不是"等一下"，是"排我前面的先跑完"。
    //     对应 Mock 里的退出条件 stopping_ && pending_.empty()。
    // ------------------------------------------------------------
    void disconnect() override
    {
        // ① 投递"退出"任务到子线程事件队列的队尾
        {
            std::lock_guard<std::mutex> lock(loop_mutex_);
            if (loop_ptr_ == nullptr) {
                // 子线程还没起来或已经退出，没什么可停的
                if (thread_.joinable()) {
                    thread_.join();
                }
                return;
            }
            // 指针按值捕获：投递之后不再读成员，避免用到可能已失效的值
            QEventLoop* lp = loop_ptr_;
            QMetaObject::invokeMethod(lp, [lp]() { lp->quit(); },
                                      Qt::QueuedConnection);
        }

        // ② join —— 等子线程把队列排空、退出循环、跑完 run() 的收尾
        if (thread_.joinable()) {
            thread_.join();
        }
    }

private:
    // ------------------------------------------------------------
    // run — 子线程主函数（对应 Mock 的 run()）
    // ------------------------------------------------------------
    void run(const std::string& url)
    {
        // ① 在本线程创建 socket —— 亲和性 = 本线程
        //    这一行决定了后面所有信号在哪个线程触发
        QWebSocket socket;

        // ② 收到报文 → 转发给上游回调
        //    第四个参数 &socket 是 context object：
        //    socket 析构时连接自动断开，不用手动管生命周期
        QObject::connect(&socket, &QWebSocket::textMessageReceived, &socket,
                         [this](const QString& msg) {
                             if (on_message_) {
                                 on_message_(msg.toStdString());
                             }
                         });

        // ③ 连上之后才发订阅 —— WebSocket 需要先握手成功
        QObject::connect(&socket, &QWebSocket::connected, &socket, [&socket]() {
            const QString sub =
                R"({"event":"subscribe","pair":["XBT/USD"],"subscription":{"name":"ticker"}})";
            socket.sendTextMessage(sub);
            std::cout << "[Kraken] 已连接，订阅报文已发出\n";
        });

        // ④ 出错（TODO 5：这里目前只打印，没有通知上游）
        //    打印 errorString() —— 光看错误码数字没用，要看人话
        QObject::connect(&socket, &QWebSocket::errorOccurred, &socket,
                         [&socket](QAbstractSocket::SocketError error) {
                             std::cerr << "[Kraken] socket error " << error << ": "
                                       << socket.errorString().toStdString() << "\n";
                         });

        // ④b 连接断开（含握手失败后的收尾）
        QObject::connect(&socket, &QWebSocket::disconnected, &socket, []() {
            std::cerr << "[Kraken] 连接已断开\n";
        });

        // ⑤⑥ 事件循环 + 发起连接
        //
        // ⚠️ 坑：open() 不能写在 exec() 前面！
        //
        //   子线程的【事件派发器】是在 exec() 时才创建的。
        //   如果提前调 open()，socket 的 notifier 注册不到派发器上
        //   → TCP 连接永远不推进 → 既不报错也不连上，干等到超时。
        //
        //   （主线程没这问题：QCoreApplication 构造时就把主线程的
        //     派发器建好了。这是子线程独有的坑。）
        //
        //   解法：用 singleShot(0) 把 open() 投进事件队列 ——
        //   循环一跑起来它就执行，那时派发器已经就位。
        QEventLoop loop;

        // ⑦ 把"通道"交给主线程（加锁保护）
        {
            std::lock_guard<std::mutex> lock(loop_mutex_);
            loop_ptr_ = &loop;
        }

        QTimer::singleShot(0, &socket, [&socket, url]() {
            socket.open(QUrl(QString::fromStdString(url)));
        });

        loop.exec();   // 阻塞在这里收发事件，直到主线程投来 quit

        // ⑧ 收尾：先把通道摘掉，再让 loop 析构。
        //    顺序关键 —— 必须赶在 loop 离开作用域前把 loop_ptr_ 置空，
        //    否则主线程可能拿到一个悬垂指针。
        //    加锁保证：主线程要么看到 nullptr，要么看到的一定是活着的 loop。
        {
            std::lock_guard<std::mutex> lock(loop_mutex_);
            loop_ptr_ = nullptr;
        }
        socket.close();
        std::cout << "[Kraken] 收包线程退出\n";
    }

    std::function<void(const std::string&)> on_message_;

    // 子线程句柄
    std::thread thread_;

    // 跨线程通道：指向子线程事件循环的指针。
    // 主线程通过 invokeMethod 往子线程事件队列投递任务。
    // 用 mutex 保护，防止"读到一个正在析构的 loop"。
    std::mutex loop_mutex_;
    QEventLoop* loop_ptr_ = nullptr;
};

// ============================================================
// TODO 5（设计决策）: 连接意外断开，要不要通知上游？
//
// 现状：IWebSocketClient 只有 set_on_message —— 没有"连接没了"的通道。
// 真实场景：VPN 掉线 / Kraken 主动踢你 / 网络抖动
//           → 连接无声无息断掉，而 AsyncReceiver 还在傻等数据。
//
// 要加 set_on_disconnect 的话：
//   IWebSocketClient     +1 个方法
//   MockWebSocketClient  要实现
//   AsyncReceiver        要用（断开时要不要 queue.shutdown()？）
//   src/core/            ← 一个都不用动 ✓
//
// 最后一行才是重点：契约演进被关在适配层内部 = 名片设计对了。
//
// ── 另一件要想清楚的事（TODO 4 的第 ③ 条）────────────────────
//
// 上面实现的"排空"排的是**事件队列**。但真实 socket 还有一层：
// 数据可能躺在 TCP 接收缓冲区里、**根本还没变成事件**。
// 那部分不在事件队列里，投在队尾的 quit 排不到它们。
//
// Mock 里 pending_ 是你自己的队列，看得见数得清，能保证排干净。
// 真实世界做不到 —— 这是 Mock 永远不会暴露的差别。
// 它意味着契约里"断开时已收到的消息会派发完"这句话，
// 在真实实现上只能做到"尽力而为"。
// ============================================================

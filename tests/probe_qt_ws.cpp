#include <iostream>

#include <QAbstractSocket>
#include <QCoreApplication>
#include <QTimer>
#include <QUrl>
#include <QWebSocket>

// ============================================================
// 诊断探针：最小化的 QWebSocket 连接测试
//
// 目的：**隔离变量**。
//
//   curl 能连上 Kraken，我们的适配器连不上。
//   到底是 Qt 的问题，还是我们适配器的线程模型的问题？
//
//   这个探针把适配器整个甩掉 —— 主线程 + 标准 Qt 事件循环
//   （app.exec()）+ 裸 QWebSocket。这是最朴素的用法。
//
//   探针能通 → 问题在适配器的线程模型
//   探针也不通 → 问题在 Qt / 网络环境
//
// 【运行】cd build && ./probe_qt_ws
// ============================================================

static const char* state_name(QAbstractSocket::SocketState s) {
    switch (s) {
        case QAbstractSocket::UnconnectedState: return "Unconnected  （没开始/已断开）";
        case QAbstractSocket::HostLookupState:  return "HostLookup   （DNS 解析中）";
        case QAbstractSocket::ConnectingState:  return "Connecting   （TCP 握手中）";
        case QAbstractSocket::ConnectedState:   return "Connected    （已连上）";
        case QAbstractSocket::BoundState:       return "Bound";
        case QAbstractSocket::ClosingState:     return "Closing";
        case QAbstractSocket::ListeningState:   return "Listening";
        default:                                return "?";
    }
}

int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);
    QWebSocket socket;

    QObject::connect(&socket, &QWebSocket::connected, &socket, [&socket]() {
        std::cout << "[probe] ✅ connected！发送订阅报文\n";
        socket.sendTextMessage(
            R"({"event":"subscribe","pair":["XBT/USD"],"subscription":{"name":"ticker"}})");
    });

    QObject::connect(&socket, &QWebSocket::textMessageReceived, &socket,
                     [](const QString& msg) {
                         std::cout << "[probe] 📨 " << msg.toStdString() << "\n";
                     });

    QObject::connect(&socket, &QWebSocket::errorOccurred, &socket,
                     [&socket](QAbstractSocket::SocketError e) {
                         std::cerr << "[probe] ❌ error " << e << ": "
                                   << socket.errorString().toStdString() << "\n";
                     });

    QObject::connect(&socket, &QWebSocket::disconnected, &socket, []() {
        std::cerr << "[probe] 连接断开\n";
    });

    // 每 2 秒报一次状态 —— 这是关键诊断：卡在哪一步
    QTimer stateTimer;
    QObject::connect(&stateTimer, &QTimer::timeout, &socket, [&socket]() {
        std::cout << "[probe] 状态: " << state_name(socket.state()) << "\n";
    });
    stateTimer.start(2000);

    // 25 秒后自动退出
    QTimer::singleShot(25000, &app, &QCoreApplication::quit);

    socket.open(QUrl("wss://ws.kraken.com"));
    std::cout << "[probe] open() 已调用，进入主线程事件循环\n";

    return app.exec();
}

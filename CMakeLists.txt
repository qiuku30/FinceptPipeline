cmake_minimum_required(VERSION 3.27)
project(FinceptPipeline VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 生成 compile_commands.json —— clangd 会自动读它，
# include path / Qt 路径 / 编译标准全都能对上（消掉 IDE 里的红波浪线噪音）
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ============================================================
# 被 FinceptTerminal 用 add_subdirectory 引入时，宿主会先把它设为 OFF，
# 免得我们这 11 个测试/工具目标污染宿主的构建。
# 独立构建时默认 ON。
# ============================================================
option(FINCEPT_PIPELINE_BUILD_TESTS "构建 FinceptPipeline 的测试与工具" ON)

# ============================================================
# 两个 target，对应架构里的两层
#
#   pipeline_core     名片层 —— 零外部依赖（尤其不含 Qt）
#   fincept_pipeline  适配层 —— 真实实现，依赖 Qt
#
# 拆成两个 target 不是多余的：**它把"核心不依赖 Qt"这件事
# 写进了构建系统**，谁想往 core 里塞 Qt 头文件，链接关系立刻会说话。
# ============================================================

# ── 名片层 ──────────────────────────────────────────────────
# MessageQueue / ThreadPool / LatencyTracker / IPublisher / IWebSocketClient …
add_library(pipeline_core INTERFACE)
target_include_directories(pipeline_core INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)

# ── 适配层 ──────────────────────────────────────────────────
# KrakenWebSocketClient（真 Qt + 真网络）等。
#
# 若宿主已经 find_package(Qt6) 过，直接复用它找到的那份 Qt ——
# 一个进程只能有一份 Qt，绝不能混用两个版本（FinceptTerminal 用的是 6.8.3）。
if(NOT TARGET Qt6::WebSockets)
    # 独立构建时的兜底：brew 的 Qt 不在 CMake 默认搜索路径里
    if(NOT CMAKE_PREFIX_PATH AND EXISTS "/opt/homebrew/opt/qt")
        set(CMAKE_PREFIX_PATH "/opt/homebrew/opt/qt")
    endif()
    find_package(Qt6 REQUIRED COMPONENTS WebSockets)
endif()

add_library(fincept_pipeline INTERFACE)
target_link_libraries(fincept_pipeline INTERFACE pipeline_core Qt6::WebSockets)

# ============================================================
# 以下全部是测试与工具 —— 只在独立构建时生成
# ============================================================
if(FINCEPT_PIPELINE_BUILD_TESTS)

    enable_testing()

    # CacheWriter 的测试和 benchmark 都需要 SQLite。
    # 放在这里而不是各 target 前面 —— 找一次就够，后面都能用。
    find_package(SQLite3 REQUIRED)

    # ── 纯核心测试（不依赖 Qt）──────────────────────────────
    add_executable(test_message_queue tests/test_message_queue.cpp)
    target_link_libraries(test_message_queue PRIVATE pipeline_core)

    add_executable(test_message_queue_mt tests/test_message_queue_mt.cpp)
    target_link_libraries(test_message_queue_mt PRIVATE pipeline_core)

    add_executable(test_thread_pool tests/test_thread_pool.cpp)
    target_link_libraries(test_thread_pool PRIVATE pipeline_core)

    add_executable(test_determinism tests/test_determinism.cpp)
    target_link_libraries(test_determinism PRIVATE pipeline_core)

    add_executable(test_mock_generator tests/test_mock_generator.cpp)
    target_link_libraries(test_mock_generator PRIVATE pipeline_core)

    add_executable(test_integration tests/test_integration.cpp)
    target_link_libraries(test_integration PRIVATE pipeline_core)

    add_executable(test_backpressure tests/test_backpressure.cpp)
    target_link_libraries(test_backpressure PRIVATE pipeline_core)

    add_executable(test_backpressure_mt tests/test_backpressure_mt.cpp)
    target_link_libraries(test_backpressure_mt PRIVATE pipeline_core)

    add_executable(test_latency_tracker tests/test_latency_tracker.cpp)
    target_link_libraries(test_latency_tracker PRIVATE pipeline_core)

    add_executable(test_adapter_e2e tests/test_adapter_e2e.cpp)
    target_link_libraries(test_adapter_e2e PRIVATE pipeline_core)

    add_executable(test_dequeue_for tests/test_dequeue_for.cpp)
    target_link_libraries(test_dequeue_for PRIVATE pipeline_core)
    add_test(NAME DequeueFor COMMAND test_dequeue_for)

    # ── 需要 SQLite 的测试 ───────────────────────────────────
    add_executable(test_cache_writer tests/test_cache_writer.cpp)
    target_link_libraries(test_cache_writer PRIVATE fincept_pipeline SQLite3::SQLite3)
    add_test(NAME CacheWriter COMMAND test_cache_writer)

    add_test(NAME BackpressureMT COMMAND test_backpressure_mt)
    add_test(NAME LatencyTracker COMMAND test_latency_tracker)
    add_test(NAME Backpressure COMMAND test_backpressure)
    add_test(NAME Integration COMMAND test_integration)
    add_test(NAME MessageQueueBasics COMMAND test_message_queue)
    add_test(NAME MessageQueueMultiThreaded COMMAND test_message_queue_mt)
    add_test(NAME ThreadPool COMMAND test_thread_pool)
    add_test(NAME Determinism COMMAND test_determinism)
    add_test(NAME MockGenerator COMMAND test_mock_generator)
    add_test(NAME AdapterE2E COMMAND test_adapter_e2e)

    # ── Benchmark（手动运行的工具，不注册为测试）─────────────
    add_executable(pipeline_bench benchmark/pipeline_bench.cpp)
    target_link_libraries(pipeline_bench PRIVATE pipeline_core)

    # M3：CacheWriter 批量写入对比（手动跑，耗时长，不进 ctest）
    add_executable(cache_bench benchmark/cache_bench.cpp)
    target_link_libraries(cache_bench PRIVATE fincept_pipeline SQLite3::SQLite3)

    # ── 需要 Qt 的测试与工具 ─────────────────────────────────
    # 联网测试：手动运行，不注册 ctest。
    # 理由和 pipeline_bench 同理 —— 依赖网络 + VPN、跑得慢，
    # 而且"收到几条"取决于当时行情，结果天然不确定。
    add_executable(test_kraken_live tests/test_kraken_live.cpp)
    target_link_libraries(test_kraken_live PRIVATE fincept_pipeline)

    # 诊断探针（排障用，可删）
    add_executable(probe_qt_ws tests/probe_qt_ws.cpp)
    target_link_libraries(probe_qt_ws PRIVATE Qt6::WebSockets)

endif()

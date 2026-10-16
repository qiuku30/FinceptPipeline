#pragma once

#include "core/Types.h"

// ============================================================
// IPublisher — 发布侧窄接口（名片 2）
//
// Pipeline Core 只认识这张名片：处理完的数据交给"会发布的人"。
// 具体发到哪（DataHub / 日志 / 丢弃）Core 不关心。
//
// 实现者：
//   MockPublisher（测试里收集数据）
//   DataHubPublisher（M2 真接入 Fincept DataHub）
//
// 契约要点：只传 TickData 不传 PipelineItem——
//   监控时间戳（T1~T5）是管道内部记账，不该泄露给业务侧
// ============================================================
class IPublisher {
public:
    // 发布一条处理完的数据
    virtual void publish(const TickData& tick) = 0;

    virtual ~IPublisher() = default;
};

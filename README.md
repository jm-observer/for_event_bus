# EventBus

一个基于 `tokio::mpsc` 的轻量异步事件总线，支持：
- 多 Worker 登录
- 按事件类型订阅
- 广播分发给同类型的所有订阅者
- 通过 `derive` 简化 `Event / Worker / Merge` 实现

## Workspace 结构

- `for-event-bus`: 运行时总线实现（Bus、SubBus、Identity）
- `for-event-bus-derive`: 过程宏（`#[derive(Event, Worker, Merge)]`）

## 核心组件

- `Bus`: 控制平面，处理登录、订阅、投递、下线清理
- `SubBus`: 数据平面，每个 `TypeId` 一个子总线，负责 fanout
- `IdentityOfRx / IdentityOfSimple / IdentityOfMerge`: Worker 侧 API

## 总体流程图

```mermaid
flowchart TD
    A[Worker login] --> B[Bus 创建 WorkerId 和专属事件通道]
    B --> C[Worker subscribe T]
    C --> D{Bus 是否已有 T 的 SubBus}
    D -- 否 --> E[创建 SubBus<T>]
    D -- 是 --> F[复用已有 SubBus<T>]
    E --> G[登记订阅者]
    F --> G
    H[Worker dispatch Event<T>] --> I[Bus 按 TypeId 定位 SubBus<T>]
    I --> J[SubBus fanout 到所有订阅者]
    J --> K[Worker recv / try_recv]
    K --> L[Worker drop]
    L --> M[Bus 反订阅并按需销毁空 SubBus]
```

## Bus 事件循环（控制平面）

```mermaid
flowchart TD
    A[BusData::Login] --> A1[创建 Identity + CopyOfWorker 并回传]
    B[BusData::Subscribe] --> B1[记录 worker 订阅关系]
    B1 --> B2[创建或复用 SubBus 并发送 Subscribe]
    C[BusData::DispatchEvent] --> C1[按 event TypeId 路由到 SubBus]
    D[BusData::Drop] --> D1[移除 worker]
    D1 --> D2[对每个已订阅类型发送 Unsubscribe]
    D2 --> D3{SubBus 是否为空}
    D3 -- 是 --> D4[发送 Drop 销毁 SubBus]
    E[BusData::Trace] --> E1[触发各 SubBus 输出订阅快照]
```

# synp-api

WebSocket 网关的 API 契约工程。proto 目录是唯一事实源，定义:

- 前端与网关的通信帧协议 ( `common/v1` + `message/v1` )
- 网关与后端之间的 gRPC 服务契约与 Kafka 记录契约 ( `message/v1` )
- 连接路由契约 ( `route/v1` )
- 连接鉴权的 JWT 契约 ( `auth/v1` )

`api/` 目录为生成代码，不要手工修改。

```bash
make grpc-go   # buf format + buf lint + buf generate
```

## 架构总览

```
                      WebSocket ( Message 帧 )
  frontend ( 业务客户端 ) <--------------------> gateway ( 网关集群 )
                                                        │  │
             Kafka 上行 topic ( UpstreamMessage ) <──────┘  │
             Kafka 通知 topic ( NotifyMessage ) ────────────┘
                        │                        │
                        ▼                        ▼
                   backend ( 业务后端 )      推送侧 ( 消费通知，初期为网关集群自身 )
                        │
                        ▼
                   存储与 seq 分配
```

- **frontend**: 业务客户端，持有本地游标 ( cursor )，按需增量拉取。
- **gateway**: 持有连接、盖章身份、转发上行、下发通知、代理同步请求。
- **backend**: 消费上行消息，落库并分配 seq，投递通知；实现 `SyncService`。
- **route**: 路由服务 ( `RouteService` )，维护「用户连接 → 网关节点」映射。

## 同步模型: 落库即事实，推送即通知

所有下行消息遵循同一模型，四条原则:

1. **落库即事实**: 消息只有被后端持久化并分配单调递增的 seq 才算成立。
2. **推送即通知**: 下行推送 ( `NOTIFY` 帧 ) 只携带最新 seq，不携带消息本体。
3. **游标驱动**: 客户端维护本地 cursor，通过 `SYNC` 增量拉取 `( cursor, latest_seq ]`；拉取幂等，重复条目按 seq 去重。
4. **at-least-once**: 链路任何环节都可能重复投递；通知丢失只影响实时性，不影响正确性 ( 客户端重连/心跳时主动 SYNC 兜底 )。

该模型同时解决: 推送丢失补偿、断线重连补发、多端同步、离线消息——它们全部统一为「cursor 落后就拉取」这一个机制。

### 单用户投递 ( 单聊，写扩散 )

用户 A 向用户 B 发消息 ( 会话人数为 2，含发送者 ):

```
A: UPSTREAM 帧 ( body = 业务消息 )
   └─ gateway: UPSTREAM_ACK ( 网关级回执 ) + 盖章身份投递 Kafka 上行 topic
        └─ backend: 校验 ( 好友关系/禁言/内容 ) 后落库
             ├─ 写 A 的 inbox ( 发送者多端自同步 )，A.inbox_seq 递增
             ├─ 写 B 的 inbox，B.inbox_seq 递增
             └─ 投递通知 topic: NotifyMessage( receiver = A ) + NotifyMessage( receiver = B )
                  └─ 推送侧: 查 RouteService 定位连接所在节点
                       └─ gateway: NOTIFY 帧 ( body = NotifyPayload{ latest_seq } )
                            └─ 客户端: 本地 cursor 落后 → SYNC 帧
                                 └─ gateway 调 SyncService.Sync
                                      └─ 返回 InboxEntry ( 内联 ChatMessage 本体 ) → 渲染
```

特点:

- 每条消息 **2 次 inbox 写** ( 会话内每个参与者各一次 )，消息本体随 SYNC 内联下发，客户端**一次拉取**即可渲染。
- 收件箱 seq 在 `( business_id, user_id )` 维度严格单调递增，是单聊消息的**全序**。
- 发送者业务级失败 ( 如被禁言 ) 不通过 `UPSTREAM_ACK` 表达，而是作为一条系统消息走同步链路下发 ( 由业务定义 )。

### 多用户投递 ( 群聊，读扩散 )

大群 ( 如万人群 ) 做写扩散意味着一条消息 = N 万次 inbox 写，不可行。群聊走**会话 timeline**:

```
A 在群 G 发消息:
   UPSTREAM → gateway 盖章 → Kafka 上行 topic        ( 同单聊 )
   └─ backend: 校验 ( 群成员/禁言 ) 后落库
        ├─ 写 G 的 timeline 一份，conversation_seq 递增
        └─ 向每个成员 inbox 写一条轻量指针
             InboxEntry { seq, ConversationUpdate{ conversation_id, conversation_latest_seq } }
   └─ 通知 topic: 每个成员一条 NotifyMessage ( 仅 latest_seq，极轻 )

成员客户端:
   SYNC → 收到 ConversationUpdate 指针 ( 而非消息本体 )
   └─ CONVERSATION_SYNC 帧 { conversation_id, cursor }
        └─ gateway 调 SyncService.ConversationSync ( 后端校验群成员身份 )
             └─ 返回 TimelineMessage 增量 → 按 conversation_seq 应用
```

特点:

- 每条消息 **1 次正文写 + N 次指针写** ( 指针远小于正文 )，万人群也只存一份正文。
- 客户端需要**两次拉取** ( inbox → timeline )，代价换写入可控。
- `conversation_seq` 保证**会话内全序**；跨会话没有全序，客户端按会话独立渲染。
- 指针写扩散可以只针对有变动的成员 ( 如在线成员 )，离线成员重连后 SYNC 时后端现算指针，具体策略由后端决定。

### 混合策略与边界

- 单聊与低于阈值的小群走**写扩散** ( 读体验优先 )，大群走**读扩散** ( 写入可控 )。阈值 ( 如 500 人 ) 是部署参数，由后端决定——**网关与客户端均不感知**: 对客户端而言两种条目都只是 `InboxEntry` 的不同 kind。
- 群成员变更、群解散等会话元数据属于业务 API，不在本契约内。
- `cursor` 超出服务端保留窗口时，`SyncResponse.cursor_expired = true`，客户端应重置本地数据后重新同步 ( 全量或按业务策略截断 )。

### 顺序与可靠性

| 维度 | 保证 |
|---|---|
| 单聊顺序 | inbox seq 全序，按 seq 应用 |
| 群聊顺序 | 会话内 conversation_seq 全序；跨会话无全序 |
| 投递语义 | at-least-once，客户端按 seq / message_id 幂等去重 |
| 通知丢失 | 仅延迟；重连/心跳/切前台触发主动 SYNC |
| 上行回执 | UPSTREAM_ACK 仅网关级；业务结果走同步链路 |

## 连接与鉴权 ( JWT )

1. 客户端先调用后端登录接口获取 JWT ( 契约见 `proto/auth/v1/auth.proto`，claims: `iss` / `aud` / `iat` / `exp` / `jti` / `business_id` / `user_id` / `session_id` )。
2. 建立 WebSocket 连接后，**第一帧必须为 `AUTH`** ( body = `AuthPayload{ token }` )。鉴权完成前网关拒绝处理任何其它指令；超时未鉴权的连接由网关关闭。
3. 网关本地验签: 签名算法 RS256，公钥经 JWKS 端点 ( `/.well-known/jwks.json` ) 拉取并缓存，token header 的 `kid` 用于密钥轮换。校验项: 签名、`exp`、`aud`。全程不回调后端。
4. 成功 → 回 `AUTH_ACK{ success: true }`；失败 → 回 `AUTH_ACK{ success: false, error_message }` 后关闭连接。
5. 鉴权通过后网关调用 `RouteService.RegisterConnection` 注册路由 ( 重复注册幂等并刷新 TTL )。
6. token 为短周期凭证 ( 建议 ≤ 2h )，过期即客户端重连需重新向后端获取。登出/顶号的即时失效依赖后端使 `session_id` 失效 + 网关鉴权时校验 session 有效性 ( 内部契约，暂未在本工程定义 )。

首帧鉴权 ( 而非 URL 参数携带 token ) 的理由: token 不落入访问日志与代理日志，且协议对客户端显式可见。

## 帧协议 ( CommandType )

| 指令 | 方向 | body | 说明 |
|---|---|---|---|
| HEARTBEAT | C → G | 空 | 心跳 |
| AUTH | C → G | `AuthPayload` | 首帧鉴权 |
| AUTH_ACK | G → C | `AuthAckPayload` | 鉴权应答 |
| UPSTREAM | C → G | 业务自定义 | 上行业务消息 |
| UPSTREAM_ACK | G → C | `AckPayload` | 网关级接收回执 |
| SYNC | C → G | `SyncFrame` | 收件箱增量同步 |
| SYNC_ACK | G → C | `SyncAckPayload` | 同步应答 |
| CONVERSATION_SYNC | C → G | `ConversationSyncFrame` | 会话 timeline 增量 ( 大群 ) |
| CONVERSATION_SYNC_ACK | G → C | `ConversationSyncAckPayload` | 会话同步应答 |
| NOTIFY | G → C | `NotifyPayload` | 服务端通知，触发同步 |
| REDIRECT | G → C | 空 ( 预留 ) | 重定向 |
| RATE_LIMIT_EXCEEDED | G → C | 空 ( 预留 ) | 限流 |

请求帧与应答帧通过 `Message.message_id` 关联；`message_id` 同时承担帧去重职责 ( 链路为 at-least-once )。

## Kafka 拓扑

| topic | 生产者 | 消费者 | 记录类型 | 分区键 |
|---|---|---|---|---|
| 上行 topic | gateway | backend | `UpstreamMessage` | `hash(business_id, 发送者 user_id)` |
| 通知 topic | backend | 推送侧 | `NotifyMessage` | `hash(business_id, receiver_id)` |

- 上行分区键保证同一发送者消息有序，便于后端串行分配 seq；通知分区键保证同一接收者的通知有序。**不要用 message_id 做分区键**——它每帧唯一，hash 后消息散落到随机分区，无法提供任何有序性。
- `UpstreamMessage` 的身份字段 ( business_id / user_id / session_id ) 由网关以 JWT 鉴权结果盖章，前端自报身份一律不可信——这是整个模型的安全前提。
- 通知 topic 的集群消费模型: 初期为**广播消费** ( 每个网关节点独立消费全量，按本地连接过滤后下发，节点不在线则忽略 )；规模增长后演进为独立推送分发层 ( dispatcher ) 单组消费 + `RouteService` 查询 `Node.service_address` 定向转发。`RouteService.Node` 的字段已为此预留。

## 服务契约一览

| 服务 | 提供方 | 调用方 | 定义 | 职责 |
|---|---|---|---|---|
| `RouteService` | 路由服务 | gateway / backend | `proto/route/v1` | 连接路由注册 ( TTL 续期 ) 与查询 |
| `SyncService` | backend | gateway | `proto/message/v1/sync.proto` | 收件箱与会话的增量同步 |
| `NotifyService` | gateway | backend | `proto/message/v1/notify.proto` | Kafka 之外的即时通知通道 |

同步请求经 WS 帧到达网关后，由网关以连接身份盖章调用 `SyncService`，应答原样放入 `*_ACK` 帧返回——客户端与后端之间不建立第二条通道，鉴权与连接复用同一条 WebSocket。

## 已知边界与后续工作

- 顶号/强制断开连接的契约未定义 ( `RouteService` 可查到目标连接，缺断开接口 )。
- 网关鉴权时校验 `session_id` 有效性的内部契约未定义。
- `NOTIFY` 携带小增量 ( 游标落后很少时免一次 SYNC ) 的优化未纳入，可作为后续扩展。
- 群聊通知的成员级扇出由后端完成 ( N 条 `NotifyMessage` )，推送侧无需理解群语义。
- 历史消息 ( 向更早翻页 ) 复用 `SyncService` 的会话同步能力即可，未单独定义翻页契约。

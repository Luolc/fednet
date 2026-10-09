# fednet 设计

这份文档只描述 fednet 当前的实际状态，状态变了就原地修改。决定和理由写在做出决定的那个 PR 里。只有维护者发起的 design 修订才改这份文件；其它 PR 不改它，只在 PR 描述的「Design impact」里写明对它的影响。

## 1. 是什么，不是什么

fednet 让用户在 Slack 线程里和各台机器上的 coding agent 打交道：在线程里交代一件事，由某台机器上的 agent 去做，中途的问题和最后的结果都回到同一个线程里。

计划只有一个二进制 `fednet`，分两个子命令：`fednet hub` 跑在一台固定的 hub 机器上，负责和 Slack 的连接；`fednet client` 跑在每台 agent 机器 (工作站、数据机) 上，主动连到 hub，把消息交给本机的 agent。

**当前状态：`fednet hub` 和 `fednet client` 都能跑起来，client 凭登记过的凭证连上 hub，hub 排队的消息送到 client 的 inbox 为止；本机的 agent 可以用 `fednet client post` 经 client 往线程发消息，hub 收到后只落进 inbox；hub 还没接 Slack，线程路由没有被子命令用到，client 也还没把收到的消息交给本机的 agent。** 下面各节照实写，不描述还不存在的东西。

fednet 不是：

- agent 的运行时。agent 由 herdr 启动和管理，fednet 只负责把消息送到它那里。
- 模型网关。fednet 不管模型账号的登录，也不转发模型流量。
- 任务看板。待办和尚未落实的设计在 Linear 上，不在这个仓里。

## 2. 组成部分

只有一个 Go 写的二进制 `fednet` ([`cmd/fednet`](../cmd/fednet))，用标准库 `flag` 分派子命令。`fednet hub` 在显式给出的地址上起 HTTP 服务，只挂通道的 handler，用登记表认 client；`fednet hub register` 与 `fednet hub revoke` 在 hub 机器上改登记表。`fednet client init` 生成本机凭证，`fednet client` 读凭证连上 hub 跑通道循环，收到的下行消息只落进 inbox，同时在本机开一个 unix socket 给 agent 用；`fednet client post` 经这个 socket 把消息交给 client。`version` 打印构建时注入的版本号，未注入时打印 `dev`。tailnet 上的流量由 WireGuard 加密，hub 与 client 之间走明文 HTTP，不加 TLS。

存储层在 [`internal/store`](../internal/store)，用纯 Go 的 SQLite 驱动 `modernc.org/sqlite`，hub 和 client 各一个库文件、各一套表。hub 端有发给每台 client 的 outbox (按 `seq` 续传、按 ack 清理，记入队时间，可以查某台 client 有没有排队超过给定时长的消息)、收上行消息的 inbox (记下每条来自哪台 client) 和线程归属表；client 端有收下行消息的 inbox 和上行的 outbox。两端的 inbox 都按 `msg_id` 去重。hub 端另有 client 登记表：client id、凭证的 SHA-256、是否已退役、client 最近一次连接时报的版本。

线程路由在 [`internal/route`](../internal/route)，新线程和已有线程里的回复分两个入口。新线程取这个 channel 的默认机器 (channel 到机器的对应目前由调用方用结构体传入)，登记为归属并写进它的 outbox，两步在同一个事务里；线程已有归属 (例如上游重发了第一条消息) 就送归属机器；既没有归属、channel 也没配默认机器，就不送，返回错误。回复写进归属机器的 outbox；线程没有归属 (例如 fednet 上线前就有的线程) 就不送，返回错误。不管机器在不在线都照样入队。归属只有显式改派才会变：改一个线程 (`adopt`)，或把一台机器的线程整批改给另一台 (`reassign`)。

通道在 [`internal/link`](../internal/link)，hub 端是一个 HTTP handler，client 端是一个常驻的循环。下行 (hub → client) 是 client 主动连到 hub 的 WebSocket (`github.com/coder/websocket`)：连上后 hub 先把这台 client outbox 里所有未 ack 的消息按 `seq` 发一遍，之后有新入队的就推；client 每收到一条先写进 inbox，再回一个累积的 ack，hub 收到 ack 才把 outbox 里 `seq` 不大于它的删掉。上行 (client → hub) 是一个 HTTP POST，一条消息一个请求，带 `msg_id`；hub 连同来源 client 一起写进 inbox 后才回 204，client 收到 204 才把这条从 outbox 删掉，否则按退避重发。一条消息的 payload 最大 256 KiB，入队时就拒绝超过的，两端的帧和请求体上限按它定。每次拨号和每个上行请求都有自己的超时。断线后 client 用有上限、带随机抖动的指数退避重连，重连后按上面的下行规则续传。client 每隔一个心跳间隔发一个 WebSocket ping，hub 在内存里记每台 client 最近一次心跳的时间，一个租约期内有心跳就算在线。client 每个请求都带请求头 `Fednet-Client` 里的 id，hub 端认身份的函数是可以替换的，由下面的认证接上。

认证在 [`internal/auth`](../internal/auth)。client 自己生成 256 bit 的随机凭证，存在本机一个只有所有者能读 (0600) 的文件里，连同 client id 一起；hub 只登记它的 SHA-256。每个请求带 `Authorization: Bearer <凭证>` 和 `Fednet-Version`，hub 算哈希、与登记表常数时间比对，并记下版本；未登记、已退役、凭证不对的一律回 401，正文固定是 `unauthorized`，原因只进 hub 的日志，凭证不进日志、错误信息和任何命令的输出。登记 (`fednet hub register`) 写入 id 与哈希，重复登记替换哈希并解除退役；退役 (`fednet hub revoke`) 让这台 client 的凭证失效，已经建立的连接要到断开才生效。Tailscale `WhoIs` 核对来源节点还没有接。

本机 socket 在 [`internal/local`](../internal/local)。agent 拿不到 client 的凭证，只能经这个 socket 把请求交给常驻的 client 进程。socket 的路径由参数给出；不给组时权限是 0600，只有 client 所在的 Unix 用户能连，给了组 (`-socket-group`) 时是 0660，组里的用户也能连。socket 先建在一个只有 client 用户能进的临时目录里，权限和组设好后再改名到给定的路径。一个路径同一时间只归一个 client：启动时先对 socket 旁边的 `<socket>.lock` 加排他的 `flock`，拿不到就不启动，拿到后一直持有到进程退出 (进程死了由内核释放)。检查和改名都在持锁之后，所以路径上已有的 socket 一定是旧进程留下的，直接替换；路径上是别的文件时不启动。一个连接只承载一个请求：调用方写一个 JSON 请求，按 `cmd` 字段分派，client 回一个 JSON 回应后关闭连接。目前只有 `post`：client 把消息写进 outbox 就回 `msg_id`，不等 hub，hub 连不上也照样排队。`fednet client post` 只连 socket，不打开数据库；它的退出码是 0 成功、2 用法错误或请求不合法、3 没有权限连 socket、4 连不上 client，其它失败是 1。client 与 hub 之间的 payload 是一个 JSON 对象，`type` 字段说明它是什么，格式在 [`internal/payload`](../internal/payload)，两端共用；post 的 payload 带线程 key 和正文。

## 3. 不变量

存储层：

1. hub outbox 的 `seq` 只增不减，outbox 删空、库重开之后也不回退。[`TestHubOutboxSeqNeverGoesBack`](../internal/store/store_test.go)
2. inbox 按 `msg_id` 去重：同一条消息重复送达只入库一次，已交付的也不会再变回未交付。[`TestInboxDedup`](../internal/store/store_test.go)
3. 迁移按库里记的版本号只跑一次，重复打开不重复执行。[`TestMigrate`](../internal/store/store_test.go)；从旧版本的库升级上来不丢数据。[`TestHubUpgradeKeepsData`](../internal/store/store_test.go)
4. 线程归属只在没有记录时登记：同一个新线程的几条消息同时到达，只登记一个归属，消息都送到它。[`TestClaimAndEnqueueConcurrent`](../internal/store/store_test.go)、[`TestRouteConcurrentFirstMessages`](../internal/route/route_test.go)
5. 首次登记归属和写进 outbox 在同一个事务里：写 outbox 失败，归属也不留下。[`TestClaimAndEnqueueAtomic`](../internal/store/store_test.go)
6. 回复只送归属机器，channel 的默认机器改了也不变；没有归属的回复不送；只有显式改派才改归属。[`TestRoute`](../internal/route/route_test.go)、[`TestOwner`](../internal/store/store_test.go)

通道：

7. 下行消息先写进 client 的 inbox 再 ack，写不进就不 ack；hub 收到 ack 才删 outbox。[`TestClientStoresBeforeAck`](../internal/link/link_test.go)、[`TestDownlinkStoresOnceAndAcks`](../internal/link/link_test.go)
8. 没 ack 的下行消息重连后再发，ack 过的不再发；同一条消息再送一次，inbox 里还是一条。[`TestHubResendsUntilAcked`](../internal/link/link_test.go)、[`TestClientStoresReplayOnce`](../internal/link/link_test.go)、[`TestDownlinkResumesAfterDisconnect`](../internal/link/link_test.go)
9. 上行消息 hub 落盘后才回成功，client 收到成功才删 outbox，否则一直重发，请求没有回应也算失败。[`TestUplinkRetriesUntilStored`](../internal/link/link_test.go)、[`TestRequestsTimeOutAndRetry`](../internal/link/link_test.go)
10. 连接保持着但心跳停了一个租约期就判为离线，心跳恢复就判为在线；心跳一直发着就一直在线。[`TestOnlineFollowsHeartbeat`](../internal/link/link_test.go)
11. 超过上限的 payload 入队时就被拒绝，恰好到上限的两个方向都送得到。[`TestPayloadLimit`](../internal/link/link_test.go)
12. `Hub.Close` 之后没有下行连接留下，包括 Close 时正在握手的；关一条连接不会被这条连接正在处理的 ping 卡住。[`TestCloseRefusesHandshakeInFlight`](../internal/link/link_test.go)、[`TestClosingAConnectionDoesNotWaitOnPing`](../internal/link/link_test.go)

认证：

13. 只有登记过、未退役、凭证哈希相符的 client 能连上，下行和上行都一样；被拒绝的请求回 401，正文不说原因。[`TestIdentify`](../internal/auth/auth_test.go)
14. 凭证不进 hub 的日志、错误信息和任何命令的输出；hub 只存它的哈希。[`TestIdentify`](../internal/auth/auth_test.go)、[`TestIdentifyErrors`](../internal/auth/auth_test.go)、[`TestHubAndClient`](../cmd/fednet/main_test.go)
15. 凭证文件只有所有者能读，已有的不会被覆盖，被别人能读的凭证文件不用。[`TestCredentialFile`](../internal/auth/auth_test.go)
16. 退役只打标记不删记录；重新登记替换哈希、解除退役、保留版本。[`TestRegistry`](../internal/store/store_test.go)
17. 登记之后 client 连上 hub，hub 在它连上之前排队的消息送到它的 inbox，hub 记下它报的版本。[`TestHubAndClient`](../cmd/fednet/main_test.go)

本机 socket：

18. `post` 写进 client 的 outbox 就返回 `msg_id`；hub 连不上时照样返回，连上后送到 hub。[`TestPost`](../internal/local/local_test.go)、[`TestPostWhileHubDown`](../internal/local/local_test.go)
19. socket 不给组时只有 client 的用户能连 (0600)，给了组时组里的用户也能连 (0660)。同一个路径同一时间只归一个 client：已有 client 持锁时 (包括它还没发布 socket 的时候) 第二个 client 起不来，几个 client 同时启动只有一个成功，连接都进它；持锁的 client 关闭后新 client 能接手；旧 socket 被替换，别的文件不被替换。[`TestListen`](../internal/local/local_test.go)、[`TestListenWhileAnotherIsStarting`](../internal/local/local_test.go)、[`TestListenConcurrent`](../internal/local/local_test.go)、[`TestListenRefusesAFile`](../internal/local/local_test.go)
20. agent 经 `fednet client post` 发的消息，hub 的 inbox 收到时 `type` 是 `post`，线程 key 和正文不变。[`TestHubAndClient`](../cmd/fednet/main_test.go)
21. `fednet client post` 用法错误退 2，没有权限连 socket 退 3，连不上 client 退 4。[`TestRun`](../cmd/fednet/main_test.go)、[`TestPostDenied`](../cmd/fednet/main_test.go)

仓库层面：

22. 这份文件不超过 200 行。[`design-length.test.sh`](../.github/scripts/design-length.test.sh)

## 4. 接口

- 命令行：`fednet hub`、`fednet hub register`、`fednet hub revoke`、`fednet client`、`fednet client init`、`fednet client post`、`fednet version`，参数以 `fednet` 不带参数时打印的用法为准 ([`cmd/fednet/main.go`](../cmd/fednet/main.go))。
- HTTP：只给 client 用，路径和帧格式在 [`internal/link/link.go`](../internal/link/link.go)。
- 本机 socket：只给本机的 agent 用，请求和回应的格式在 [`internal/local/local.go`](../internal/local/local.go)。
- payload：hub 与 client 之间消息的内容，格式在 [`internal/payload/payload.go`](../internal/payload/payload.go)。

## 5. 已知问题与下一步

- 新建一个只有用户本人的 Slack workspace，在里面建 fednet 用的 Slack app。
- 写一个最小程序，用 slack-go 的 Socket Mode 连这个 workspace，连续跑一天，看断线重连和 ack 是否可靠，再决定 hub 用不用 slack-go。
- 建 hub 机器。
- 两个进程同时首次打开同一个还不存在的库文件，有一方可能拿到 `SQLITE_BUSY`；文件已存在时没有观察到。client 的库现在只有常驻进程打开；hub 的库在 hub 首次启动的同时跑 `fednet hub register` 时仍可能碰到。
- hub 认身份时查库失败也回 401，client 会按退避重试。
- 不给版本过旧的 client 派消息，留给不停机升级那一步；client 把收到的消息交给本机 agent 的钩子，是阶段 2。

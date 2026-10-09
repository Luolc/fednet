# fednet 设计

这份文档只描述 fednet 当前的实际状态，状态变了就原地修改。决定和理由写在做出决定的那个 PR 里。只有维护者发起的 design 修订才改这份文件；其它 PR 不改它，只在 PR 描述的「Design impact」里写明对它的影响。

## 1. 是什么，不是什么

fednet 让用户在 Slack 线程里和各台机器上的 coding agent 打交道：在线程里交代一件事，由某台机器上的 agent 去做，中途的问题和最后的结果都回到同一个线程里。

计划只有一个二进制 `fednet`，分两个子命令：`fednet hub` 跑在一台固定的 hub 机器上，负责和 Slack 的连接；`fednet client` 跑在每台 agent 机器 (工作站、数据机) 上，主动连到 hub，把消息交给本机的 agent。

**当前状态：`fednet hub` 和 `fednet client` 都能跑起来，client 凭登记过的凭证连上 hub，hub 排队的消息送到 client 的 inbox 后，由 client 执行配置的钩子命令交给本机的 agent，交不出去的进死信；本机的 agent 可以用 `fednet client post` 经 client 往线程发消息，hub 收到后只落进 inbox；agent 还可以经 client 请 hub 当场回答：读一个线程、开一个线程、列出本机的线程、读写 channel 的描述、接管一个线程、列出用户名单、给名单上的用户发私信。Slack 接口已经有了经 Slack Web API 的真实现，但 `fednet hub` 还没用上它，要用 Slack 的请求只在测试里对着假实现跑通；线程路由没有被子命令用到。把 hub inbox 里的 post 发到 Slack 的组件和经 Slack incoming webhook 的报警组件也有了，同样还没接进 `fednet hub` 和 `fednet client`，只在测试里跑通。** 下面各节照实写，不描述还不存在的东西。

fednet 不是：

- agent 的运行时。agent 由 herdr 启动和管理，fednet 只负责把消息送到它那里。
- 模型网关。fednet 不管模型账号的登录，也不转发模型流量。
- 任务看板。待办和尚未落实的设计在 Linear 上，不在这个仓里。

## 2. 组成部分

只有一个 Go 写的二进制 `fednet` ([`cmd/fednet`](../cmd/fednet))，用标准库 `flag` 分派子命令。`fednet hub` 在显式给出的地址上起 HTTP 服务，只挂通道的 handler，用登记表认 client，可选的 JSON 配置文件 (`-config`) 写明每个 channel 允许哪些 client 开线程，以及用户名单；`fednet hub register` 与 `fednet hub revoke` 在 hub 机器上改登记表，`fednet hub reassign` 把一台 client 的线程整批改给另一台。`fednet client init` 生成本机凭证，`fednet client` 读凭证连上 hub 跑通道循环，收到的下行消息落进 inbox，命令行末尾给了钩子命令时每条消息再交给它，同时在本机开一个 unix socket 给 agent 用；`fednet client post`、`read-thread`、`open-thread`、`threads`、`adopt`、`channel-context`、`users`、`dm` 经这个 socket 把请求交给 client。`version` 打印构建时注入的版本号，未注入时打印 `dev`。tailnet 上的流量由 WireGuard 加密，hub 与 client 之间走明文 HTTP，不加 TLS。

存储层在 [`internal/store`](../internal/store)，用纯 Go 的 SQLite 驱动 `modernc.org/sqlite`，hub 和 client 各一个库文件、各一套表。hub 端有发给每台 client 的 outbox (按 `seq` 续传、按 ack 清理，记入队时间，可以查某台 client 有没有排队超过给定时长的消息)、收上行消息的 inbox (记下每条来自哪台 client，读未交付的消息时一并读出) 和线程归属表；client 端有收下行消息的 inbox (每行带钩子的尝试次数、下次尝试时间和交付时间)、钩子放弃的消息所在的死信表，和上行的 outbox。两端的 inbox 都按 `msg_id` 去重，client 端连死信一起算。hub 端另有 client 登记表：client id、凭证的 SHA-256、是否已退役、client 最近一次连接时报的版本，可以列出所有未退役的 client。

线程路由在 [`internal/route`](../internal/route)，新线程和已有线程里的回复分两个入口。新线程取这个 channel 的默认机器 (channel 到机器的对应目前由调用方用结构体传入)，登记为归属并写进它的 outbox，两步在同一个事务里；线程已有归属 (例如上游重发了第一条消息) 就送归属机器；既没有归属、channel 也没配默认机器，就不送，返回错误。回复写进归属机器的 outbox；线程没有归属 (例如 fednet 上线前就有的线程) 就不送，返回错误。不管机器在不在线都照样入队。归属只有显式改派才会变：改一个线程 (`adopt`)，或把一台机器的线程整批改给另一台 (`reassign`)。

通道在 [`internal/link`](../internal/link)，hub 端是一个 HTTP handler，client 端是一个常驻的循环。下行 (hub → client) 是 client 主动连到 hub 的 WebSocket (`github.com/coder/websocket`)：连上后 hub 先把这台 client outbox 里所有未 ack 的消息按 `seq` 发一遍，之后有新入队的就推；client 每收到一条先写进 inbox，再回一个累积的 ack，hub 收到 ack 才把 outbox 里 `seq` 不大于它的删掉。上行 (client → hub) 是一个 HTTP POST，一条消息一个请求，带 `msg_id`；hub 连同来源 client 一起写进 inbox 后才回 204，client 收到 204 才把这条从 outbox 删掉，否则按退避重发。一条消息的 payload 最大 256 KiB，入队时就拒绝超过的，两端的帧和请求体上限按它定。每次拨号和每个上行请求都有自己的超时。断线后 client 用有上限、带随机抖动的指数退避重连，重连后按上面的下行规则续传。client 每隔一个心跳间隔发一个 WebSocket ping，hub 在内存里记每台 client 最近一次心跳的时间，一个租约期内有心跳就算在线。另有一条同步的请求通道：client 发一个 HTTP POST，hub 当场回答，不排队；hub 连不上、或者在超时之内没有回答，请求立即失败。hub 拒绝的请求分三种 (请求不合法、不允许、要的东西不存在)，各用一个 HTTP 状态码，拒绝的原因原样回给 client；其它失败的原因只进 hub 的日志，client 只知道失败了。client 每个请求都带请求头 `Fednet-Client` 里的 id，hub 端认身份的函数是可以替换的，由下面的认证接上。

hub 怎么回答请求在 [`internal/hubapi`](../internal/hubapi)，请求和回答的格式也在这里，两端共用。要用 Slack 的请求经 hub 侧的 Slack 接口 ([`internal/slack`](../internal/slack)) 去做，Slack token 只在 hub 上，client 永远拿不到。这个接口只有 hub 用到的方法：读一个线程的消息，在 channel 里发一条消息开线程，在线程里以某台机器的名义回复，删一条消息，读写 channel 的 purpose，给一个用户发私信；它有两个实现：一个是测试用的假实现；另一个经 `github.com/slack-go/slack` 调 Slack 的 Web API，bot token 由调用方传入，Slack 限速时按它给的 `Retry-After` 等待后重试，重试有次数上限，单次等待也有上限，超过的不等、直接失败。`fednet hub` 还没用上真实现，这些请求都失败。线程 key 是 channel 与线程第一条消息的 ts，中间用 `/` 连起来。`read-thread` 由 hub 代读线程；`open-thread` 只在 hub 配置允许调用方的 channel 里开线程，hub 先在 Slack 发出第一条消息，再把新线程登记为调用方所有，登记失败时删掉刚发的那条消息，再向调用方报错，删也失败时线程留在 Slack 里、没有归属，错误里写明这一点；`threads` 列出归调用方的线程；`channel-context get` / `set` 读写 channel 的描述，描述就是 Slack 里这个 channel 的 purpose；`adopt` 把一个已有归属的线程改归发请求的 client，没有归属的线程不接管。用户名单是 hub 配置里的一组 Slack 用户 id，每个可以带一个给 agent 看的名字；`users` 列出名单上每个人的 id 和名字；`dm` 给名单上的一个人发私信，不属于任何线程，名单之外的人一律拒绝、不发到 Slack。`dm` 和 `open-thread` 一样是同步请求，hub 在 Slack 发出后才回答，hub 连不上时立即失败、不排队。

钩子在 [`internal/hook`](../internal/hook)。client 把 inbox 里未交付的消息按到达顺序、一次一条交给配置里的命令：把消息写成一个 JSON 事件文件 (`msg_id` 加原样内嵌的 payload)，路径作为最后一个参数，用参数数组直接执行，不经过 shell；环境变量只有命令行明确放行的几个 (`PATH`、`HOME` 加每个 `-hook-env`)，client 自己的环境不带过去。钩子跑在自己的进程组里，它一退出，不论结果，整个进程组就被杀掉，所以钩子不能留下后台进程，长命的东西要交给守护进程；每次执行有超时，超时同样杀整个进程组。钩子本身由 client 回收，被杀的后代由 init 回收：client 不是 subreaper。钩子没起来 (事件文件写不出、命令起不来) 是 client 这边的事，不算尝试，消息等下一轮。退出码 0 且没留下握着 stderr 的进程，才标记已交付；失败或超时的留在 inbox，等逐次加倍、有上限的间隔后重试，到上限转进死信表并记一条带 `msg_id` 和原因的日志，给了报警 webhook 时再报一条带 `msg_id` 和原因的警，不删、不再自动重试。每次执行的结果先写进库，再执行下一条；库暂时写不进时结果留在内存里反复补写，补写成功之前不执行任何新消息。被 client 关停打断的那次不计入尝试；client 死在钩子退出之后、结果写进库之前，下次启动会再执行一次，所以钩子要按 `msg_id` 幂等。钩子异步于通道跑，不卡住收消息。已交付的行保留一个保留期后清理。

出站在 [`internal/outbound`](../internal/outbound)：hub 按到达顺序把 inbox 里未交付的 post 发到它的线程里，每条开头用 Slack 的 context 区块标出来源机器，正文放在 markdown 区块里；超过约 4000 字的按字数拆成同一线程里的连续几条，尽量在换行处断开。发到 Slack 之后才标记已交付。Slack 不收的 (限速的等待由 Slack 接口自己做完之后仍然失败) 留在 inbox，过一会儿再试，它后面的也不抢先发；拆开的 post 已经发出的段落记在内存里，重试时不再发。永远发不出去的 (payload 不是 post、线程 key 不合法、正文为空、线程在 Slack 里不存在) 记日志、报警，然后标记已交付，不挡后面的。

报警在 [`internal/alert`](../internal/alert)：经 Slack incoming webhook 直接发到报警 channel，不经 hub 的 Slack 连接，所以 hub 坏了 client 照样能报。webhook URL 由调用方传入，每条报警开头标出发报警的机器；URL 不进错误和日志。hub 侧的检查在 [`internal/watch`](../internal/watch)，定期看两件事：hub 与 Slack 断开超过阈值 (默认 5 分钟)；某台 client 离线、且有消息为它排队超过阈值 (默认 10 分钟)，这时除了报警，还在每个有消息为它排队的线程里说一声。同一件事只报一次，恢复之后再出现才再报；报警发不出去的下一轮再试。hub 与 Slack 的连接状态经一个小接口 (`SlackLink`，断开了多久) 交给它。两个阈值写在 hub 配置文件的 `alerts` 里，`fednet hub` 还没用上。

认证在 [`internal/auth`](../internal/auth)。client 自己生成 256 bit 的随机凭证，存在本机一个只有所有者能读 (0600) 的文件里，连同 client id 一起；hub 只登记它的 SHA-256。每个请求带 `Authorization: Bearer <凭证>` 和 `Fednet-Version`，hub 算哈希、与登记表常数时间比对，并记下版本；未登记、已退役、凭证不对的一律回 401，正文固定是 `unauthorized`，原因只进 hub 的日志，凭证不进日志、错误信息和任何命令的输出。登记 (`fednet hub register`) 写入 id 与哈希，重复登记替换哈希并解除退役；退役 (`fednet hub revoke`) 让这台 client 的凭证失效，已经建立的连接要到断开才生效。Tailscale `WhoIs` 核对来源节点还没有接。

本机 socket 在 [`internal/local`](../internal/local)。agent 拿不到 client 的凭证，只能经这个 socket 把请求交给常驻的 client 进程。socket 的路径由参数给出；不给组时权限是 0600，只有 client 所在的 Unix 用户能连，给了组 (`-socket-group`) 时是 0660，组里的用户也能连。socket 先建在一个只有 client 用户能进的临时目录里，权限和组设好后再改名到给定的路径。一个路径同一时间只归一个 client：启动时先对 socket 旁边的 `<socket>.lock` 加排他的 `flock`，拿不到就不启动，拿到后一直持有到进程退出 (进程死了由内核释放)。检查和改名都在持锁之后，所以路径上已有的 socket 一定是旧进程留下的，直接替换；路径上是别的文件时不启动。一个连接只承载一个请求：调用方写一个 JSON 请求，按 `cmd` 字段分派，client 回一个 JSON 回应后关闭连接。`post` 由 client 自己回答：把消息写进 outbox 就回 `msg_id`，不等 hub，hub 连不上也照样排队。其余命令由 client 经请求通道转给 hub，把 hub 的回答或拒绝的原因带回来，hub 的那一段另有一个比 socket 请求短的超时。`fednet client` 的这几条命令只连 socket，不打开数据库；退出码是 0 成功、2 用法错误或请求不合法、3 没有权限连 socket 或 hub 不允许、4 连不上 client 或 hub，其它失败 (例如线程不存在) 是 1。client 与 hub 之间的 payload 是一个 JSON 对象，`type` 字段说明它是什么，格式在 [`internal/payload`](../internal/payload)，两端共用；post 的 payload 带线程 key 和正文。

## 3. 不变量

存储层：

1. hub outbox 的 `seq` 只增不减，outbox 删空、库重开之后也不回退。[`TestHubOutboxSeqNeverGoesBack`](../internal/store/store_test.go)
2. inbox 按 `msg_id` 去重：同一条消息重复送达只入库一次，已交付的也不会再变回未交付。[`TestInboxDedup`](../internal/store/store_test.go)
3. 迁移按库里记的版本号只跑一次，重复打开不重复执行。[`TestMigrate`](../internal/store/store_test.go)；从旧版本的库升级上来不丢数据。[`TestHubUpgradeKeepsData`](../internal/store/store_test.go)、[`TestClientUpgradeKeepsData`](../internal/store/store_test.go)
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

请求通道：

22. 请求不排队：hub 连不上或者不回答，请求在超时之内失败，client 的 outbox 里不留下东西。[`TestRequestUnreachable`](../internal/link/link_test.go)、[`TestAskWhileHubDown`](../internal/local/local_test.go)
23. hub 拒绝的原因原样回到 client，其它失败的原因不出 hub；hub 不认凭证的请求算不允许。[`TestRequest`](../internal/link/link_test.go)、[`TestRequestUnauthorized`](../internal/link/link_test.go)
24. `adopt` 只改已有归属的线程，改成发请求的 client；`hub reassign` 把一台 client 的线程全部改给另一台。`open-thread` 开的线程归调用方，配置不允许调用方的 channel 不开，也不发到 Slack；已有归属的线程不能再登记；`threads` 只列调用方的线程。`dm` 只发给用户名单上的人，名单之外的不发到 Slack；`users` 按 id 排序列出名单。[`TestAdopt`](../internal/hubapi/hubapi_test.go)、[`TestOpenThread`](../internal/hubapi/hubapi_test.go)、[`TestThreads`](../internal/hubapi/hubapi_test.go)、[`TestDM`](../internal/hubapi/hubapi_test.go)、[`TestUsers`](../internal/hubapi/hubapi_test.go)、[`TestClaimAndThreads`](../internal/store/store_test.go)、[`TestHubRequests`](../cmd/fednet/request_test.go)
25. agent 经 socket 发的 `read-thread`、`open-thread`、`threads`、`channel-context`、`adopt`、`users`、`dm` 走真的认证到 hub，拿回 hub 的回答；线程 key 不合法或 `dm` 缺用户退 2，线程不存在退 1，在配置不允许的 channel 开线程、给名单之外的人发私信、client 已退役都退 3，hub 停了退 4。[`TestHubRequests`](../cmd/fednet/request_test.go)、[`TestAsk`](../internal/local/local_test.go)

钩子：

26. 在 client 存活期间，每条进 inbox 的消息钩子至多成功执行一次：重复下发的不再执行，成功过的不再执行，结果写不进库时留在内存里补写、不重跑。client 死在钩子退出之后、写库之前的那条会再执行一次。[`TestRunsOncePerMessage`](../internal/hook/hook_test.go)、[`TestRetriesUntilSuccess`](../internal/hook/hook_test.go)、[`TestOutcomeKeptWhenStoreFails`](../internal/hook/hook_test.go)、[`TestHubAndClient`](../cmd/fednet/main_test.go)
27. 失败或超时的消息留在 inbox，按逐次加倍、有上限的间隔重试；到上限转进死信，不删、不再自动重试，重复下发的也不再执行。重试记录或死信写不进库时同样不重跑，上限不会被突破；钩子没起来不算尝试。[`TestRetriesUntilSuccess`](../internal/hook/hook_test.go)、[`TestDeadLetterAtLimit`](../internal/hook/hook_test.go)、[`TestOutcomeKeptWhenStoreFails`](../internal/hook/hook_test.go)、[`TestNotRunIsNotAnAttempt`](../internal/hook/hook_test.go)、[`TestClientInboxRetryAndDeadLetter`](../internal/store/store_test.go)
28. 钩子退出后，不论成功、失败还是超时，它的整个进程组都被杀掉并回收，不留残留进程，僵尸也算残留；退出 0 但留下进程握着 stderr 的算失败。[`TestTimeoutKillsProcessGroup`](../internal/hook/hook_test.go)、[`TestLeftoverProcessesAreKilled`](../internal/hook/hook_test.go)、[`TestResidueCountsZombies`](../internal/hook/hook_test.go)
29. 钩子的环境变量只有显式给出的那些，client 自己的环境不带过去。[`TestEnvIsOnlyWhatIsGiven`](../internal/hook/hook_test.go)、[`TestHubAndClient`](../cmd/fednet/main_test.go)
30. client 重启后未交付的消息继续执行，尝试次数保留；被关停打断的那次不计。[`TestResumesAfterRestart`](../internal/hook/hook_test.go)、[`TestShutdownDoesNotCountAsAttempt`](../internal/hook/hook_test.go)
31. 已交付的行在保留期内仍去重，过了保留期才清理。[`TestClientInboxPrune`](../internal/store/store_test.go)

出站与报警：

32. post 发到 Slack 之后才标记已交付；发不出去的留在 inbox，它后面的不抢先发，重试时已经发出的段落不再发。[`TestFailedPostIsKeptAndRetried`](../internal/outbound/outbound_test.go)、[`TestRun`](../internal/outbound/outbound_test.go)
33. 每条 post 发在它的线程里，开头标出来源机器；超长的拆成同一线程里的连续几条，顺序不变。永远发不出去的报警后标记已交付，不挡后面的。[`TestPostNamesTheMachine`](../internal/outbound/outbound_test.go)、[`TestLongPostIsSplit`](../internal/outbound/outbound_test.go)、[`TestSplit`](../internal/outbound/outbound_test.go)、[`TestPermanentFailureIsAlertedAndSkipped`](../internal/outbound/outbound_test.go)、[`TestWebPostReplyAndDelete`](../internal/slack/web_test.go)
34. `open-thread` 登记归属失败时，刚发的消息被删掉，调用方收到错误。[`TestOpenThreadUndoneWhenClaimFails`](../internal/hubapi/hubapi_test.go)
35. 每条死信报一次警，带 `msg_id` 和原因。hub 与 Slack 断开超过阈值报一次，恢复后再断再报；报警发不出去的下一轮再试。client 离线且有消息排队超过阈值时报一次警，在每个受影响的线程里说一声，回来后再离线再报。[`TestDeadLetterAlertsOnce`](../internal/hook/hook_test.go)、[`TestSlackDown`](../internal/watch/watch_test.go)、[`TestFailedAlertIsRetried`](../internal/watch/watch_test.go)、[`TestOfflineWithQueue`](../internal/watch/watch_test.go)
36. webhook URL 不进报警的错误和日志。[`TestSend`](../internal/alert/alert_test.go)、[`TestDeadLetterAlertsOnce`](../internal/hook/hook_test.go)

仓库层面：

37. 这份文件不超过 200 行。[`design-length.test.sh`](../.github/scripts/design-length.test.sh)

## 4. 接口

- 命令行：`fednet hub`、`fednet hub register`、`fednet hub revoke`、`fednet hub reassign`、`fednet client`、`fednet client init`、`fednet client post`、`fednet client read-thread`、`fednet client open-thread`、`fednet client threads`、`fednet client adopt`、`fednet client channel-context`、`fednet client users`、`fednet client dm`、`fednet version`，参数以 `fednet` 不带参数时打印的用法为准 ([`cmd/fednet/main.go`](../cmd/fednet/main.go))。
- hub 配置文件 (开线程的权限、用户名单和报警阈值)：格式在 [`cmd/fednet/main.go`](../cmd/fednet/main.go) 的 `hubConfig`，用法里也有一个例子。
- HTTP：只给 client 用，路径和帧格式在 [`internal/link/link.go`](../internal/link/link.go)。
- 本机 socket：只给本机的 agent 用，请求和回应的格式在 [`internal/local/local.go`](../internal/local/local.go)。
- payload：hub 与 client 之间消息的内容，格式在 [`internal/payload/payload.go`](../internal/payload/payload.go)。
- 请求通道：client 发给 hub 的请求和 hub 的回答，格式在 [`internal/hubapi/hubapi.go`](../internal/hubapi/hubapi.go)。
- Slack：hub 用到的 Slack 接口在 [`internal/slack/slack.go`](../internal/slack/slack.go)。
- 报警：webhook 在 [`internal/alert/alert.go`](../internal/alert/alert.go)；hub 侧检查的阈值和它要的 Slack 连接状态接口 `SlackLink` 在 [`internal/watch/watch.go`](../internal/watch/watch.go)。
- 钩子：命令怎么被调用、它要守的契约、事件文件的格式、重试与超时的默认值在 [`internal/hook/hook.go`](../internal/hook/hook.go)。

## 5. 已知问题与下一步

- 新建一个只有用户本人的 Slack workspace，在里面建 fednet 用的 Slack app。
- 写一个最小程序，用 slack-go 的 Socket Mode 连这个 workspace，连续跑一天，看断线重连和 ack 是否可靠，再决定 hub 用不用 slack-go。
- 建 hub 机器。
- 两个进程同时首次打开同一个还不存在的库文件，有一方可能拿到 `SQLITE_BUSY`；文件已存在时没有观察到。client 的库现在只有常驻进程打开；hub 的库在 hub 首次启动的同时跑 `fednet hub register` 时仍可能碰到。
- hub 认身份时查库失败也回 401，client 会按退避重试。
- 入站消息还没接；接上后只放行用户名单上的人，名单就是 `users` 列出的那份。
- 不给版本过旧的 client 派消息，留给不停机升级那一步。
- 看过死信的原因后手动重放的命令还没有。
- 出站、报警的组件还没接进 `fednet hub` 和 `fednet client`：Slack token 与 webhook URL 从哪来、hub 与 Slack 的连接状态由谁提供，留到接 Slack 的那一步。
- 拆开发的长 post 发到一半时 hub 重启，重启后前面的段落会再发一次。
- `open-thread` 开线程的第一条消息不标来源机器。

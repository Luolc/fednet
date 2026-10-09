# fednet 设计

这份文档只描述 fednet 当前的实际状态，状态变了就原地修改。决定和理由写在做出决定的那个 PR 里。只有维护者发起的 design 修订才改这份文件；其它 PR 不改它，只在 PR 描述的「Design impact」里写明对它的影响。

## 1. 是什么，不是什么

fednet 让用户在 Slack 线程里和各台机器上的 coding agent 打交道：在线程里交代一件事，由某台机器上的 agent 去做，中途的问题和最后的结果都回到同一个线程里。

计划只有一个二进制 `fednet`，分两个子命令：`fednet hub` 跑在一台固定的 hub 机器上，负责和 Slack 的连接；`fednet client` 跑在每台 agent 机器 (工作站、数据机) 上，主动连到 hub，把消息交给本机的 agent。

**当前状态：`fednet hub` 和 `fednet client` 都能跑起来，client 凭登记过的凭证连上 hub，hub 排队的消息送到 client 的 inbox 后，由 client 执行配置的钩子命令交给本机的 agent，交不出去的进死信，并经 hub 报警；本机的 agent 可以用 `fednet client post` 经 client 往线程发消息；agent 还可以经 client 请 hub 当场回答：读一个线程、开一个线程、列出本机的线程、读写 channel 的描述、接管一个线程、列出用户名单、给名单上的用户发私信。给了 Slack 的两个 token 文件时，hub 用 Socket Mode 收人在 Slack 里发的消息，按线程路由送给 client，再把 client 发的 post 发到线程里，给了报警 webhook 时还会报警；没给时 hub 只跑 client 用的 HTTP 服务，post 只落进 inbox，要用 Slack 的请求都失败。hub 和 client 都能不停机换成新版本的进程：新进程接过监听的端口和 socket 之后旧进程才退出，新进程起不来就还是旧的在服务；hub 不给版本过旧的 client 派消息。这些都只在测试里对着假的 Slack 跑通，还没有连真的 Slack workspace 跑过。** 下面各节照实写，不描述还不存在的东西。

fednet 不是：

- agent 的运行时。agent 由 herdr 启动和管理，fednet 只负责把消息送到它那里。
- 模型网关。fednet 不管模型账号的登录，也不转发模型流量。
- 任务看板。待办和尚未落实的设计在 Linear 上，不在这个仓里。

## 2. 组成部分

只有一个 Go 写的二进制 `fednet` ([`cmd/fednet`](../cmd/fednet))，用标准库 `flag` 分派子命令。`fednet hub` 在显式给出的地址上起 HTTP 服务，只挂通道的 handler，用登记表认 client，可选的 JSON 配置文件 (`-config`) 写明每个 channel 的默认机器、允许哪些 client 开线程、私信的默认机器、用户名单和报警阈值。Slack 的 app-level token、bot token 和报警 webhook 的 URL 各从一个文件读 (`-slack-app-token-file`、`-slack-bot-token-file`、`-alert-webhook-file`)，去掉首尾空白后使用，不读环境变量；两个 Slack 文件要么都给、要么都不给。都给了时，hub 在 HTTP 服务之外同时跑入站、出站，再给了 webhook 时还跑报警检查；Socket Mode 因 token 不对退出、HTTP 服务出错或者进程收到停止信号时，全部按顺序停下，前两种退出码是 1。部署用的 systemd 单元模板和示例配置在 [`deploy`](../deploy)。`fednet hub register` 与 `fednet hub revoke` 在 hub 机器上改登记表，`fednet hub reassign` 把一台 client 的线程整批改给另一台；给了管理 socket (`-admin-socket`) 时，`fednet hub handoff` 经它让 hub 换成新进程。`fednet client init` 生成本机凭证，`fednet client` 读凭证连上 hub 跑通道循环，收到的下行消息落进 inbox，命令行末尾给了钩子命令时每条消息再交给它，同时在本机开一个 unix socket 给 agent 用；`fednet client post`、`read-thread`、`open-thread`、`threads`、`adopt`、`channel-context`、`users`、`dm` 经这个 socket 把请求交给 client，`fednet client handoff` 经它让 client 换成新进程。`version` 打印构建时注入的版本号，未注入时打印 `dev`。tailnet 上的流量由 WireGuard 加密，hub 与 client 之间走明文 HTTP，不加 TLS。

存储层在 [`internal/store`](../internal/store)，用纯 Go 的 SQLite 驱动 `modernc.org/sqlite`，hub 和 client 各一个库文件、各一套表。hub 端有发给每台 client 的 outbox (按 `seq` 续传、按 ack 清理，记入队时间，可以查某台 client 有没有排队超过给定时长的消息)、收上行消息的 inbox (记下每条来自哪台 client，读未交付的消息时一并读出) 和线程归属表；client 端有收下行消息的 inbox (每行带钩子的尝试次数、下次尝试时间和交付时间)、钩子放弃的消息所在的死信表，和上行的 outbox。两端的 inbox 都按 `msg_id` 去重，client 端连死信一起算。hub 端另有 client 登记表：client id、凭证的 SHA-256、是否已退役、client 最近一次连接时报的版本，可以列出所有未退役的 client。hub 端还记入站的 Slack 消息：一张表记收过哪些消息 (channel 加消息的 ts，实时收到的再记带来它的事件 id，两个键都唯一)，一张表记和 Slack 历史的进度 (最后看到的消息 ts；一次没做完的补拉从哪开始)。记一条入站消息和为它写 outbox、登记归属在同一个事务里：存储层把一个绑在事务上的 hub 库交给调用方，调用方在它上面做路由。

线程路由在 [`internal/route`](../internal/route)，新线程和已有线程里的回复分两个入口。新线程取这个 channel 的默认机器 (channel 到机器的对应来自 hub 配置)，登记为归属并写进它的 outbox，两步在同一个事务里；私信里的新线程取 hub 配置里私信的默认机器，走法一样；线程已有归属 (例如上游重发了第一条消息) 就送归属机器；既没有归属、channel 也没配默认机器，就不送，返回错误。回复写进归属机器的 outbox；线程没有归属 (例如 fednet 上线前就有的线程) 就不送，返回错误。不管机器在不在线都照样入队。归属只有显式改派才会变：改一个线程 (`adopt`)，或把一台机器的线程整批改给另一台 (`reassign`)。

通道在 [`internal/link`](../internal/link)，hub 端是一个 HTTP handler，client 端是一个常驻的循环。下行 (hub → client) 是 client 主动连到 hub 的 WebSocket (`github.com/coder/websocket`)：连上后 hub 先把这台 client outbox 里所有未 ack 的消息按 `seq` 发一遍，之后有新入队的就推；不经 hub 的入队接口、直接在存储层写进 outbox 的 (入站就是这样)，写完由调用方唤醒所有连着的下行连接，各自去 outbox 里取；client 每收到一条先写进 inbox，再回一个累积的 ack，hub 收到 ack 才把 outbox 里 `seq` 不大于它的删掉。上行 (client → hub) 是一个 HTTP POST，一条消息一个请求，带 `msg_id`；hub 连同来源 client 一起写进 inbox、通知出站组件之后才回 204，client 收到 204 才把这条从 outbox 删掉，否则按退避重发。一条消息的 payload 最大 256 KiB，入队时就拒绝超过的，两端的帧和请求体上限按它定。每次拨号和每个上行请求都有自己的超时。断线后 client 用有上限、带随机抖动的指数退避重连，重连后按上面的下行规则续传。client 每隔一个心跳间隔发一个 WebSocket ping，hub 在内存里记每台 client 最近一次心跳的时间，一个租约期内有心跳就算在线。另有一条同步的请求通道：client 发一个 HTTP POST，hub 当场回答，不排队；hub 连不上、或者在超时之内没有回答，请求立即失败。hub 拒绝的请求分三种 (请求不合法、不允许、要的东西不存在)，各用一个 HTTP 状态码，拒绝的原因原样回给 client；其它失败的原因只进 hub 的日志，client 只知道失败了。client 每个请求都带请求头 `Fednet-Client` 里的 id 和 `Fednet-Version` 里的版本，hub 端认身份的函数是可以替换的，由下面的认证接上。hub 对下行连接还看版本：不服务的版本在握手时就拒绝 (HTTP 426，正文说明要求)，这台 client 的消息留在 outbox 里等它升级，上行和请求照常；client 把拒绝的原因记进日志，按退避重试。

hub 怎么回答请求在 [`internal/hubapi`](../internal/hubapi)，请求和回答的格式也在这里，两端共用。要用 Slack 的请求经 hub 侧的 Slack 接口 ([`internal/slack`](../internal/slack)) 去做，Slack token 只在 hub 上，client 永远拿不到。这个接口只有 hub 用到的方法：读一个线程的消息，读一个 channel 某个时刻之后的顶层消息，列出 bot 所在的 channel 与私信会话，在 channel 里发一条消息开线程，在线程里以某台机器的名义回复，删一条消息，读写 channel 的 purpose，给一个用户发私信；它有两个实现：一个是测试用的假实现；另一个经 `github.com/slack-go/slack` 调 Slack 的 Web API，bot token 由调用方传入，Slack 限速时按它给的 `Retry-After` 等待后重试，重试有次数上限，单次等待也有上限，超过的不等、直接失败。`fednet hub` 给了 bot token 文件时用真实现，没给时这些请求都失败。线程 key 是 channel 与线程第一条消息的 ts，中间用 `/` 连起来。`read-thread` 由 hub 代读线程；`open-thread` 只在 hub 配置允许调用方的 channel 里开线程，hub 先在 Slack 发出第一条消息，再把新线程登记为调用方所有，登记失败时删掉刚发的那条消息，再向调用方报错，删也失败时线程留在 Slack 里、没有归属，错误里写明这一点；`threads` 列出归调用方的线程；`channel-context get` / `set` 读写 channel 的描述，描述就是 Slack 里这个 channel 的 purpose；`adopt` 把一个已有归属的线程改归发请求的 client，没有归属的线程不接管。用户名单是 hub 配置里的一组 Slack 用户 id，每个可以带一个给 agent 看的名字；`users` 列出名单上每个人的 id 和名字；`dm` 给名单上的一个人发私信，不属于任何线程，名单之外的人一律拒绝、不发到 Slack。`dm` 和 `open-thread` 一样是同步请求，hub 在 Slack 发出后才回答，hub 连不上时立即失败、不排队。

钩子在 [`internal/hook`](../internal/hook)。client 把 inbox 里未交付的消息按到达顺序、一次一条交给配置里的命令：把消息写成一个 JSON 事件文件 (`msg_id` 加原样内嵌的 payload)，路径作为最后一个参数，用参数数组直接执行，不经过 shell；环境变量只有命令行明确放行的几个 (`PATH`、`HOME` 加每个 `-hook-env`)，client 自己的环境不带过去。钩子跑在自己的进程组里，它一退出，不论结果，整个进程组就被杀掉，所以钩子不能留下后台进程，长命的东西要交给守护进程；每次执行有超时，超时同样杀整个进程组。钩子本身由 client 回收，被杀的后代由 init 回收：client 不是 subreaper。钩子没起来 (事件文件写不出、命令起不来) 是 client 这边的事，不算尝试，消息等下一轮。退出码 0 且没留下握着 stderr 的进程，才标记已交付；失败或超时的留在 inbox，等逐次加倍、有上限的间隔后重试，到上限转进死信表并记一条带 `msg_id` 和原因的日志，再把一条带 `msg_id` 和原因的 `alert` 写进上行的 outbox，由 hub 报警；不删、不再自动重试。每次执行的结果先写进库，再执行下一条；库暂时写不进时结果留在内存里反复补写，补写成功之前不执行任何新消息。被 client 关停打断的那次不计入尝试；client 死在钩子退出之后、结果写进库之前，下次启动会再执行一次，所以钩子要按 `msg_id` 幂等。钩子异步于通道跑，不卡住收消息。已交付的行保留一个保留期后清理。

出站在 [`internal/outbound`](../internal/outbound)：hub 按到达顺序把 inbox 里未交付的 post 发到它的线程里，每条开头用 Slack 的 context 区块标出来源机器，正文放在 markdown 区块里；超过约 4000 字的按字数拆成同一线程里的连续几条，尽量在换行处断开。发到 Slack 之后才标记已交付。Slack 不收的 (限速的等待由 Slack 接口自己做完之后仍然失败) 留在 inbox，过一会儿再试，它后面的也不抢先发；拆开的 post 已经发出的段落记在内存里，重试时不再发。永远发不出去的 (payload 不是 post、线程 key 不合法、正文为空、线程在 Slack 里不存在) 记日志、报警，然后标记已交付，不挡后面的。inbox 里还有 client 发来的 `alert`：出站把它以那台 client 的名义发到报警 webhook，发不出去的留在 inbox 等下一轮，后面的 post 照常发；hub 没配 webhook 时只记日志。出站平时每隔一段时间看一次 inbox，上行写进 inbox 或者入站往 inbox 放了 hub 的 post 时立刻看一次。

报警在 [`internal/alert`](../internal/alert)：经 Slack incoming webhook 直接发到报警 channel，不经 hub 的 Slack 连接，所以 hub 和 Slack 断了也能报。webhook URL 只放在 hub 上，不分发到 agent 机器；client 的报警经上行交给 hub，hub 停了的时候就报不出去。URL 由调用方传入，每条报警开头标出发报警的机器；URL 不进错误和日志。hub 侧的检查在 [`internal/watch`](../internal/watch)，定期看两件事：hub 与 Slack 断开超过阈值 (默认 5 分钟)；某台 client 离线、且有消息为它排队超过阈值 (默认 10 分钟)，这时除了报警，还在每个有消息为它排队的线程里说一声。还看每台 client 最近一次报的版本，hub 不服务的就报警，说明是哪台、什么版本、要求是什么，一个版本只报一次。同一件事只报一次，恢复之后再出现才再报；报警发不出去的下一轮再试。hub 与 Slack 的连接状态经一个小接口 (`SlackLink`，断开了多久) 交给它，由入站组件实现。两个阈值写在 hub 配置文件的 `alerts` 里。

入站在 [`internal/inbound`](../internal/inbound)。hub 用 Socket Mode 收 `message` 事件：每个事件先过滤，只留用户名单上的人发的、不带 `bot_id`、子类型是人说的话的 (普通消息、`/me`、带文件的消息、广播到 channel 的回复)；编辑、删除、有人加入、改 topic 这些子类型都不是；留下的在一个事务里记进收过的消息表并交给路由写进 outbox，事务提交了才向 Slack ack，所以 hub 死在 ack 之前 Slack 会重投，重投的按事件 id、按 channel 加 ts 都去重，只交一次。channel 里的顶层消息是新线程，连同 channel 的 purpose 一起交下去 (purpose 读不到就不带，agent 可以自己再问)；线程里的是回复；私信里每条顶层消息都算一个新线程，不带 purpose，之后在它下面的回复照归属走。hub 不认识的线程里来了回复 (断线那一刻在路上的两条消息，Slack 重投时次序不定)，先从 Slack 读这个线程的第一条，它在补拉的回看窗口里就先把它当新线程收进来，再处理回复；线程还是没有归属的回复记下、不送。新线程的 channel 没配默认机器时不送，hub 在同一个事务里往自己的 inbox 放一条 `post`，由出站以 `hub` 的名义发到线程里：「没有机器接这个 channel」，发不出去由出站重试。上传的文件只把文件名和链接列在正文末尾，不下载。送给 client 的 payload 类型是 `message`，带线程 key、正文、发消息的人的 Slack 用户 id、消息的 ts，新线程再带 channel 的 purpose。连接每次建立时，先停掉上一条连接还在跑的补拉，再定下这次补拉从哪开始 (最后看到的消息；有没做完的补拉就从它那里)，然后才处理这条连接的消息，所以实时消息先到也推不走补拉的起点；一次补拉只在它开始之后没有新连接时才清起点，上一条连接的补拉做完也清不掉新连接的；补拉在后台跑：对 bot 所在的每个 channel 和私信会话，从起点之后、最多回看 24 小时，读顶层消息的历史和每个有归属的线程的回复，走和实时收到的同一条路、同一套去重；起点全部读完才清掉，所以读到一半失败的补拉下次从同一处重来，hub 重启也从持久化的位置继续；还没看到过任何消息时不补。补拉失败按固定间隔重试。连接状态 (连着还是断着、从什么时候起、断了多久) 可以查，报警要的 `SlackLink` 就是它。每处理完一条消息，入站唤醒下行连接，并让出站立刻看一次 inbox。Socket Mode 的传输层是 `slack-go` 的 `socketmode`，它自己重连；凭证不对时退出。

认证在 [`internal/auth`](../internal/auth)。client 自己生成 256 bit 的随机凭证，存在本机一个只有所有者能读 (0600) 的文件里，连同 client id 一起；hub 只登记它的 SHA-256。每个请求带 `Authorization: Bearer <凭证>` 和 `Fednet-Version`，hub 算哈希、与登记表常数时间比对，并记下版本；未登记、已退役、凭证不对的一律回 401，正文固定是 `unauthorized`，原因只进 hub 的日志，凭证不进日志、错误信息和任何命令的输出。登记 (`fednet hub register`) 写入 id 与哈希，重复登记替换哈希并解除退役；退役 (`fednet hub revoke`) 让这台 client 的凭证失效。这两个命令是另起的进程，和 hub 只共用库文件，所以 hub 对每条开着的下行连接每隔一个心跳间隔 (默认 10 秒，和 client 的默认心跳间隔相同) 按握手时的请求头重新认一次，认不过就断开；上行和请求本来就每次都认。Tailscale `WhoIs` 核对来源节点还没有接。

本机 socket 在 [`internal/local`](../internal/local)。agent 拿不到 client 的凭证，只能经这个 socket 把请求交给常驻的 client 进程。socket 的路径由参数给出；不给组时权限是 0600，只有 client 所在的 Unix 用户能连，给了组 (`-socket-group`) 时是 0660，组里的用户也能连。socket 先建在一个只有 client 用户能进的临时目录里，权限和组设好后再改名到给定的路径。一个路径同一时间只归一个 client：启动时先对 socket 旁边的 `<socket>.lock` 加排他的 `flock`，拿不到就不启动，拿到后一直持有到进程退出 (进程死了由内核释放)。检查和改名都在持锁之后，所以路径上已有的 socket 一定是旧进程留下的，直接替换；路径上是别的文件时不启动。一个连接只承载一个请求：调用方写一个 JSON 请求，按 `cmd` 字段分派，client 回一个 JSON 回应后关闭连接。`post` 由 client 自己回答：把消息写进 outbox 就回 `msg_id`，不等 hub，hub 连不上也照样排队。其余命令由 client 经请求通道转给 hub，把 hub 的回答或拒绝的原因带回来，hub 的那一段另有一个比 socket 请求短的超时。`version` 回答答话的进程的版本和 pid；`handoff` 起新进程，等它就绪或起不来再回答，不受请求超时的限制。hub 的管理 socket 是同一套实现，只回答这两条。`fednet client` 的这几条命令只连 socket，不打开数据库；退出码是 0 成功、2 用法错误或请求不合法、3 没有权限连 socket 或 hub 不允许、4 连不上 client 或 hub，其它失败 (例如线程不存在) 是 1。client 与 hub 之间的 payload 是一个 JSON 对象，`type` 字段说明它是什么，格式在 [`internal/payload`](../internal/payload)，两端共用；post 的 payload 带线程 key 和正文。

交接在 [`internal/handoff`](../internal/handoff)，用 `github.com/cloudflare/tableflip`。旧进程收到 `handoff` 就用自己的命令行起一个新进程 (二进制取路径上现在的那个)，把监听的 fd 传过去：hub 是 TCP 端口和管理 socket，client 是本机 socket，socket 连同它的锁文件一起传，锁跟着打开的文件走，两个进程都持有时不会松开。新进程打开数据库、接过 fd、开始回答之后报就绪：先经 `$NOTIFY_SOCKET` 告诉 systemd `MAINPID` 换了并且 `READY=1`，再告诉旧进程。旧进程这时停止接新连接，把手上的请求 (包括这次 `handoff`) 回答完，断开全部下行 WebSocket，退出码 0；client 按退避重连到新进程，没 ack 的消息按 `seq` 续传。新进程在一个超时 (`-handoff-timeout`，默认一分钟) 内没就绪、或者退出了，就被杀掉，旧进程照常服务，`handoff` 回答失败的原因。出站、报警检查、入站的 Socket Mode 连接和 client 的钩子只在一个进程里跑：新进程等旧进程退出后才起它们，所以交接期间 hub 对 Slack 的连接会断一小会儿，断线期间的事件由入站的补拉补回、按事件 id 去重。两个进程短时同时写同一个 SQLite 库，所以只有旧二进制也能写的迁移才能走交接。版本的规矩在 [`internal/release`](../internal/release)：版本是 `vMAJOR.MINOR.PATCH`；hub 服务和自己同版本的 client，以及不低于它写死的最低版本的发布版，其它的 (包括 `dev` 对发布版) 不派消息。升级先 client 后 hub：新 client 要能连旧 hub。

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
18. 下行连接开着的时候退役这台 client，或者给它换了凭证，hub 在一个心跳间隔内断开这条连接，旧凭证再连、再发上行都回 401，新凭证能连上并收到消息；没有变化的 client 不受影响。[`TestRegistryChangeDropsConnection`](../internal/auth/auth_test.go)

本机 socket：

19. `post` 写进 client 的 outbox 就返回 `msg_id`；hub 连不上时照样返回，连上后送到 hub。[`TestPost`](../internal/local/local_test.go)、[`TestPostWhileHubDown`](../internal/local/local_test.go)
20. socket 不给组时只有 client 的用户能连 (0600)，给了组时组里的用户也能连 (0660)。同一个路径同一时间只归一个 client：已有 client 持锁时 (包括它还没发布 socket 的时候) 第二个 client 起不来，几个 client 同时启动只有一个成功，连接都进它；持锁的 client 关闭后新 client 能接手；旧 socket 被替换，别的文件不被替换。[`TestListen`](../internal/local/local_test.go)、[`TestListenWhileAnotherIsStarting`](../internal/local/local_test.go)、[`TestListenConcurrent`](../internal/local/local_test.go)、[`TestListenRefusesAFile`](../internal/local/local_test.go)
21. agent 经 `fednet client post` 发的消息，hub 的 inbox 收到时 `type` 是 `post`，线程 key 和正文不变。[`TestHubAndClient`](../cmd/fednet/main_test.go)
22. `fednet client post` 用法错误退 2，没有权限连 socket 退 3，连不上 client 退 4。[`TestRun`](../cmd/fednet/main_test.go)、[`TestPostDenied`](../cmd/fednet/main_test.go)

请求通道：

23. 请求不排队：hub 连不上或者不回答，请求在超时之内失败，client 的 outbox 里不留下东西。[`TestRequestUnreachable`](../internal/link/link_test.go)、[`TestAskWhileHubDown`](../internal/local/local_test.go)
24. hub 拒绝的原因原样回到 client，其它失败的原因不出 hub；hub 不认凭证的请求算不允许。[`TestRequest`](../internal/link/link_test.go)、[`TestRequestUnauthorized`](../internal/link/link_test.go)
25. `adopt` 只改已有归属的线程，改成发请求的 client；`hub reassign` 把一台 client 的线程全部改给另一台。`open-thread` 开的线程归调用方，配置不允许调用方的 channel 不开，也不发到 Slack；已有归属的线程不能再登记；`threads` 只列调用方的线程。`dm` 只发给用户名单上的人，名单之外的不发到 Slack；`users` 按 id 排序列出名单。[`TestAdopt`](../internal/hubapi/hubapi_test.go)、[`TestOpenThread`](../internal/hubapi/hubapi_test.go)、[`TestThreads`](../internal/hubapi/hubapi_test.go)、[`TestDM`](../internal/hubapi/hubapi_test.go)、[`TestUsers`](../internal/hubapi/hubapi_test.go)、[`TestClaimAndThreads`](../internal/store/store_test.go)、[`TestHubRequests`](../cmd/fednet/request_test.go)
26. agent 经 socket 发的 `read-thread`、`open-thread`、`threads`、`channel-context`、`adopt`、`users`、`dm` 走真的认证到 hub，拿回 hub 的回答；线程 key 不合法或 `dm` 缺用户退 2，线程不存在退 1，在配置不允许的 channel 开线程、给名单之外的人发私信、client 已退役都退 3，hub 停了退 4。[`TestHubRequests`](../cmd/fednet/request_test.go)、[`TestAsk`](../internal/local/local_test.go)

钩子：

27. 在 client 存活期间，每条进 inbox 的消息钩子至多成功执行一次：重复下发的不再执行，成功过的不再执行，结果写不进库时留在内存里补写、不重跑。client 死在钩子退出之后、写库之前的那条会再执行一次。[`TestRunsOncePerMessage`](../internal/hook/hook_test.go)、[`TestRetriesUntilSuccess`](../internal/hook/hook_test.go)、[`TestOutcomeKeptWhenStoreFails`](../internal/hook/hook_test.go)、[`TestHubAndClient`](../cmd/fednet/main_test.go)
28. 失败或超时的消息留在 inbox，按逐次加倍、有上限的间隔重试；到上限转进死信，不删、不再自动重试，重复下发的也不再执行。重试记录或死信写不进库时同样不重跑，上限不会被突破；钩子没起来不算尝试。[`TestRetriesUntilSuccess`](../internal/hook/hook_test.go)、[`TestDeadLetterAtLimit`](../internal/hook/hook_test.go)、[`TestOutcomeKeptWhenStoreFails`](../internal/hook/hook_test.go)、[`TestNotRunIsNotAnAttempt`](../internal/hook/hook_test.go)、[`TestClientInboxRetryAndDeadLetter`](../internal/store/store_test.go)
29. 钩子退出后，不论成功、失败还是超时，它的整个进程组都被杀掉并回收，不留残留进程，僵尸也算残留；退出 0 但留下进程握着 stderr 的算失败。[`TestTimeoutKillsProcessGroup`](../internal/hook/hook_test.go)、[`TestLeftoverProcessesAreKilled`](../internal/hook/hook_test.go)、[`TestResidueCountsZombies`](../internal/hook/hook_test.go)
30. 钩子的环境变量只有显式给出的那些，client 自己的环境不带过去。[`TestEnvIsOnlyWhatIsGiven`](../internal/hook/hook_test.go)、[`TestHubAndClient`](../cmd/fednet/main_test.go)
31. client 重启后未交付的消息继续执行，尝试次数保留；被关停打断的那次不计。[`TestResumesAfterRestart`](../internal/hook/hook_test.go)、[`TestShutdownDoesNotCountAsAttempt`](../internal/hook/hook_test.go)
32. 已交付的行在保留期内仍去重，过了保留期才清理。[`TestClientInboxPrune`](../internal/store/store_test.go)

出站与报警：

33. post 发到 Slack 之后才标记已交付；发不出去的留在 inbox，它后面的不抢先发，重试时已经发出的段落不再发。[`TestFailedPostIsKeptAndRetried`](../internal/outbound/outbound_test.go)、[`TestRun`](../internal/outbound/outbound_test.go)
34. 每条 post 发在它的线程里，开头标出来源机器；超长的拆成同一线程里的连续几条，顺序不变。永远发不出去的报警后标记已交付，不挡后面的。[`TestPostNamesTheMachine`](../internal/outbound/outbound_test.go)、[`TestLongPostIsSplit`](../internal/outbound/outbound_test.go)、[`TestSplit`](../internal/outbound/outbound_test.go)、[`TestPermanentFailureIsAlertedAndSkipped`](../internal/outbound/outbound_test.go)、[`TestWebPostReplyAndDelete`](../internal/slack/web_test.go)
35. `open-thread` 登记归属失败时，刚发的消息被删掉，调用方收到错误。[`TestOpenThreadUndoneWhenClaimFails`](../internal/hubapi/hubapi_test.go)
36. 每条死信报一次警，带 `msg_id` 和原因，由 hub 以那台 client 的名义报；hub 发不出去的 client 报警留到下一轮再发，不挡 post。hub 与 Slack 断开超过阈值报一次，恢复后再断再报；报警发不出去的下一轮再试。client 离线且有消息排队超过阈值时报一次警，在每个受影响的线程里说一声，回来后再离线再报。[`TestDeadLetterAlertsOnce`](../internal/hook/hook_test.go)、[`TestClientAlertIsRelayed`](../internal/outbound/outbound_test.go)、[`TestHubWithSlack`](../cmd/fednet/slack_test.go)、[`TestSlackDown`](../internal/watch/watch_test.go)、[`TestFailedAlertIsRetried`](../internal/watch/watch_test.go)、[`TestOfflineWithQueue`](../internal/watch/watch_test.go)
37. webhook URL 不进报警的错误和日志。[`TestSend`](../internal/alert/alert_test.go)、[`TestHubWithSlack`](../cmd/fednet/slack_test.go)

入站：

38. 同一条 Slack 消息只交一次：同一个事件重投、同一条消息由另一个事件带来、或者补拉时又读到，都只入库一次、只写一次 outbox。[`TestHandleDedupsEvents`](../internal/inbound/inbound_test.go)、[`TestReceiveSlack`](../internal/store/store_test.go)
39. 先落盘再 ack：入库、登记归属和写 outbox 在同一个事务里，事务提交了传输层才 ack，没提交就不 ack、什么都不留下；同一个事件再来照常入库，重投的 ack 但不再交。[`TestRunAcks`](../internal/inbound/inbound_test.go)、[`TestHandleFailsWhenStoreFails`](../internal/inbound/inbound_test.go)、[`TestRouteInTransaction`](../internal/route/route_test.go)
40. 只放行用户名单上的人说的话，`/me` 也算；带 `bot_id` 的、编辑、删除、有人加入、改 topic 这些子类型都不交、也不入库。[`TestHandleFilters`](../internal/inbound/inbound_test.go)、[`TestHandleRoutes`](../internal/inbound/inbound_test.go)
41. channel 里的顶层消息送 channel 的默认机器并登记归属，带 channel 的 purpose，读不到就不带；回复送归属机器，不带；私信里每条顶层消息都是新线程，送私信的默认机器；上传的文件只把文件名和链接列在正文末尾。[`TestHandleRoutes`](../internal/inbound/inbound_test.go)、[`TestHandleWithoutPurpose`](../internal/inbound/inbound_test.go)、[`TestHandleFiles`](../internal/inbound/inbound_test.go)、[`TestRouteDM`](../internal/route/route_test.go)
42. 没配默认机器的 channel 或私信里的新线程不送、不登记归属，hub 的那句话作为 `post` 和记录同一个事务进 inbox，由出站发到线程里；重投不重复放；之后在那个线程里的回复不送。[`TestHandleNoMachineTellsThread`](../internal/inbound/inbound_test.go)
43. 断线期间的消息重连后补到，顶层消息和有归属的线程里的回复都算，和实时收到的不重复，重连后实时消息先到也不漏；回复先于它的线程首条到达时，先收首条再收回复；早于回看窗口的不补；补拉没做完时下次从同一处重来，上一条连接的补拉这时做完也清不掉新连接的起点，hub 重启后从持久化的位置继续；还没看到过任何消息时不补。[`TestBackfill`](../internal/inbound/inbound_test.go)、[`TestBackfillOfOldConnectionKeepsNewStart`](../internal/inbound/inbound_test.go)、[`TestHandleReplyBeforeRoot`](../internal/inbound/inbound_test.go)、[`TestBackfillWindow`](../internal/inbound/inbound_test.go)、[`TestBackfillAfterRestart`](../internal/inbound/inbound_test.go)、[`TestBackfillRetriesFromWhereItFailed`](../internal/inbound/inbound_test.go)、[`TestRunAcks`](../internal/inbound/inbound_test.go)
44. 连接状态报的是连着还是断着、这个状态从什么时候起，以及断了多久：连着时是 0，从第一次尝试连接起算，还没尝试过也是 0；重复报同一状态不改时间；传输层连上、断开都反映进来。[`TestStatus`](../internal/inbound/inbound_test.go)、[`TestRunAcks`](../internal/inbound/inbound_test.go)

接线：

45. hub 的 Slack token 和 webhook URL 从文件读，去掉首尾空白；它们和 client 的凭证都不进任何日志和错误信息。Slack 拒绝 token 时 hub 退出 1。[`TestHubWithSlack`](../cmd/fednet/slack_test.go)、[`TestHubStopsWhenSlackRejectsToken`](../cmd/fednet/slack_test.go)、[`TestRun`](../cmd/fednet/main_test.go)
46. 给了 Slack 时，人在 Slack 里发的消息送到已经连着的 client，不等它重连；agent 发的 post 发到线程里、标出机器，不等出站的轮询。没给 Slack 时 hub 只跑 HTTP 服务，post 留在 inbox。[`TestHubWithSlack`](../cmd/fednet/slack_test.go)、[`TestWakeAllPushesStoreQueued`](../internal/link/wake_test.go)、[`TestUplinkedAfterStored`](../internal/link/wake_test.go)、[`TestHubAndClient`](../cmd/fednet/main_test.go)

交接：

47. hub 和 client 交接时，交接前、交接中、交接后发出的消息各送达一次、顺序不变：client 的钩子按序各执行一次，client 的 post 都进 hub 的 inbox；旧进程没推出去的消息由新进程推。交接后旧进程退出 0，端口、本机 socket 和管理 socket 都由新进程回答。[`TestClientHandoff`](../cmd/fednet/handoff_test.go)、[`TestHubHandoff`](../cmd/fednet/handoff_test.go)
48. 新进程起不来时 `handoff` 失败并说明原因，旧进程照常服务，之后能正常停止。[`TestClientHandoffFailsWhenNewProcessCannotStart`](../cmd/fednet/handoff_test.go)、[`TestHubHandoffFailsWhenNewProcessCannotStart`](../cmd/fednet/handoff_test.go)
49. 就绪时经 `$NOTIFY_SOCKET` 发 `MAINPID` 与 `READY=1`，没有这个 socket 时什么都不发。[`TestNotify`](../internal/handoff/handoff_test.go)
50. 出站、报警检查、入站连接和钩子在交接期间只在一个进程里跑。untested
51. hub 不给版本过旧的 client 派消息：下行握手被拒，消息留在 outbox，上行照收，兼容的 client 照常收；这台 client 换成兼容的版本后收到留下的消息。版本相同的总算兼容。[`TestOutdatedClientGetsNoDownlink`](../internal/link/link_test.go)、[`TestCompatible`](../internal/release/release_test.go)
52. 一台 client 以 hub 不服务的版本连过，报一次警，说明哪台、什么版本、要求什么；换一个不服务的版本再报，换成服务的版本不报。[`TestOutdatedClient`](../internal/watch/watch_test.go)

仓库层面：

53. 这份文件不超过 200 行。[`design-length.test.sh`](../.github/scripts/design-length.test.sh)

## 4. 接口

- 命令行：`fednet hub`、`fednet hub register`、`fednet hub revoke`、`fednet hub reassign`、`fednet hub handoff`、`fednet client`、`fednet client init`、`fednet client handoff`、`fednet client post`、`fednet client read-thread`、`fednet client open-thread`、`fednet client threads`、`fednet client adopt`、`fednet client channel-context`、`fednet client users`、`fednet client dm`、`fednet version`，参数以 `fednet` 不带参数时打印的用法为准 ([`cmd/fednet/main.go`](../cmd/fednet/main.go))。
- hub 配置文件 (每个 channel 的默认机器和开线程的权限、私信的默认机器、用户名单、报警阈值)：格式在 [`cmd/fednet/main.go`](../cmd/fednet/main.go) 的 `hubConfig`，例子在 [`deploy/hub.example.json`](../deploy/hub.example.json)。
- 部署：hub 要的文件和参数见 [`deploy/README.md`](../deploy/README.md)，systemd 单元模板是 [`deploy/fednet-hub.service`](../deploy/fednet-hub.service)。
- 入站：收事件、补拉和连接状态的查询在 [`internal/inbound/inbound.go`](../internal/inbound/inbound.go)，报警要的 `SlackLink` 由 `Receiver.DownFor` 实现，处理完一条消息后的通知是 `Receiver.Stored`；接 Socket Mode 的入口在 [`internal/inbound/socket.go`](../internal/inbound/socket.go)。
- HTTP：只给 client 用，路径和帧格式在 [`internal/link/link.go`](../internal/link/link.go)；唤醒下行连接的 `Hub.WakeAll` 在 [`internal/link/wake.go`](../internal/link/wake.go)。
- 本机 socket：只给本机的 agent 用，请求和回应的格式在 [`internal/local/local.go`](../internal/local/local.go)；hub 的管理 socket 用同一种格式。
- 交接：进程要做的几步在 [`internal/handoff/handoff.go`](../internal/handoff/handoff.go) 的 `Process`；版本是否兼容在 [`internal/release/release.go`](../internal/release/release.go)，hub 要求的最低版本写在 [`cmd/fednet/main.go`](../cmd/fednet/main.go) 的 `minClientVersion`。
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
- 收过的消息表只增不删。补拉对每个有归属的线程各读一次回复，归属的线程多了会慢，也会撞 Slack 的限速。
- `handoff` 失败时只回答新进程退出了或者超时，新进程自己打印的错误在它的 stderr 里，和旧进程的是同一处 (systemd 下是 journal)。
- 不停机升级还没有在 systemd 下真跑过，`MAINPID` 的交接只按 systemd 的文档写。
- 看过死信的原因后手动重放的命令还没有。
- client 的报警要经 hub 发出，hub 停了时 client 的死信报警发不出去，留在 client 的上行 outbox 里等 hub 回来。
- 拆开发的长 post 发到一半时 hub 重启，重启后前面的段落会再发一次。
- `open-thread` 开线程的第一条消息不标来源机器。

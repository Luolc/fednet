# 部署 hub

这个目录放部署 `fednet hub` 用的通用材料，随代码一起改：

- [`fednet-hub.service`](fednet-hub.service)：systemd 单元模板，标了 `replace` 的几行按自己的机器改。
- [`echo-hook.sh`](echo-hook.sh)：client 的示例钩子，把收到的每条消息的概要回到同一个线程，见下面「回声钩子」一节。
- `fednet-hub-upgrade.path`、`fednet-hub-upgrade.service`、`fednet-client-upgrade.path`、`fednet-client-upgrade.service`：让 hub 和 client 自己升级的 root 单元，见下面「升级」一节。
- [`hub.example.json`](hub.example.json)：hub 配置的示例，channel id、机器名、用户 id 都是示例值。字段的含义以 `fednet` 不带参数时打印的用法为准。

机器本身怎么建、文件怎么装上去，不在这个仓里。

## 下载二进制

每个版本是 GitHub 上的一个 release，tag 是 `vX.Y.Z`，由推这个 tag 触发 [`release.yml`](../.github/workflows/release.yml) 发布。release 里有三个文件，部署和自己升级都按这几个名字下载：

- `fednet_vX.Y.Z_linux_amd64`、`fednet_vX.Y.Z_linux_arm64`：静态链接的裸二进制 (`CGO_ENABLED=0`)，`fednet version` 打印的就是 `vX.Y.Z`。
- `SHA256SUMS`：`sha256sum` 的标准格式，每行是哈希、两个空格、文件名。

发布之前流水线先跑和 PR 相同的检查，再确认 tag 指向的 commit 已经在 `main` 上，每个架构的二进制在同架构的 runner 上构建，跑一遍 `fednet version`，打出的版本不等于 tag 就不发布。

在要装的机器上下载、校验，再放到单元模板用的 `/usr/local/bin/fednet`：

```sh
v=vX.Y.Z arch=amd64   # arm64 machines: arch=arm64
base=https://github.com/Luolc/fednet/releases/download/$v
curl -fsSLO "$base/fednet_${v}_linux_${arch}"
curl -fsSLO "$base/SHA256SUMS"
sha256sum -c --ignore-missing SHA256SUMS
sudo install -m 0755 "fednet_${v}_linux_${arch}" /usr/local/bin/fednet
```

`sha256sum -c` 要打出 `fednet_vX.Y.Z_linux_<arch>: OK` 并退出 0；不是这样就不装。

## hub 要的文件

| 文件 | 参数 | 说明 |
| --- | --- | --- |
| 数据库 | `-db` | 不存在时自动建。单元模板放在 `StateDirectory` 下。 |
| 配置 | `-config` | JSON，格式见 `hub.example.json`。 |
| Slack app-level token | `-slack-app-token-file` | Socket Mode 用。 |
| Slack bot token | `-slack-bot-token-file` | Web API 用。 |
| 报警的 incoming webhook URL | `-alert-webhook-file` | 只放在 hub 上。 |
| 审批签名的私钥 | `-approval-key-file` | Ed25519，PKCS#8 PEM (`-----BEGIN PRIVATE KEY-----`)，其他用户 (others) 不能有任何权限，属组可读 (`LoadCredential=` 交来的是 0440)。只放在 hub 上；公钥分发到各台 agent 机器给 `fednet approval verify -pubkey` 用，格式是 OpenSSH 的一行 `ssh-ed25519 <base64> [注释]`。 |
| 管理 socket | `-admin-socket` | 收 `fednet hub handoff`。单元模板放在 `RuntimeDirectory` 下。 |
| 升级请求 | `-upgrade-request` | hub 要升级时写的文件，由 root 的升级单元监视，见「升级」一节。单元模板放在 `RuntimeDirectory` 下。 |

前三个凭证文件各放一个凭证，首尾的空白 (包括末尾换行) 读的时候去掉；私钥文件按 PEM 整个读。fednet 只从这几个文件读凭证，不读环境变量，也不把凭证写进日志和错误信息；文件由部署方提供，单元模板用 `LoadCredential=` 把它们交给服务。

两个 Slack 文件要么都给，要么都不给。都不给时 hub 只跑 client 用的 HTTP 服务，启动日志里有一句 `Slack not configured`，适合本地试跑。没给 Slack 时 webhook 文件不读，给了也不用；给了 Slack、没给 webhook 时，报警只进日志。

`/fednet upgrade` 要在配置里的 `upgrade.admins` 写上谁能升级 (Slack 用户 id，每个都得在 `users` 名单上)；`upgrade.auto` 默认是 `true`。

审批要同时有 Slack 和私钥，还要在配置里的 `approvals` 写上审批卡发到哪个 channel (`channel`) 和谁能批 (`approvers`，Slack 用户 id，每个都得在 `users` 名单上)。缺任何一样，agent 的 `request-approval` 直接被拒绝，不发卡、不签名；没给私钥时启动日志里有一句 `approvals are off`。

channel 里的消息只有 @ 了 bot 才交给 agent，@ 的那条带上线程里之前的消息作为上下文；配置里的 `history` 定上下文的上限：`max_messages` 最多几条，`max_chars` 这几条的正文合计最多几个字符，`max_message_chars` 单条正文超过几个字符就截断 (@ 的那条本身也按它截)。不写就用示例里的默认值 (10、4000、2000)。hub 启动时用 bot token 调一次 `auth.test` 认自己的用户 id，调不到就不启动。

人在 Slack 里上传的文件由 hub 用 bot token 代 client 下载 (app 的 manifest 要有 `files:read`)；配置里的 `files` 定上限：`prefetch_types` 是交给 agent 之前就先取到它机器上的类型 (`image/*` 是所有图片)，`prefetch_max_bytes` 先取的单个文件最大几个字节，`prefetch_max_total_bytes` 一条消息先取的合计最多几个字节，`fetch_max_bytes` agent 用 `fednet client fetch-file` 按需取时单个文件最大几个字节，`upload_max_bytes` 与 `upload_max_files` 是 agent 用 `fednet client post -file` 发文件时单个文件最大几个字节、一次最多几个 (manifest 要有 `files:write`)。不写就用示例里的默认值 (图片与 PDF、20 MiB、50 MiB、200 MiB、50 MiB、10 个)。发文件的消息用 `files.completeUploadExternal` 的 `blocks` 标来源机器，没对真 Slack 试过；不行就把 `upload_comment` 设成 `true`，改为写在消息文字的开头。hub 不存文件；client 存在它数据库旁边的 `files/` 下 (`-files-dir` 可改)，默认保留 7 天、合计 2 GiB (`-files-retention`、`-files-max-total-bytes`)。

## 启动之后

在 hub 机器上登记一台 client (ID 和 HASH 是那台机器上 `fednet client init` 打印的)：

```sh
sudo -u fednet fednet hub register -db /var/lib/fednet-hub/hub.db CLIENT-ID HASH
```

client 那边不需要 webhook：钩子放弃的消息由 client 经上行告诉 hub，由 hub 报警。

## 升级

先升 client，再升 hub：新版本的 client 要能连旧版本的 hub，反过来不保证。hub 不给版本过旧的 client 派消息，消息留在它的 outbox 里，报警里会说明是哪台、跑的什么版本。

### 从 Slack 升级，或者让 hub 自己升

hub 和 client 都不改自己的二进制。要升级时它们只写一个升级请求文件，内容是目标版本 (`vX.Y.Z` 加换行)；每台机器上一对 root 的 systemd 单元监视这个文件：path 单元看到文件就起 oneshot 单元，oneshot 跑 `fednet upgrade`，它先把请求文件删掉，再经 socket 问正在跑的进程是什么版本，目标不比它新就拒绝；然后从 GitHub release 下载本机架构的二进制和 `SHA256SUMS`，校验通过才用一次 rename 换掉 `/usr/local/bin/fednet` (旧的留作 `/usr/local/bin/fednet.prev`)，最后经同一个 socket 交接 (hub 是管理 socket，client 是本机 socket)。下载不对、校验不过都不换二进制；新进程起不来、交接失败，就把 `.prev` 换回去，这样下次重启仍跑能起来的那个；成功才删 `.prev`。原因在 `journalctl -u fednet-hub-upgrade` (client 上是 `fednet-client-upgrade`) 里，结局也写在请求文件旁边的 `.result` 文件里。

每台机器要装的东西：

- hub 机器：[`fednet-hub-upgrade.path`](fednet-hub-upgrade.path) 和 [`fednet-hub-upgrade.service`](fednet-hub-upgrade.service)，照抄到 `/etc/systemd/system/`，`systemctl enable --now fednet-hub-upgrade.path`。hub 单元模板已经带了 `-upgrade-request %t/fednet-hub/upgrade`，和 path 单元监视的是同一个文件。
- 每台 agent 机器：[`fednet-client-upgrade.path`](fednet-client-upgrade.path) 和 [`fednet-client-upgrade.service`](fednet-client-upgrade.service)，标了 `replace` 的几行改成本机的路径：请求文件 (client 的 `-upgrade-request`，要放在 client 用户能写的目录里)、二进制、client 的 socket (`-socket`)；`ReadWritePaths` 要盖住二进制和请求文件所在的目录。client 启动参数加上 `-upgrade-request <同一个文件>`；没有这个参数、或者请求文件写不了时，client 把失败经上行报给 hub，hub 报一次警 (同一个版本一次)，等它下次重连或者下一次定时检查时 hub 再补发。
- 首次安装仍按「下载二进制」一节手工装；装好之后版本就由 fednet 自己管，部署工具不要再把二进制钉回某个版本，否则每次重新配置都会把升过的版本降回去。

升级怎么触发，两条路都走上面这套单元：

- Slack 里的 slash command `/fednet`。正式 app 的 manifest 要加 `commands` 权限和这条命令，在任何 channel 或私信里都能发，bot 不必在那个 channel 里，命令文字不会作为消息进 channel、也不会路由给 agent。`/fednet version` 回 hub 的版本、最新的 release、每台 client 的版本和是否在线，用户名单上的人都能用，只有发命令的人看得到。`/fednet upgrade` 只有配置里 `upgrade.admins` 列出的人能用，只能升到最新的 release、不能降级；它先回一张只有发命令的人看得到的确认卡「从 vX 升到 vY？」，点「升级」才开始，点「取消」或者十分钟没点就作废。
- hub 每小时查一次最新的 release，有新的就自己开始升级；配置里 `upgrade.auto` 设成 `false` 就只留手动。hub 跑的不是发布版 (`fednet version` 打出 `dev`) 时不查，也不能从 Slack 升级。

一次升级的顺序：hub 先给每台在线、版本不是目标版本的 client 发升级通知，client 程序自己写请求文件 (不经过 agent)；hub 等它们都重连并报上新版本 (默认最多等十分钟)，全部到齐才写自己的请求文件、换成新进程。有一台失败或超时，hub 不升，汇总里列出是哪台，处理好了再发一次 `/fednet upgrade`；hub 自己到时没换成新进程，汇总里带上升级器写的结局 (回退到了哪个版本)。离线的 client 不等，它每次连 hub 时 hub 发现版本比自己旧，就在握手的回应里再告诉它一次，hub 因版本过旧拒绝的 client 也一样能收到。开始、hub 开始升级、最后的汇总都作为普通消息发到报警 webhook 对应的 channel，没配 webhook 就只进日志。

### 手工升级

在 hub 机器上，按上面「下载二进制」一节把新的二进制放到 `/usr/local/bin/fednet`，然后：

```sh
sudo systemctl reload fednet-hub
```

它经管理 socket 执行 `fednet hub handoff`：新进程起来、接过端口和 socket 之后，旧进程处理完手上的请求再退出，client 的连接会断一次、自己重连。新进程起不来时 reload 失败，旧进程照常服务，原因在 `journalctl -u fednet-hub` 里。改了单元文件、或者新版本的数据库迁移旧版本写不了的，用 `systemctl restart`。

agent 机器上的 client 由本机的 agent 执行 `fednet client handoff -socket <socket>`，走同一套机制。

## 回声钩子

[`echo-hook.sh`](echo-hook.sh) 是 client 钩子的最小示例，也可以拿来检查一台 client 收到的消息是否完整：每收到一条 `message` 类型的消息，就用 `fednet client post` 在同一个线程里回一行概要。其它类型的事件 (例如审批结果) 不回，直接退出 0。它只用 `sh` 和 `jq`，`fednet` 要在 `PATH` 上。

接法：client 的命令行末尾给这个脚本作为钩子，并用 `-hook-env` 把 socket 路径放行给它 (钩子的环境只有 `PATH`、`HOME` 和 `-hook-env` 列出的变量)，client 单元里再设这个变量，值和 `-socket` 一样：

```sh
Environment=FEDNET_SOCKET=/run/fednet-client/client.sock   # replace: same as -socket
ExecStart=/usr/local/bin/fednet client ... -socket ${FEDNET_SOCKET} -hook-env FEDNET_SOCKET /usr/local/bin/echo-hook.sh
```

没设 `FEDNET_SOCKET` 时脚本报错退出 (非 0)，消息按钩子失败的规则重试。钩子失败重试、或 client 在钩子退出后崩溃时，同一条消息可能回声两次，`msg_id` 相同。

输出是一行：`回声：type=…，trigger=…，history <included>/<total> 条 (<截断数> 条截断)，附件 <n> 个 (<名字>，已下载/下载失败/未下载)，msg_id=…`。字段不存在时写「无」，旧版本的 hub 没有 `trigger`、`history` 和附件。附件项有非空的 `path` (已下载到本机的路径) 写「已下载」，有 `error` (下载失败的原因) 写「下载失败」，两个都没有写「未下载」，现在的 hub 不下载附件，就是这种。样例：

```
回声：type=message，trigger=mention，history 10/12 条 (1 条截断)，附件 3 个 (a.png，未下载；b.pdf，已下载；c.zip，下载失败)，msg_id=m1
回声：type=message，trigger=无，history 无，附件 无，msg_id=m2
```

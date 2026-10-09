# 部署 hub

这个目录放部署 `fednet hub` 用的通用材料，随代码一起改：

- [`fednet-hub.service`](fednet-hub.service)：systemd 单元模板，标了 `replace` 的几行按自己的机器改。
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
| 审批签名的私钥 | `-approval-key-file` | Ed25519，PKCS#8 PEM (`-----BEGIN PRIVATE KEY-----`)，只有所有者能读。只放在 hub 上；公钥分发到各台 agent 机器给 `fednet approval verify` 用。 |
| 管理 socket | `-admin-socket` | 收 `fednet hub handoff`。单元模板放在 `RuntimeDirectory` 下。 |

前三个凭证文件各放一个凭证，首尾的空白 (包括末尾换行) 读的时候去掉；私钥文件按 PEM 整个读。fednet 只从这几个文件读凭证，不读环境变量，也不把凭证写进日志和错误信息；文件由部署方提供，单元模板用 `LoadCredential=` 把它们交给服务。

两个 Slack 文件要么都给，要么都不给。都不给时 hub 只跑 client 用的 HTTP 服务，启动日志里有一句 `Slack not configured`，适合本地试跑。没给 Slack 时 webhook 文件不读，给了也不用；给了 Slack、没给 webhook 时，报警只进日志。

审批要同时有 Slack 和私钥，还要在配置里的 `approvals` 写上审批卡发到哪个 channel (`channel`) 和谁能批 (`approvers`，Slack 用户 id，每个都得在 `users` 名单上)。缺任何一样，agent 的 `request-approval` 直接被拒绝，不发卡、不签名；没给私钥时启动日志里有一句 `approvals are off`。

## 启动之后

在 hub 机器上登记一台 client (ID 和 HASH 是那台机器上 `fednet client init` 打印的)：

```sh
sudo -u fednet fednet hub register -db /var/lib/fednet-hub/hub.db CLIENT-ID HASH
```

client 那边不需要 webhook：钩子放弃的消息由 client 经上行告诉 hub，由 hub 报警。

## 升级

先升 client，再升 hub：新版本的 client 要能连旧版本的 hub，反过来不保证。hub 不给版本过旧的 client 派消息，消息留在它的 outbox 里，报警里会说明是哪台、跑的什么版本。

在 hub 机器上，按上面「下载二进制」一节把新的二进制放到 `/usr/local/bin/fednet`，然后：

```sh
sudo systemctl reload fednet-hub
```

它经管理 socket 执行 `fednet hub handoff`：新进程起来、接过端口和 socket 之后，旧进程处理完手上的请求再退出，client 的连接会断一次、自己重连。新进程起不来时 reload 失败，旧进程照常服务，原因在 `journalctl -u fednet-hub` 里。改了单元文件、或者新版本的数据库迁移旧版本写不了的，用 `systemctl restart`。

agent 机器上的 client 由本机的 agent 执行 `fednet client handoff -socket <socket>`，走同一套机制。

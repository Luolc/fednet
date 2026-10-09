# 部署 hub

这个目录放部署 `fednet hub` 用的通用材料，随代码一起改：

- [`fednet-hub.service`](fednet-hub.service)：systemd 单元模板，标了 `replace` 的几行按自己的机器改。
- [`hub.example.json`](hub.example.json)：hub 配置的示例，channel id、机器名、用户 id 都是示例值。字段的含义以 `fednet` 不带参数时打印的用法为准。

机器本身怎么建、文件怎么装上去，不在这个仓里。

## hub 要的文件

| 文件 | 参数 | 说明 |
| --- | --- | --- |
| 数据库 | `-db` | 不存在时自动建。单元模板放在 `StateDirectory` 下。 |
| 配置 | `-config` | JSON，格式见 `hub.example.json`。 |
| Slack app-level token | `-slack-app-token-file` | Socket Mode 用。 |
| Slack bot token | `-slack-bot-token-file` | Web API 用。 |
| 报警的 incoming webhook URL | `-alert-webhook-file` | 只放在 hub 上。 |
| 审批签名的私钥 | `-approval-key-file` | Ed25519，PKCS#8 PEM (`-----BEGIN PRIVATE KEY-----`)，只有所有者能读。只放在 hub 上；公钥分发到各台 agent 机器给 `fednet approval verify` 用。 |

前三个凭证文件各放一个凭证，首尾的空白 (包括末尾换行) 读的时候去掉；私钥文件按 PEM 整个读。fednet 只从这几个文件读凭证，不读环境变量，也不把凭证写进日志和错误信息；文件由部署方提供，单元模板用 `LoadCredential=` 把它们交给服务。

两个 Slack 文件要么都给，要么都不给。都不给时 hub 只跑 client 用的 HTTP 服务，启动日志里有一句 `Slack not configured`，适合本地试跑。没给 Slack 时 webhook 文件不读，给了也不用；给了 Slack、没给 webhook 时，报警只进日志。

审批要同时有 Slack 和私钥，还要在配置里的 `approvals` 写上审批卡发到哪个 channel (`channel`) 和谁能批 (`approvers`，Slack 用户 id，每个都得在 `users` 名单上)。缺任何一样，agent 的 `request-approval` 直接被拒绝，不发卡、不签名；没给私钥时启动日志里有一句 `approvals are off`。

## 启动之后

在 hub 机器上登记一台 client (ID 和 HASH 是那台机器上 `fednet client init` 打印的)：

```sh
sudo -u fednet fednet hub register -db /var/lib/fednet-hub/hub.db CLIENT-ID HASH
```

client 那边不需要 webhook：钩子放弃的消息由 client 经上行告诉 hub，由 hub 报警。

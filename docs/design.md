# fednet 设计

这份文档只描述 fednet 当前的实际状态，状态变了就原地修改。末尾的决策日志按日期列出做过的决定。2026-10-09 开源之前的决定只记结论，讨论过程不公开；之后的决定，理由写在做出决定的那个 PR 里。只有维护者发起的 design 修订才改这份文件；其它 PR 不改它，只在 PR 描述的「Design impact」里写明对它的影响。

## 1. 是什么，不是什么

fednet 让用户在 Slack 线程里和各台机器上的 coding agent 打交道：在线程里交代一件事，由某台机器上的 agent 去做，中途的问题和最后的结果都回到同一个线程里。

计划只有一个二进制 `fednet`，分两个子命令：`fednet hub` 跑在一台固定的 hub 机器上，负责和 Slack 的连接；`fednet client` 跑在每台 agent 机器 (工作站、数据机) 上，主动连到 hub，把消息交给本机的 agent。

**当前状态：只有一个空的二进制骨架，子命令尚未实现。** 下面各节照实写，不描述还不存在的东西。

fednet 不是：

- agent 的运行时。agent 由 herdr 启动和管理，fednet 只负责把消息送到它那里。
- 模型网关。fednet 不管模型账号的登录，也不转发模型流量。
- 任务看板。待办和尚未落实的设计在 Linear 上，不在这个仓里。

## 2. 组成部分

目前只有一个 Go 写的二进制 `fednet` ([`cmd/fednet`](../cmd/fednet))，用标准库 `flag` 分派三个子命令：`hub` 和 `client` 还是空壳，只报「尚未实现」并以 2 退出；`version` 打印构建时注入的版本号，未注入时打印 `dev`。

## 3. 不变量

fednet 本身还只有空壳，也就还没有不变量。仓库层面目前只有一条：

1. 这份文件不超过 200 行。[`design-length.test.sh`](../.github/scripts/design-length.test.sh)

## 4. 接口

暂无。

## 5. 已知问题与下一步

- 新建一个只有用户本人的 Slack workspace，在里面建 fednet 用的 Slack app。
- 写一个最小程序，用 slack-go 的 Socket Mode 连这个 workspace，连续跑一天，看断线重连和 ack 是否可靠，再决定 hub 用不用 slack-go。
- 建 hub 机器。
- 之后按讨论中的设计实现 `fednet hub` 与 `fednet client`，实现了哪一部分，就把它写进上面各节。

## 决策日志

- 2026-10-08：仓库文字改用中文。
- 2026-10-08：只有一个二进制 `fednet`，子命令为 `fednet hub` 与 `fednet client`。
- 2026-10-08：仓库里只留这份 design.md，尚未落实的设计与待办移到 Linear。
- 2026-10-08：不再把 fednet 写成某个更大系统的一部分，只描述 fednet 本身。
- 2026-10-09：以 Apache-2.0 许可证开源。
- 2026-10-09：用 Go 实现，一个二进制 fednet，标准库 flag 分派子命令。

---
name: fednet-pr-review
description: fednet 仓特有的审查增量：公开内容里允许出现的主机名，以及设计文档的检查。审查本仓任何 PR 时使用，实现方开 PR 之前自查也用。
---

# fednet PR 审查增量

## 公开内容

hub 机器的 hostname `fednet-hub` 是设计的一部分，可以写。Linear 的 workspace 名、team 名、issue 编号和链接不能出现，只写「在 Linear 上」。

## 设计文档

- PR 描述的「Design impact」要填：写影响哪一节或哪条不变量以及守护它的测试，没有影响写「无」。
- 只有维护者发起的 design 修订可以改 `docs/design.md`；其它 PR 改了它，是阻塞项。
- `docs/design.md` 列出的每条不变量要链到一个存在的测试；链接指向的测试找不到，是阻塞项。没有测试的不变量必须标「untested」并列进 PR 描述。
- 仓里不应该出现 ADR、调研报告和待办清单。

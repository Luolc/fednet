# Agent 守则

本仓的文字用中文。代码、代码注释、commit message 和 PR 标题用英文。

## 范围

fednet 是用户与各台机器上的 coding agent 之间的 Slack 消息桥：用户在 Slack 里与各 agent 机器对话，由它居中转发。设计细节见 `docs/design.md`。

仓库是公开的；机器按角色称呼，例如 hub 机器、agent 机器。

## 目录与文档

- 仓里只有 `docs/design.md` 一份设计文档，没有 ADR、调研报告和待办清单；尚未落实的设计和待办在 Linear 上。
- `docs/design.md` 只描述当前状态，原地修改。开源 (2026-10-09) 之后的决策，理由写在对应的 PR 里；之前的决定只在决策日志里记结论。
  - 固定五节：是什么、不是什么；组成部分与连接方式 (一张图加一段话)；不变量，每条链到守护它的测试 (没有测试的标「untested」并列进 PR 描述，或者不写)；接口 (只放链接)；已知问题与下一步。末尾是决策日志，每条一行：日期和定了什么。
  - 不超过 200 行，由 CI 检查。
  - 只有维护者发起的 design 修订可以改它。其它 PR 不碰它，在描述的「Design impact」里写：影响哪一节或哪条不变量以及守护它的测试，没有影响就写「无」。
- PR 描述按 `.github/pull_request_template.md` 写，包括 Design impact。

## 检查与测试

- 本地检查：`uvx pre-commit run --all-files`，裸跑，看退出码。新文件先 `git add`。
- clone 之后执行一次 `uvx pre-commit install`，装上 git 钩子。
- 钩子不跑 Go 的检查，本地要另外跑：`gofmt -l .` 输出必须为空 (它列出文件时退出码仍是 0)，`go vet ./...` 和 `go test -race ./...` 看退出码。
- 钩子按顺序是 `gitleaks` (扫暂存区的 diff) 和 `limae` (检查中文 Markdown 的排版)，`rev` 固定在 tag 上，升级要单独开 PR。

## CI

`.github/workflows/ci.yml` 的 `check` 是 `main` 上的 required status check：对完整历史跑 gitleaks，跑 `pre-commit run --all-files` (跳过 gitleaks 钩子)，检查 `docs/design.md` 的长度，最后跑 gofmt、vet 和带 `-race` 的测试。

## 审查

仓级审查 skill 在 `.agents/skills/fednet-pr-review/`，由 `.claude/skills/fednet-pr-review` 链到它。

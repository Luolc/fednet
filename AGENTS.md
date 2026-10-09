# Agent 守则

本仓的文字用中文。代码、代码注释、commit message 和 PR 标题用英文。

## 范围

fednet 是用户与各台机器上的 coding agent 之间的 Slack 消息桥：用户在 Slack 里与各 agent 机器对话，由它居中转发。设计细节见 `docs/design.md`。

仓库是公开的，一切内容按可发布的标准写：不进凭证、私有机器名、私有仓名、家目录路径、内部名字和内部守则的编号。机器按角色称呼，例如「hub 机器」「agent 机器」。

## 目录与文档

- `docs/design.md` 是唯一的设计文档，只描述当前状态，原地修改。开源 (2026-10-09) 之后的决策，理由写在对应的 PR 里；之前的决定只在决策日志里记结论。
  - 固定五节：是什么、不是什么；组成部分与连接方式 (一张图加一段话)；不变量，每条链到守护它的测试 (没有测试的标「untested」并列进 PR 描述，或者不写)；接口 (只放链接)；已知问题与下一步。末尾是决策日志，每条一行：日期和定了什么。
  - 不超过 200 行，由 CI 检查。
  - 只有维护者发起的 design 修订可以改它。其它 PR 不碰它，在描述的「Design impact」里写：影响哪一节或哪条不变量以及守护它的测试，没有影响就写「无」。
- PR 描述按 `.github/pull_request_template.md` 写，包括 Design impact。
- 仓里没有 ADR、操作手册目录、调研报告和待办清单，这与一般做法不同。偏离的内容、理由和来源如下：
  - 所有者 2026-10-08 定：只留 `docs/design.md`，不设 ADR 和操作手册目录；尚未落实的设计放到 Linear；`docs/design.md` 只反映仓库当前状况。
  - 维护者 2026-10-08 定：
    - 调研报告不进仓库。仓库是公开的，调研报告大多带着私有环境的背景。
    - 待办清单不进仓库，待办在 Linear 上。
    - 决定的理由写在做出决定的那个 PR 里，`docs/design.md` 只写当前状态，不留理由。

## 待办与想法

待办和想法记在 Linear 的 `fednet` project 里，仓里不写 workspace 名、team 名和链接。

- 还没落实的设计是打 `idea` 标签的 issue；要做的事是工作 issue。
- 实验工作流：父 issue 表示要做的事，子 issue 表示某个 agent 的一次尝试。一次尝试只有两种结局，Done 或 Abandoned，不重开。接手的 agent 开新的子 issue，用 related 关联旧的。
- 有疑问时打 `blocked` 往上问；要用户过目或决定的，打 `needs-user`。
- 领取与释放只用 `atb linear claim` 和 `atb linear release`。

## 公开仓流程

push 之前先在本地审一遍，确认没有泄漏再开 PR：

1. 本地 commit 并跑完检查后先不 push。实现方把本次 diff (`git diff origin/main...HEAD`)、PR 描述草稿 (含 commit message) 和 head SHA 交给审查方预审。
2. 审查方回 `Pre-push: OK, head <sha>` 之后才能 push，push 的 head 必须是预审过的那个 SHA。预审之后又改了，就重新预审。
3. PR 描述里写一行 `本地泄漏预审：reviewer OK，head <sha>`，用角色名，不写 agent 名；描述里也不写 Pair 行。

## 检查与测试

- 本地检查：`uvx pre-commit run --all-files`，裸跑，看退出码。新文件先 `git add`。
- clone 之后执行一次 `uvx pre-commit install`，装上 git 钩子。
- 钩子不跑 Go 的检查，本地要另外跑下面三条，CI 的 `check` 跑的也是这三条：`gofmt -l .` 输出必须为空 (它列出文件时退出码仍是 0，要看输出)，`go vet ./...` 和 `go test -race ./...` 看退出码。
- 钩子按顺序是：`gitleaks` (扫暂存区的 diff，排在最前，保证泄漏在其他钩子改文件之前就被发现)，然后是 `limae` (检查中文 Markdown 的排版)。`rev` 固定在 tag 上，升级要单独开 PR。
- `--no-verify` 会跳过所有钩子，包括 gitleaks。确实要用时，暂存之后手动跑 `gitleaks git --staged --redact --no-banner --verbose .`，`--redact` 必须带。

## CI 与合并

- `.github/workflows/ci.yml` 只有一个 job `check`，是 `main` 上的 required status check。它先对完整历史跑一遍 gitleaks (钩子只看得到暂存区的 diff，两者覆盖的输入不同)，再跑 `pre-commit run --all-files`，并跳过 gitleaks 钩子 (`SKIP: gitleaks`)，因为完整历史的扫描已经覆盖了它。`check` 里还会检查 `docs/design.md` 的长度，最后跑 Go 的 gofmt、vet 和带 `-race` 的测试。
- PR 不挂 auto-merge。LGTM 之后由维护者执行 `gh pr merge <N> --squash --delete-branch --match-head-commit <approved-sha> --auto` 合并：`--auto` 只是等 `check` 变绿，`--match-head-commit` 把合并固定在已审的 head 上，远端 head 变了就不合。

## 审查

审查本仓的 PR，用仓级审查 skill `.agents/skills/fednet-pr-review/` (由 `.claude/skills/fednet-pr-review` 链到它)。它不依赖仓外的文件，写了审查步骤、输出格式，以及哪些内容不能出现在公开内容里。

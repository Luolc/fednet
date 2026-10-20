---
name: statute-migrate
description: How to move a repository from hand-written instruction files (AGENTS.md, CLAUDE.md and the like) onto a statute rule set, so that its AGENTS.md is compiled from the constitution and the rules it chooses. Use when a person asks to migrate a repository to statute, to redo the rule set of one that was migrated before default switches, or to set up a new clone of a public repository on statute that has a private output.
metadata:
  statute-rules: "126"
---

# Moving a repository onto statute

The steps, in order: check the tools (step 0), read the constitution
(a), find the rules that apply (b), take stock of how the repository
works (c), write its own rules in one go (d), then rename the
hand-written files, write the rule set, compile, add the compile check
to the commit hooks and CI, and check (e to g), all in one pull request.
Questions for the person are collected in one place
([Questions](#questions)); ask them together, at most five at a time,
each with its default, and go on with what they do not block.

**A private rule in a public repository** is enabled only in the
private rule set, with `statute add --private <rule>`, both during the
migration and whenever one is enabled after it. Never copy it into the
local rules directory as a rule of the repository's own: the copy
publishes what the private rule keeps private and drifts from its
source. No pull request carries a change to the private rule set, by
`statute add --private` or `statute remove --private`; before you run
either, have the change reviewed, and approved by a principal where it
enables a rule that needs that or turns off a rule that is a guard, as
[Enabling a private rule](https://github.com/Luolc/agent-constitution/blob/main/docs/design/rule-sources.md#enabling-a-private-rule)
says.

## Files

Every path the skill uses is in this table, and the text below names
them only by the name in the first column. Where the layout of these
files changes, this table is the one place to change.

| Name | Path | Committed |
| --- | --- | --- |
| the global configuration | `$XDG_CONFIG_HOME/statute/config.toml` (`~/.config/statute/config.toml` by default) | no, the user's home |
| the rule set | `statute.toml` at the top of the repository | yes, in the same commit as the compiled file (step g) |
| the lock file | `statute.lock`, next to the rule set; its `statute = "<version>"` line, the first key after the header comment, names the statute that wrote it | yes |
| the compiled file | `AGENTS.md`, next to the rule set | yes |
| the local rules directory | `.statute/rules/` next to the rule set, or what `local-rules` in the rule set names | yes |
| the installed skills | `.agents/skills/<name>/`, with a link `.claude/skills/<name>` | yes |
| the private rule set | `private.toml` in `$(git rev-parse --git-path common/statute)`, shared by the worktrees of a clone | no, in git's own directory |
| the private lock file | `private.lock`: the main worktree's next to the private rule set; a linked worktree's own in `$(git rev-parse --git-path statute)`, once `statute lock` or `statute upgrade --private` run there writes it, and until then it reads the main worktree's | no, in git's own directory |
| the private output | `.statute/AGENTS.full.md` | no, the statute ignore file lists it |
| the statute ignore file | `.statute/.gitignore`, written by statute | yes |
| the private links | `CLAUDE.local.md` and `AGENTS.override.md`, both leading to the private output | no, ignored through the exclude file |
| the exclude file | `.git/info/exclude`, shared by the worktrees of a clone | no, in git's own directory |
| the checkout hook | the file `git rev-parse --git-path hooks/post-checkout` prints, shared by the worktrees of a clone | no, in git's own directory |
| the hook line | `[ "$3" = 1 ] && statute compile --private-only; exit 0` | in the checkout hook |
| the merge hook | the file `git rev-parse --git-path hooks/post-merge` prints, shared by the worktrees of a clone | no, in git's own directory |
| the merge hook line | `statute compile --private-only; exit 0` | in the merge hook |
| a legacy copy | `<name>.legacy.md` beside the hand-written `<name>.md` it replaces, as `AGENTS.legacy.md` | yes, until the person says to delete it |
| the constitution for agents | `constitution/<edition>/generated/constitution.<lang>.agent.md` in the first-party source | in the source |
| the rules of a source | `rules/<id>.toml` at the top of that source, or in the directory its `rules-dir` names | in the source |

## 0. Before you start

1. statute runs: `statute --version` prints 0.1.18 or later, the first
   version that records its version in the lock file (step g). Run every
   `statute` command from the directory of the rule set.
2. The global configuration exists. Without it, `statute init` takes
   the built-in first-party source, which is enough for a public
   repository with no private rules. When the person keeps private
   rules, it must name their private source; ask before writing it
   ([Questions](#questions), Q1). In a public repository, a
   `personal` parameter whose value in the global configuration differs
   from the committed one (such as the language an agent writes to the
   person in) makes a private output by itself, so even with no
   private rules `statute compile` asks for the two private links in
   the exclude file first (step g).
3. The repository has no rule set yet. One that has a rule set
   written before default switches is redone from step b: its `rules`
   and `packs` are where the three lists of step f start.
4. The work is on a branch of its own with a clean tree, and goes
   into the main branch the way the repository takes changes, as one
   pull request (step g says why it is not split).
5. Know whether the repository is public:
   `gh repo view <owner>/<repo> --json visibility --jq .visibility`
   prints `PUBLIC` for a public one. A public repository keeps every
   private rule out of its committed files (step d).
6. `statute init` (with `--public` or `--private` when gh cannot
   tell) writes the rule set with the first-party defaults and no
   `rules`, `domains` or `exclude`, so that `statute site` and
   `statute explain` work from step b on.

## a. Read the constitution

Read the constitution for agents of the edition the rule set will
use, in the language the compiled file will be written in. It is what
every rule rests on, and the compiled file opens with it.

Product: nothing written. You know which provisions the repository's
practices fall under, and so what a rule may require and what the
constitution already says on its own.

## b. Find the rules that apply

Go through the first-party rules domain by domain: `statute site`
writes a local site of every rule the sources hold, with each rule's
statement, reason and verification; the rules of a source can also be
read directly. For each domain, decide whether the repository needs
it, then, inside each domain it keeps, which rules do not fit it, and
at each one-of which child it takes.

The defaults decide most of this
([Default switches](https://github.com/Luolc/agent-constitution/blob/main/docs/design/rule-config.md#default-switches)):
a domain or rule that is `on` comes in without being listed. You look
for departures only.

- A rule `on` by default that the repository must not have: a rule
  whose environment it lacks (no CI, no reviewer, no shared machine)
  or that contradicts how it works on purpose.
- A domain or rule `off` by default that it needs: fleet (R78) when agents
  divide work there, Writing Rules (R96) when agents write or change
  rules there, which a repository with local rules does (step d).
- A one-of whose default child is not how the repository works.

Product: a draft of the three lists of step f, each departure with
one line that says why.

## c. Take stock of how the repository works

Read everything that tells an agent or a person how to work here: the
hand-written instruction files at the top and in subdirectories
(`AGENTS.md`, `CLAUDE.md`, `.cursorrules`, `GEMINI.md`, and any file
they import), the README and the contributing guide, the commit hooks,
the CI workflows, the scripts the docs tell people to run, and the
repository's own skills.

Give each item of the hand-written instruction files exactly one
destination:

- the constitution, when it already says so;
- a first-party rule the rule set enables (name it, `R42`);
- a parameter, when the item is a value a first-party rule leaves
  open, such as the language of commit messages (name the parameter
  and the value);
- a local rule of this repository (step d);
- a private rule, when it rests on a private asset (step d);
- a skill, when it says how to do something rather than what must be
  done;
- removal, when it is out of date or said better elsewhere; say why.

Items that only describe the repository (what it is, its layout, its
check commands) become local rules too when an agent must follow them,
and stay in the README otherwise.

Product: the mapping, one line per item, written into the pull
request description. In a public repository, an item whose destination
is a private rule is written there as "private rule" only, without its
text.

## d. Write the repository's own rules in one go

Write every local and private rule the mapping calls for in one
change, not one rule per change, so that the person reviews the
repository's rules together.

A source and the repository's own rules are two different things
([Four kinds of source](https://github.com/Luolc/agent-constitution/blob/main/docs/design/rule-sources.md#four-kinds-of-source)).
A source publishes rules for other repositories to use: they are the
rules of a source, it has a git address or a path, and a rule set
that wants them names it in `[sources]` and cites them as `R42` or
`team:7`. The repository's own rules bind this repository alone: they
are in the local rules directory, cited as `L12`, compiled into this
repository's output only, and no other repository can name them. A
repository may be both; this step writes only its own rules. Say
which is which when you show the person the rules.

- Write them as the rules of domain R96 (Writing Rules) require: one
  requirement per rule, worded so that a reviewer can decide it,
  self-contained, citing in `authority` the most specific provisions
  it refines, and new rules as trial rules. Their file format is that
  of the rules of a source; take a first-party rule of the same kind
  as the template.
- Local rules go in the local rules directory, and are referred to as
  `L12`. Leave `local-rules` unset: its default is the local rules
  directory.
- Keep in a private source only the private assets a requirement rests
  on (rule R126). For each rule, take the private assets out and see
  what is left: a whole requirement is a local rule; a principle whose
  values are private becomes a local rule, and the private source keeps
  only a rule that sets the values and requires it; nothing left, and
  the rule is private. What the test does not settle stays private
  until the person answers (Q5).
- In a public repository, name the private source in the private rule
  set, never in the rule set, and list there the private rules it
  enables (`statute add --private`), reviewed as the note at the top
  says.
- When a practice of the repository cannot hold together with a public
  rule that a private rule requires, `exclude` in the rule set cannot
  take the public rule out while the private rule is enabled: the
  private layer fails, naming both. Turn the private rule off for this
  repository in the private rule set, `statute remove --private
  <rule>`, which takes it out of `rules` there or, when a default or a
  pack enables it, writes it to `exclude` there; then the rule set can
  exclude the public rule. When the private rule is wrong for every
  repository, have whoever keeps the private source change it.

Product: the rules, each passing the source's build check; for the
private source, the change in the repository that holds it.

## e. Rename the hand-written files

`statute compile` writes the compiled file and refuses to overwrite a
file it did not write, so the hand-written files leave first.

- Rename each hand-written instruction file that the compiled file
  replaces to its legacy copy, with `git mv`: `AGENTS.md` to
  `AGENTS.legacy.md`, a `CLAUDE.md` that is a file to
  `CLAUDE.legacy.md`. The legacy copies stay in the repository; ask
  the person whether to delete them (Q2), delete them on a yes, and
  keep them otherwise.
- A `CLAUDE.md` that is a link to `AGENTS.md` is not renamed. Ask the
  person whether to delete it (Q3), with this explanation:

  > Recent Claude Code reads `AGENTS.md` itself when there is no
  > `CLAUDE.md`, so the link is no longer needed. In our setup
  > `AGENTS.md` is the public file, and `CLAUDE.local.md` is the
  > override that replaces it whole with the full text, public and
  > private rules together. Claude Code also reads a `CLAUDE.md` beside
  > `CLAUDE.local.md`, so with the link in place it reads the public
  > rules twice. `statute compile` warns about it on every run.

- An `AGENTS.md` in a subdirectory: its items are in the mapping like
  any other. With the private output, Claude Code does not load it, so
  its rules belong in the rule set; once they are there, rename it as
  above.

## f. Write the rule set: three lists

Start from the rule set `statute init` wrote in step 0 and go
through three lists, in this order
([Moving a repository over](https://github.com/Luolc/agent-constitution/blob/main/docs/design/rule-config.md#moving-a-repository-over)):

1. The rules on by default that the repository keeps: nothing to do.
2. Those on by default that it must turn off: `exclude`, each entry
   with a comment on the line above that says why.
3. The domains and rules off by default that it needs: `domains` and
   `rules`, each with a comment that says why; and each one-of where
   it takes another child than the default: that child in `rules`,
   with its reason.

```toml
domains = [
  # Agents write and change the local rules here.
  "R96",
]
exclude = [
  # This repository has no commit hooks, and its checks run only in CI.
  "R53",
]
```

`statute add` writes a domain to `domains` and a rule to `rules`, and
`statute remove` writes a rule the defaults enable to `exclude`; add
the comments afterwards. A rule that a private rule requires cannot go
in `exclude` while that private rule is enabled; step d says what to do
instead. A rule to turn off in the private output alone goes in
`exclude` of the private rule set, `statute remove --private <rule>`.
Set the parameters the mapping found in their tables (`[language]` and
others). A `personal` parameter, the language an agent writes to the
person in, takes the repository's value there;
the person's own value goes into the global configuration, which in a
public repository reaches the private output only.

Then show the person the three lists, with the reason on each line,
and ask them to look them over (Q4). `statute explain --defaults`
prints the three lists from the rule set as written; `statute explain
<rule>` says why one rule is in or out. An `exclude` entry for a rule
under a domain that is off by default is not listed under "On by
default, and turned off", since the rule was never on by default;
`statute.lock` shows that it is out.

## g. Lock, compile, check

1. `statute lock`. In a public repository with private rules, it
   writes the private lock file as well.
2. `statute compile`. It writes the compiled file, the installed
   skills, and with a private layer the private output
   and the private links, and the statute ignore file before the files
   it lists. When it names lines to add to the exclude file or a file
   git does not ignore, fix that and run it again; it writes nothing
   until everything is in place.
3. `statute check`, which exits 0 when no rule a script checks is
   violated, and lists the rules for review.
4. In a public repository with a private output (private rules, or a
   `personal` value that differs from the committed one), install the
   checkout hook, so that each new worktree and each switch of branch
   writes the private output and the private links
   ([Private rules in a public repository](https://github.com/Luolc/agent-constitution/blob/main/docs/design/rule-sources.md#private-rules-in-a-public-repository),
   A new worktree).
   statute does not install it. First see which hooks are really
   there, under `git rev-parse --git-path hooks`, and then decide
   whether to write the hook directly or go through a hook manager.
   With no checkout hook, write one with
   `#!/bin/sh` and the hook line, and make it executable; when there is
   one, append the hook line and change nothing else in it. When a hook
   manager owns the checkout hook, add the line that manager's way;
   pre-commit runs a hook of stage `post-checkout` once installed with
   `pre-commit install --hook-type post-checkout`, and gives the
   checkout type in `PRE_COMMIT_CHECKOUT_TYPE` instead of `$3`. The
   hook does not reach the worktrees that exist already: run `statute
   compile --private-only` once in each of them.
   Install the merge hook the same way, with the merge hook line, so
   that a merge, a fast-forward of the main branch included, writes the
   private output anew: git runs no checkout hook then, and a checkout
   that only ever moves forward by merging, as a main checkout does,
   would keep the private output of its first checkout, or none. With
   pre-commit, the stage is `post-merge`, installed with `pre-commit
   install --hook-type post-merge`. The private lock
   file lists only sources; to see which rules the private layer
   enables, read the rule ID lines of the private output.
5. Add `statute compile --check` to the repository's commit hooks, so
   that a commit cannot leave the compiled file or an installed skill
   out of step with the rule set
   ([Compilation and the site](https://github.com/Luolc/agent-constitution/blob/main/docs/design/statute-command.md#compilation-and-the-site)).
   With pre-commit, add the hook from the mirror repository, which
   installs statute from its prebuilt wheel on PyPI:

   ```yaml
   - repo: https://github.com/Luolc/statute-pre-commit
     rev: v<version>
     hooks:
       - id: statute-compile
   ```

   `<version>` is the one the `statute` line of `statute.lock` names,
   which `sed -n 's/^statute = "\(.*\)"$/\1/p' statute.lock` prints.
   The two must name the same version: `statute compile --check`
   refuses a lock file another version wrote, so a `rev` out of step
   with the lock file fails the hook. The hook's default `files` covers
   `statute.toml`, `statute.lock`, `AGENTS.md`, `.statute/`, `.agents/`,
   `.claude/` and `.pre-commit-config.yaml`; when the local rules or
   another statute input or output lie elsewhere, override `files` with
   that list plus their directories. With another hook manager, run
   `statute compile --check` that manager's way. With no commit hooks,
   add only the CI step, and say so in the pull request description.
   Then confirm CI runs it: when CI already runs every pre-commit hook,
   nothing more is needed; otherwise add a CI step that runs
   `pre-commit run --all-files`, or only this hook with `pre-commit run
   statute-compile --all-files`, so that CI gets statute from the same
   `rev` and fetches no statute of its own. Without pre-commit, CI
   installs the statute version of the lock file and runs `statute
   compile --check`.

Acceptance, before you ask for a review:

- `statute compile --check` exits 0, which shows the public output is
  what a fresh compile writes (`statute compile` prints `wrote` for a
  file even when its content did not change), and `statute compile`
  warns of no `CLAUDE.md` unless the person chose to keep it. Where there
  is a private output (a public repository with private rules, or a
  personal value that differs), git ignores it, so compare it by hand:
  copy the private output aside, note where each private link points
  with `readlink`, run `statute compile`, and `cmp` each file with its
  copy and compare each `readlink` result with the note.
- The compiled file's first line says statute wrote it.
- The pull request adds the `statute compile --check` hook, its `rev`
  naming the version of `statute.lock`, and the CI
  step too unless CI already runs every pre-commit hook, and changes
  nothing else in the hook and CI configuration. On the pull request,
  CI runs `statute compile --check`, and it passes.
- `statute check` exits 0.
- The compiled file is compared with the legacy copies, section by
  section, against the mapping of step c: every item is where the
  mapping says, and every difference is recorded in the pull request
  description as intended.
- In a public repository, `git status --ignored` shows the private
  output and the private links as ignored, and `git ls-files` lists
  none of them; the private rule set and the private lock file are in
  git's own directory, where git tracks nothing.
- In a public repository with a private output, the checkout hook holds
  the hook line, and a worktree added with `git worktree add` has the
  private output and the private links without any other command.
- In a public repository with a private output, the merge hook holds
  the merge hook line, and a fast-forward writes the private output:
  in the working tree of the migration, `git switch -c merge-hook-test
  HEAD~1`, delete the private output, `git merge --ff-only -` (back to
  the head of the migration branch), and the private output is there
  again; then `git switch -` and `git branch -D merge-hook-test`. git
  runs the merge hook only when a merge moves the branch, so a merge
  that finds nothing to do shows nothing.

Commit the rule set in the same commit as the compiled file, and no
earlier: until then it stays in the working tree, uncommitted. Some
environments read a rule set at the top of a repository as the sign
that the repository has moved to statute and that its compiled file is
all an agent there follows, and set aside the instructions from
outside the repository. A rule set committed while the compiled file
is still the old one would make an agent drop those instructions, the
safety rules among them.

The pull request carries the rule set, the lock file, the compiled
file, the installed skills, the local rules, the
legacy copies, the statute ignore file and the compile check in the
commit hooks and CI, and nothing else: no other code, however small
the fix. It is one pull request, not an instruction change and a code
change: under rule R334 the hook and CI steps that run statute are
part of the instruction system and change with it, and R334 has this
pull request add the compile check. R46 and R280, which R334
supersedes in turn, do not count those steps so: a rule set that
would enable either enables R334 in its place (step f). Its
description holds the mapping, the three lists with their reasons
and the answers to the questions.

## New clone

A new clone of a public repository with a private output, on a new
machine or one rebuilt, holds only what git tracks: none of the files
the table marks "no, in git's own directory" or "no, ignored through
the exclude file" are there, and statute's cache holds no private
source. Check statute and the global configuration first (step 0.1
and 0.2; `statute upgrade --private` needs statute 0.1.23 or later),
then put the rest back in this order, from the directory of the rule
set in the main worktree: a linked worktree writes a private lock file
of its own, which the main worktree and the worktrees added later do
not read (the table):

1. Add `/CLAUDE.local.md` and `/AGENTS.override.md`, the lines for the
   private links, to the exclude file (step g.2).
2. Put the private rule set in place: copy it from another clone of
   the repository, or, with none left, write it again, reviewed as the
   note at the top says: `statute source add --private <name>
   <address>` for each private source, then `statute add --private
   <rule>` for each rule it enabled.
3. `statute upgrade --private`, which fetches the private sources at
   their newest commits and writes the main worktree's private lock
   file. To keep the commits another clone follows, copy its private
   lock file as well and run `statute compile` in place of this step
   and step 5: `statute compile --private-only` fetches nothing, and
   fails on a commit that statute's cache lacks.
4. Install the checkout hook and the merge hook (step g.4).
5. `statute compile --private-only`, which writes the private output
   and the private links.

Acceptance:

- The private output holds the private layer: among its rule ID lines,
  `grep -cE '^- <name>:?[0-9]+ ' <the private output>` counts more than
  0 for a private source `<name>` of the private rule set. Only this
  check shows it: neither the exit status of `statute compile
  --private-only` nor a private output being there does. Without the
  private rule set, once the exclude file has the two lines, it exits
  0, writing no private output when nothing
  else is private, or one without the private layer when the global
  configuration makes a private output by itself (step 0.2).
- `statute compile --private-only` fails without the two lines: take
  them out of the exclude file and run it, and it exits 1, naming the
  lines to add; put them back and run it again, and it exits 0.

## Upgrading statute

To move a repository to a newer statute, run `statute lock` with the
new version, then `statute compile`, and change `rev` of the
`statute-compile` hook to `v<new version>`, all in one pull request:
with the lock file and `rev` changed apart, the hook fails on the pull
request in between. `statute lock` keeps the pinned sources and changes
only the `statute` version line (and the `[[commands]]` tables where
the new version records others). `statute upgrade` also moves the
sources to newer commits, which changes the rules the repository
follows: run it in a pull request of its own, as an instruction change,
before or after the one that changes `rev`. Under rule R334 the lock
file and the `rev` of the `statute-compile` hook are both part of the
instruction system, so the pull request is an instruction change. A
repository whose own rules keep a change of a hook's `rev` apart from
every other change first excepts the `rev` of the `statute-compile`
hook from them, so that it changes together with `statute.lock`.

Acceptance: `statute compile --check` exits 0 with the new statute, and
on the pull request, CI runs the hook at the new `rev`, and it passes.

## Questions

Ask together, each with its default, by whatever route the person
gave you. Q1 comes in step 0, before `statute init`; Q2 to Q5 once the
mapping and the draft lists exist.

| | Ask | Default |
| --- | --- | --- |
| Q1 | The global configuration does not exist (or names no private source). Write it with `statute init --global`, and which private source should it name? | Write it with the built-in first-party source; name a private source only when the person gives one. |
| Q2 | The hand-written files are kept as legacy copies (list them). Delete them? | Keep them. Delete only on a yes. |
| Q3 | `CLAUDE.md` is a link to `AGENTS.md`. Delete it? (the explanation in step e) | Delete it. |
| Q4 | Here are the three lists, each line with its reason. Anything to change? | As drafted. |
| Q5 | These parts of requirements may or may not be private (list each with the private asset it rests on). Public or private? | Private, until answered. |

# rrx-core

Standing rules for agents working in this checkout.

Read the workspace layer first: [`../../../AGENTS.md`](https://github.com/wiltonlazary/rui/blob/main/AGENTS.md) and [`../../../.ai/config/`](https://github.com/wiltonlazary/rui/tree/main/.ai/config)
(javascript.md, design.md, benchmarks.md, concepts.md, resources.md,
stability.md, git.md).
Then read [README.md](README.md) for the API, design and benchmarks, and
[TODO.md](TODO.md) for open work.

## What this is

rrx-core is the reactive core, a production library packaged as the `rrx-core`
package: signals, memos, derivations, effects, scopes, contexts, tasks,
stores and keyed lists on an integer-id graph. The renderer-neutral
component layer is the sibling project ren-core
([`../../../ren-core/wtree/_repo_`](https://github.com/wiltonlazary/ren-core)), which depends on this checkout through a
`file:` link. The renderers are packages of
their own on top of ren-core: ren-dom, ren-canv, ren-skia, ren-three,
ren-html and ren-json.

## Layout and outputs

- Checkout: `wtree/_repo_` (branch `main`); feature branches are sibling
  worktrees under `wtree/<name>`, each with its own build directory
  `build/<name>_<version>` and its own `artifact` link to it.
- Build output: `build/_repo__0.1.10`, reached through the ignored `artifact`
  link. Scratch, profiles and bench artifacts go to `artifact/tmp/<task>/`,
  never into tracked paths or outside this project.
- `types/` is generated (`scripts/types.mjs`) and ignored; the package's
  `exports` point at it.

## Project specifics

- ren-core relies on the exports, the `rrx-core/core` and
  `rrx-core/context` hooks and their behavior; rui-core on the exports
  (through ren-core). The export lock
  (`test/exports.lock.json`) names them all;
  `grep -n "from 'rrx-core/" ../../../ren-core/wtree/_repo_/src/*.js`
  lists the hooks ren-core imports.
  A change to any of them must keep ren-core's, rui-core's and ren-show's
  suites green: run `npm test` in each before committing it, and record in
  the commit what each needs.
- `test/core.test.js` runs against both the records core (`src/core.js`) and
  the column arena (`src/arena.js`); keep the two on the same contract.
- README §Design choices records each deliberate layout or shape choice with
  its measured effect; add to it when you make one.
- ren-bench owns the harness. Its `bench/reactivity/scenarios.json` is the
  canonical scenario definition, with the
  expected checksums (`sum`, `runs`) every library must match.
  `RRX_SCENARIOS=<path>` runs the harness on another file in the same format.

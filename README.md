# rrx-core

Fine-grained reactivity on an integer-id graph: signals, memos, derivations,
effects, scopes, contexts, tasks, stores and keyed lists, in plain JavaScript
with JSDoc types, strict `tsc` checking and generated declarations. rrx-core is the
reactive core; [ren-core](https://github.com/wiltonlazary/ren-core) is the renderer-neutral
component layer built on it, and the renderers are packages of their own on
top of ren-core (ren-dom, ren-canv, ren-skia, ren-three, ren-html, ren-json).

The package is published on npm as `@maxenve/rrx-core` under the MIT license;
install it under its import name: `npm install rrx-core@npm:@maxenve/rrx-core`.
There are no runtime dependencies.

| Entry | Contents |
| --- | --- |
| `rrx-core` | the reactive core, tasks, stores, lists and selection |
| `rrx-core/store` | stores alone |
| `rrx-core/arena` | the core over integer columns with recycled ids (for measurement; same contract) |
| `rrx-core/core` | the records core module itself, including the renderer hooks a component layer binds to (`Application`, `$bind`, `waitedOn`) and the `Graph`, `Node` and `Owner` types |
| `rrx-core/context` | the context records (`$context`, `Layer`, `Provided`) and the root id check (`checkId`) |

`rrx-core/core` and `rrx-core/context` exist for a component layer such as ren-core; an
application imports `rrx-core`.

## Core

```js
import { $root, $signal, $memo, $watch, $read, $write, $flush, $dispose } from 'rrx-core';

const root = $root('app');
const count = $signal(root, 0);
const twice = $memo(root, () => $read(count) * 2);
$watch(root, () => $read(twice), value => console.log(value)); // 0
$write(count, 2);
$flush(root); // 4
$dispose(root);
```

By default, writes coalesce until `$flush(root)`. `$auto(root, true)` flushes
every write outside a `$batch` at once. Effects run
when they are created, and again in creation order at each flush, so an owner
always runs before the children it may dispose.

| Group | Operations |
| --- | --- |
| roots | `$root`, `$dispose`, `$closed`, `$owner`, `$stats`, `$development` |
| nodes | `$signal`, `$read`, `$write`, `$equals`, `$memo`, `$scan`, `$derive`, `$writable`, `$whenUnwatched`, `$discard`, `$invalidate`, `$touch` |
| observers | `$effect`, `$watch`, `$render`, `$cancel`, `$untrack`, `$observer` |
| scheduling | `$flush`, `$batch`, `$batching`, `$auto`, `$defer`, `$deferred`, `$after`, `$settled`, `$commit` |
| ownership | `$scope`, `$enter`, `$stop`, `$cleanup`, `$release`, `$cleanups`, `$alive`, `$mount`, `$mounts`, `$unowned`, `$retire`, `$sweep`, `$sweeper` (background disposal) |
| context and failures | `$context`, `$provide`, `$lookup`, `$update`, `$entries`, `$with`, `$current`, `$catch`, `$throw`, `$deliver` |
| introspection | `$kind`, `$sources`, `$observers`, `$identity` |
| tasks | `$task`, `$stream`, `$load`, `$outcome`, `$lastReady`, `$pending`, `$rerun`, `$behind`, `$whenDone`, `$optimistic`, `$act`, `$changing` |
| stores | `$store`, `$at` (own keys only: an inherited member such as `constructor` reads as missing), `$put`, `$raw`, `$update` (on a store path; an array there stays an array, merged by index as `$put` writes one), `$reuse`, `$mirror`, `$overlay` |
| lists | `$keyed`, `$rows` (Keyed with item accessors whose signal exists once read tracked), `$positions` (each takes `{ retire: true }` to retire removed rows, or `{ retire: n }` to retire those of updates that remove at least n), `$select` (unobserved keys released in amortized sweeps and with the reader's owner; a direct call answers fresh, earlier readers follow at the next flush), `$merge`, `$split` |

Some points worth knowing:

- `$root(id)` takes the root's id, `[a-z][a-z0-9-]{0,31}` (checked; kept as
  `root.id`). Everything above the graph derives from it: an application
  root's storage keys, lock names and element ids. rrx-core itself does not
  require ids to be unique; an application host does (see Application roots).
- `$defer(root, fn)` queues the effects `fn` invalidates in the low lane,
  after urgent work. Deferred work stays deferred: what the low lane's
  effects and applications write queues its readers in the low lane too
  (`$deferred(root)` is true there), so work handed on from deferred work
  (a region's placement, a list's update) never jumps ahead as urgent.
- A memo that nothing watches does no work until it is read. It keeps its
  value across unrelated writes. When it is read after a source change, it
  revalidates against the `seen` versions of its links, and recomputes only
  if a source actually moved.
- A memo whose computation throws keeps that failure as its result. A read
  raises it, and a later source change recomputes it. A read that is pending
  (a task that is not ready yet) throws the `WAITING` sentinel, which
  `$outcome`, `$lastReady`, `$pending` and the ren-core boundaries turn back into data.
- `$watch(root, read, apply)` and `$render(root, read, apply)` return
  `{ observer, stop }`. `read` is pure, so a write inside it throws. `apply`
  runs after the computations settle, and only when the value changed; Render
  applications run before Watch applications.
- A node created inside a scope is freed when the scope stops. Its readers are
  marked dirty and recompute without it. A freed handle reads its final value
  untracked; a disposed root refuses.
- A self-triggering effect is reported as a cycle after 100 runs in one flush,
  and commit hooks that keep the graph busy are reported after 100 rounds.
  Failures go to the nearest `$catch`. The flush completes, then raises the
  first unhandled failure.
- A `$batch` whose build throws still flushes what it wrote, then raises the
  build's failure; a failure that flush leaves unhandled is raised by the
  next `$flush`.
- A list never fails because a removed row fails to stop: the row still
  stops whole, its failure goes to the list owner's `$catch` (or is raised
  when the flush ends), and the list commits its new rows.
- A task read after its source changed, before the flush, launches the new
  work at once and reads pending (with `$lastReady` kept); its readers track the
  task's state, which tracks the input, so a cached reader rereads it. The
  flush does not launch the same input again, and superseded completions stay
  ignored. A completion that arrives after the input changed, before
  anything launched for it, first brings the task up to date: the new work
  launches and the completion is ignored as superseded.
- A task's work may write the task's own input while it starts: the input is
  read again after that launch, the running work is cancelled and the new
  input launches in the same computation (at most 100 times, then reported
  as a cycle).
- A cancel that throws, when a task relaunches or stops, goes to the task
  owner's boundary (raised when the flush ends if none handles it); the
  new input launches all the same.
- A task launches for every new input whether or not anything reads it.
  A read can pull the new launch before the flush; the flush then sees the
  clean task and does not launch it again.
- `$mirror` applies its first value at once, inside a `$batch` too, so no
  reader sees the empty store.
- `$behind(source)` is true while a shown value waits on newer work for a
  changed input or a `$changing` mark. A value counts once it has been
  ready, null and undefined too; a first load and a quiet `$rerun` do not.
- **Async memos.** A memo whose computation returns a promise or an async
  iterator is async: async lives in the graph. It reads pending,
  keeping its last value for `$lastReady`, until the promise settles (a
  rejection is its failure) or the iterator yields (each value in turn; the
  end keeps the last). Only the reads before the first `await` are tracked. A
  newer run supersedes the work in flight: its completion is ignored and an
  iterator is stopped (`return()`); a completion that finds the memo's
  sources changed brings it up to date first. The root counts the work as
  loading, so `$settled` waits for it, and a freed memo ends it. rrx-core
  makes no promise of its own: it only subscribes to the one returned.
- **Transitions: no tearing.** A Watch or Render whose value is shown and
  whose read goes pending (an async memo or task with a new input) suspends:
  it keeps its value, and every application of that update is parked in a
  transition until nothing in it waits, then all apply together in creation
  order, so the screen never mixes the new state with the old (the graph
  holds a consistent state). Reads in code see the newest state at
  once. Observers whose latest run read `$pending`, `$behind`,
  `$lastReady` or `$outcome` are indicators and apply at once. An update that
  touches parked work, or resumes a suspended observer, joins its transition
  (an update touching two merges them); unrelated updates apply as usual,
  and plain `$effect`s are never held. A suspended observer that stops no
  longer waits, and one whose read fails resumes, so the failure's boundary
  shows at once. `rrx-core/core` exports `suspend(observer)`,
  `resume(observer, stopping)`, `peeked()` and `peekCount()` for a renderer
  whose bindings catch their own pending reads (ren-core).
- **Development mode.** Under the `development` export condition (a
  bundler's dev server, esbuild `conditions: ['development']`, `node
  --conditions=development`), or after `$development(true, report?)`,
  rrx-core reports what the model makes easy to get wrong: a Watch or Render
  application that reads reactive state (untracked, so it never reruns).
  ren-core adds reads in component bodies and row callbacks. A second copy
  of rrx-core on the page is reported under the `development` condition
  only: `$development(true)` at run time comes after the copies loaded. Reports go to `console.warn`, or to `report`.
  Production builds carry the checks as one branch in the apply path.
- **Rerun is quiet.** `$rerun(task)` runs the work again and
  `$rerun(memo)` reruns an async memo now (when something reads it); the
  value shown stays ready, not pending, until the new one replaces it. A changed input still reads pending.
- **Changing.** `$changing(source)` declares that a task or memo is changing:
  it reads pending, keeping its value for `$lastReady`, so `$pending` shows it
  and a transition holds what shows it. Made while an
  action starts (`$act`), it ends when the action completes or is stopped;
  otherwise at the source's next value, so `$changing(user); $rerun(user)`
  is a reload that reads as pending. The function it returns ends it
  earlier. A signal cannot be marked changing (TypeError).

### Background disposal

`$retire(scope)` makes a scope and everything it owns inert in O(1): no
effect, memo, Watch, Render or `$commit` hook of it runs again, whatever is
written later, and `$alive(scope)` is false. The freeing happens later, in
the order `$stop` uses (owned nodes newest first, child scopes depth first,
then the scope's own cleanups newest first), by `$sweep(root, budget)`:

- `$sweep(root, ms = 4)` frees retired work for about `ms` milliseconds and
  returns how many retired scopes remain (0 when done). It reads the clock
  after each chunk of about 0.05 ms of work, sized from its running cost
  estimate (an exponential average), so a slice overshoots by about one
  chunk. Every call frees at least one chunk; while effects or applications
  are queued it frees only that chunk, and inside a flush it does nothing, so
  urgent work always goes first. A host that sees a late frame passes a
  smaller budget.
- A cleanup that throws during a sweep goes to the nearest `$catch`; the
  sweep goes on, and the first unhandled failure is raised when the slice
  ends.
- `$sweeper(root, schedule)` installs the host's scheduler: `schedule(root)`
  is called once when retired work appears after none was left, and the
  host calls `$sweep` until it returns 0. `$sweeper(root, null)` leaves
  sweeping to explicit calls. Without a scheduler, a host with idle
  callbacks (`requestIdleCallback`) sweeps in its idle periods, each slice
  the idle time left and at most 8 ms, and in 2 ms slices when 100 ms pass
  without an idle period, so a page that is never idle still finishes;
  elsewhere rrx-core sweeps in 4 ms slices on macrotasks (`setTimeout`).
- `$stop(scope)` on a retired scope and `$dispose(root)` finish the teardown
  at once. A retired memo read from outside keeps its last value until it is
  freed; a late task completion into a retired scope is ignored.
- `$stats` counts a retired scope's nodes until they are swept (they still
  exist).
- A list's rows live in one scope of their own (made with the first row). A
  retiring list (`{ retire: true }`) whose update keeps none of its rows (a
  clear, or a replace by new keys) retires that one scope instead of each
  row, so the caller's cost does not grow with the row count.
- `{ retire: n }` decides per update: an update that removes at least n rows
  retires them (one mark each, or the one row scope when none stay), and a
  smaller update stops its removed rows at once, as without the option. A
  host passes its own threshold, so a large partial removal (9,000 of 10,000
  rows) goes to the background too while small edits stay synchronous.

A host with frames of its own schedules `$sweep` in a small slice (2–4 ms)
after each frame's work, passing a smaller budget after a late frame, and a
worker host between messages. The column arena (`rrx-core/arena`) has `$retire`,
`$sweep` and `$sweeper` with the same contract; it has no lists or tasks (it is
the graph algorithm over columns, for measurement).

### Contexts

A context is a typed key, and itself the provider component:

```jsx
import { $context, $lookup, $update, $entries, $provide } from 'rrx-core';

const Theme = $context('light');            // $context<string>, with its fallback

<Theme value="dark">{/* everything built below sees "dark" */}</Theme>;
$provide(root, Theme, 'dark', () => build()); // the same, without JSX

$lookup(Theme);                             // the nearest value, or 'light' (running root)
$lookup(root, Theme);                       // the same, naming the root
$update(Theme, 'blue');                     // writes the nearest provider
$update(root, Theme, previous => `${previous}!`);
$entries(root);                             // [[context, value], …] visible there, nearest first
```

- A provider holds its value in a signal (a writable derivation when the
  value is an accessor or node, so it follows its source until an `$update`
  overrides it). `$lookup` inside a computation tracks that provider, so
  `$update` reaches exactly the consumers below it; consumers of another
  provider, or outside, do not run.
- Values reach everything created under the provider: scopes, lazy
  components, `Show`/`Choose` branches, `For` rows, `Portal`,
  `Pending` and `Recover`. Nested providers shadow outer ones. Event
  handlers run in the context their element was built in.
- Each context operation has exactly two spellings:
  root-first `$lookup(root, context)`, `$update(root, context, value)`,
  `$entries(root)` (and ren-core's `$name(root, name)`), like
  `$signal(root, …)`; and the short `$lookup(context)`,
  `$update(context, value)`, `$entries()` (and `$name(name)`), told apart
  by the first argument (a root is a `Graph`), which use the root whose
  code is running: inside `$enter`, `$with`, a flush (effects,
  applications, hooks), an effect's first run, a component or an event
  handler. The register is set once per flush and per `$enter`/`$with`,
  not per effect run, and restored after, so it never names another root.
  A memo sees the context of the code that pulls it.
- `$current(root)` captures the current layer and `$with(root, layer, fn)`
  re-enters it later (a callback, a continuation). A context is found by
  identity.
- `$update` is one operation on three shapes: `$update(root, context, value)`,
  `$update(context, value)`, and a store path (`$update(store, path,
  fields)`); rrx-core exports one `$update` that dispatches on its first argument.


## Design

- **Graph**: every node has an integer state (clean, pending, dirty), a
  liveness that includes running (freed, closing, alive, running), a `mode`
  whose meaning depends on the kind (a memo's readiness, an effect's queue
  lane, a signal's write permit), a version (an effect's run count within a
  flush), doubly linked dependency and subscriber lists, and a watcher
  count. Links carry `seen` (the source's version when last read) and
  `cycle` (the tracking run that last confirmed them).
- **Tracking**: a repeated read of the tail does
  nothing, the next link is reused, and otherwise a new link is inserted
  after the tail unless the source's last subscriber link already belongs to
  this run. The links after the cursor are purged when the run ends. A rerun
  that reads the same sources allocates nothing.
- **Propagation** walks the subscriber lists without computing anything.
  Direct subscribers become dirty, and the rest pending. Pending marking
  walks a reused list of memos in the order they were reached.
- **Checking** (`refresh`) is iterative, with an explicit stack of (node,
  cursor) frames. A pending source memo is resolved before its reader
  continues; a dirty one recomputes; a version past `seen` makes the reader
  dirty. A chain of 10 000 memos propagates and checks without recursion. A
  first computation still recurses through user code.
- **Watchers**: a memo's first watcher attaches it to its sources,
  iteratively; its last watcher detaches it. A signal left without watchers
  runs its `$whenUnwatched` hook.
- **Flush** phases: urgent effects in creation order, then low-lane effects,
  Render applications, Watch applications, `$commit` hooks, `$after`, and
  `$settled` once no task is pending. Under `$auto`, a `$stop` or a `$sweep`
  slice that ends the last pending task flushes too, so `$settled` runs
  without a `$flush`. The cycle guards are 100 runs and 100 rounds.

### Design choices

Each choice below records what rrx-core does instead of the obvious alternative,
and why. Measured effects are from paired in-process runs; see Benchmarks.

1. **Records instead of columns.** The production core
   (`src/core.js`) stores each node as one fixed-shape `Node` record and
   each edge as one `Link` record. Records are never reused, so `gen` is
   always 0, `id` doubles as the creation order, and a freed node keeps its
   final value (`last`). `src/arena.js` is the same algorithm over
   `Int32Array` columns, recycled ids, `gen` and `order`; it passes the
   same core test suite. In V8 a record field is one
   load, while a typed-array column access also loads the column, its
   backing store and its length for the bounds check. ren-bench's
   reactivity results run the arena as `rrx-arena`: about 2.4 times the
   records core's time (geomean over the market scenarios; see Benchmarks).
   An interleaved single-array layout measured no better than separate
   columns.
2. **Scopes join owner lists.** Scopes are nodes. Their owned nodes and
   child scopes form one intrusive doubly linked list, so owning a node
   allocates nothing, where a `$cleanup` closure per owned node in a bag would
   allocate one each. `$stop` frees owned nodes newest first, then runs the scope's own
   cleanups newest first.
3. **Watch and Render keep their state on the node.** A small application
   record per Watch holds the last applied value and the queued lane, so a
   run allocates no closure or entry record.
4. **No re-marking after a recompute.** A memo whose value changed does not
   propagate to its subscribers again. With versions, that would be redundant: a memo that is not clean never has clean
   readers, so they already find the new version through `seen`.
5. **Readiness encoding.** A memo's readiness (its `mode`) is `NONE`,
   `VALUE`, `FAILURE` or `WAITS`, so a clean read tests one integer instead
   of a flag and then the `waits` and `failure` payloads.
6. **Pending marking in reach order.** Effects queue in
   subscriber order, so an already ordered batch needs no sort.
7. **Tracking cycle (records only).** A nested computation does not restore
   the outer run's cycle. At worst, an outer run then adds one duplicate link.
   The arena restores it.
8. **Identity equality.** Values compare with `Object.is` by default, not
   structurally. `$mirror` keeps the old references of equal parts, so
   only the paths that changed notify, as structural equality would. A task
   does not relaunch for an equal outcome.
9. **Pending reads are a thrown sentinel.** A pending read records what it
   waits on and throws `WAITING`; every boundary turns it back into data.
10. **`$keyed` keys.** `$keyed` accepts any key value (a `Map`), and a null key
    function uses the item itself, which `For` needs. Duplicate keys raise.
11. **Unchanged store subtrees are skipped.** `$put` skips descendant subtrees
    whose reference did not change: every cell below them would be written its
    own value, which is a no-op under identity equality.
12. **Module-local constants** (the records core; the arena entry exports
    its own). The kind, state, lane,
    readiness and liveness constants are not exported. V8 keeps every exported
    binding in a cell, so optimized code reloads an exported constant and
    checks it for the uninitialized hole at each use. A module-local `const`
    folds into the code as an immediate. Measured: deep −16%, avoidable −15%,
    memo create and free −10%, churn minimum −12% (steady state).
13. **Counts come from the owner tree.** `$stats` walks the owner lists
    instead of keeping live node, link, effect and scope counters on every
    creation, link and free. Measured: neutral within noise (±2%), with 12 fewer
    counter updates per memo/effect pair.
14. **Freeing does only what correctness needs.** A freed record
    unlinks its sources, orphans its readers, leaves its owner list and is
    marked freed. It keeps its payload fields, which the collector reclaims
    with the record, and its final value (`last`). The arena still clears,
    because it recycles ids. Measured: neutral.
15. **A new effect runs without a second try.** The first run is
    `execute` without the checks a new node cannot need, and a failure is
    delivered in place instead of thrown to a catching caller. Measured:
    create-and-free of an effect −18%, churn −3% to −5%.
16. **Hot helpers are inlined by hand, and the constructor joins the owner
    list (same algorithm).** V8's baseline and mid-tier
    compilers inline only one level of calls that are not tiny, so the
    `Node` constructor refuses a closed root, derives the owner and the first
    state, and joins the owner's list itself; `$signal`, `$memo` and `$effect`
    stay small enough to inline; `purge` and `insert` inline the watcher
    bookkeeping. Measured: churn −6% in the harness (warm-up) shape, neutral
    in steady state.
17. **The mark list is cleared after propagation.** The list of
    memos a write reached is emptied after the walk, so it cannot
    keep freed nodes alive through a young-generation collection. Measured:
    churn −11% in the harness shape, where collections are frequent; neutral
    in steady state.
18. **Fields shared by kind, not packed into a flag word.** A memo's
    readiness, an effect's queue lane and a signal's write permit share one
    field (`mode`), an effect keeps its cycle-guard count in its unused
    `version`, and running is a liveness (FREED, CLOSING, ALIVE, RUNNING: a
    closing node is always mid-run, so running is an odd liveness). A node is
    20 fields, 184 bytes, against 23 and 208 with a field each. Measured
    (paired ABBA, 16–24 pairs, load 1.6–2.9): the shared fields churn −2.7%
    (12/16 pairs), the rest within ±1%; the liveness fold neutral (±2.5%).
    A single bitwise flag word (kind, state, liveness, running, readiness,
    lane and permits: 17 fields) measures churn −3% to −10%, but avoidable
    and unstable +3% to +9% in the harness shape, where their timed rounds
    still run in Sparkplug and Maglev and each mask is one more bytecode (a
    feedback-updating builtin call in Sparkplug); in steady state it is
    neutral. Keeping `state` and `running` as plain fields beside it (19
    fields) shows the same pattern (avoidable +9%). Compute's rare paths sit
    in helpers, which keeps Read, readMemo and compute within TurboFan's
    cumulative inlining budget (with them inline, diamond +5.5%).
19. **Retirement is checked only while retired work waits.** Effects,
    applications, `$commit` hooks and memo recomputes test `R.retiring` (one
    load of a root field) and only then the owner's cached retire epoch
    (`scope.seen`, one more field per scope), with an ancestor walk once per
    scope per `$retire`. Unguarded, the check costs deep, diamond and avoidable
    2–5%; guarded, every scenario is within ±2% (ABBA, 16–20 pairs, load
    2.3–4.2).
20. **`$retire` is O(1) per scope; the sweep is time-sliced.** ren-bench's `bench/reactivity/sweep.mjs`
    (node --expose-gc, 5 repeats, rows of memo + effect + cleanup, load
    3–5): making 10k rows inert takes 0.32 ms against 0.69 ms for `$stop`, and
    100k rows 7 ms against 14 ms (one cache miss per scattered row scope,
    where `$stop` touches every node and link). The sweep then frees 10k rows
    in 0.7–1.0 ms and 100k in 16–19 ms in total; the slowest slice is
    2.05/4.05/8.05 ms at budgets of 2/4/8 ms (8/4/3 slices for 100k), so a
    slice overshoots its budget by at most 0.06 ms.
21. **A task is one state memo and a record.** Its state is a memo whose
    computation reads the input and, when the input's outcome (status,
    value and error by identity, kept as three fields, not a record)
    changed, launches the work and yields the pending state; a completion
    stores the next state into it as a local write (`assign`, a `$derive`
    write without the Extra record). The memo reaches the task through its
    unused `apply` field, so it needs no closure. Readers read only that
    memo, which gives the stale-read pull for free. Measured (ABBA, 16
    pairs) against a task made of a scope, a state signal, an input memo,
    a Watch with its Application, and closures for publishing and
    launching: async −47% (16/16, load 14); without the Extra record and
    the closure for the memo, a further −4.6% (12/16, load 2.7).
22. **A list's rows live in one container scope.** Keyed, Rows and Positions
    attach their rows to one scope made with the first row, under the list's
    owner; a retiring list whose update keeps no row retires that scope in
    O(1) and makes a fresh one for the next rows. Keys that stay at either
    end rule a full replace out without a scan. Measured
    (ren-bench's `bench/reactivity/sweep.mjs` list-retire, 100k rows, ABBA, load 2.1): the caller
    blocks 0.03 ms against 5.7–7.2 ms retiring each row; the sweep total is
    the same.
23. **A lazy row keeps no accessor; a scope's rare fields are one record.**
    A Rows row hands `build` its item accessor without storing it (the item
    value sits in the `item` field), so a row that reads its
    item only while it builds, as ren-core's rows do, keeps no closure. A
    scope's `$catch` handler and `$mount` callbacks live in a record made on first
    use (13 fields, against 14 with a field each). Measured (10k rows, one
    effect each, after a full collection): Rows 583 bytes a row against 697
    with a stored accessor, Keyed 770 against 777, Positions 730 against
    739; building and clearing 1,000 Rows rows −10% (ABBA, 16 pairs, 12/16,
    load 4.1); disposal and churn neutral (13/24, 9/16).
24. **`$stop` frees plain nodes inline.** A node that is not running and has no
    cleanup is freed inside `stopScope` as the tail of its owner's list,
    without the release, cancel, free and leave calls (which V8's baseline
    and mid tiers do not inline). Measured: the stop phase of a memo/effect
    pair 27–45 ns, against 39–61 ns through those calls (six processes each,
    harness shape; the fastest market library 28–37); churn −5.2% and −4.3%
    (ABBA, 32 pairs, 21/32 and 18/32, load 3.6–4.1); disposal neutral (13/32).
25. **The arena pays the same retirement guard.** `$retire` and `$sweep` in the
    arena use the records core's guard (`R.retiring`, then a per-scope
    epoch column), so the two layouts run one algorithm. Measured on
    `rrx-arena`: avoidable +2.8% (ABBA, 24 pairs, 6/24), deep, diamond and
    churn neutral.
26. **Operations start with `$`.** Every public operation is a short
    lowercase word or phrase after a `$`: `$root`, `$signal`, `$read`, `$flush`,
    `$catch`. Classes and types stay capitalized (`Graph`, `Node`,
    `Readable<T>`, `Writable<T>`).
    - reserved words stay usable: `$catch`, `$throw` and `$with` are plain
      identifiers, with no prefix word or longer name;
    - common words such as `$read`, `$write`, `$update`, `$stop`, `$positions`,
      `$select`, `$merge` and `$split` neither shadow nor are shadowed by an
      application's own `read`, `update` or `select`;
    - a capital keeps the meaning JavaScript and JSX give it: a class, a type
      or a component. A component layer's `<Root>` or positional `<For>` never
      collides with the operations `$root` and `$positions`;
    - `$` marks the reactive vocabulary: a reader sees which calls touch the
      graph, and an editor completes the whole family from `$`;
    - linters accept the calls (`new-cap` flags capitalized calls).

    The cost: one more character per call, and `$` at first reads like a
    library global or a template placeholder.

27. **A launch that writes its own input relaunches in the same
    computation.** The launch runs inside the task state's computation, so
    a write it makes to the input would otherwise be lost: the computation
    would finish as current and the task stay on the old input. After a
    launch, the state checks only this run's links (`moved`: a source whose
    version moved, or a memo source not yet recomputed) and, on that slow
    path, reads the input again. The check has no cost on writes; the
    straight path of the computation is unchanged. Measured against the
    task without the check: async 0.974 (ABBA, 20 pairs, 9/20). Counting
    every write on the graph instead costs 2–3% on writes, and a loop in
    the computation 6.5% on async.
28. **A task's state is an eager memo.** A memo with an application (a
    task's state: nothing else sets a memo's `apply`) is queued when a
    change reaches it, as an effect is, and the flush refreshes it before
    its lane's effects, from queues of its own (`eagerUrgent`,
    `eagerLow`), so the effect queue keeps its creation order and needs no
    sort. It is queued once per change, when it leaves CLEAN, so it needs no
    lane field; a read that pulled it first leaves it clean and the flush
    skips it. No observer reads the memo: it holds a watch of its own
    (`watchers` 1), so its input stays observed while the task lives,
    readers or not, and is released when it stops. Every guarantee above
    holds: the stale-read pull (a read of a dirty memo refreshes it),
    status at once, superseded completions ignored, eager launches.
    Measured against an eager observer (a Bind reading the memo on every
    flush, with its link, its pending mark and logic to rest while others
    read the task) (ABBA, 16 pairs, load 2.5–3.3): async 0.909 (15/16
    faster); deep 1.006 (8/16), broad 1.007 (8/16), diamond 1.011 (6/16),
    avoidable 0.987 (9/16), churn 0.991 (9/16), disposal 1.018 (7/16): the
    extra `apply` test on a memo leaving CLEAN reads neutral. Queuing the
    memos with the effects costs more than it saves: the effect queue
    loses its creation order and needs a sort at every flush. The job
    itself is the memo's `apply` (no Application record per task): async
    0.924 against an Application record per task (ABBA, 20 pairs, 15/20,
    load 2.7–3.6). A separate value signal for `$lastReady` readers (so a
    launch would not rerun them) cuts the relaunch phases by 40% but costs
    15–25% on creation, completion and disposal (a second node per task),
    and reads 1.02–1.17 with bimodal timings in pairs: not taken.
29. **A relaunch while pending keeps the state.** A task already pending
    for the same value shows the same snapshot after a relaunch, so its
    readers see no change and do not rerun; the pending state of a value is
    made once and kept for the next launch from that value. The launch
    still happens and superseded completions stay ignored. Measured: async
    0.952 (ABBA, 20 pairs, 15/20, load 2.6–3.0).
30. **A task's teardown is its state memo's cleanup.** An eager memo's
    `cleanup` (never a `$whenUnwatched` hook here: the memo's own watch keeps
    it observed) runs when the memo is freed, with its owner, by `stop` or
    when the root is disposed; it ends the job. No closure and no owner
    cleanup entry per task. Measured against an owner cleanup per task:
    async 0.946 (ABBA, 20 pairs, 18/20), disposal 0.968 (11/20), load
    2.9–3.4.
31. **A task's state queues its effect readers only when it changes.** A
    change reaching the state memo marks its memo readers pending (they are
    lazy and must see the change when read), but not its effect readers:
    the memo recomputes before its lane's effects, and when its computation
    yields a new state it marks them dirty and queues them itself
    (`changed`), whoever refreshed it (the flush, or an early read). A
    relaunch that leaves the state equal (pending again) therefore queues
    no effect at all; an effect already running is reading it and is not
    queued again. Deferred work stays deferred: the flush refreshes a
    deferred memo inside the transition, so the effects it queues are
    deferred too. The check sits in the effect branch of propagatePending
    (the memo branch is unchanged) and in the task's own computation, not
    in the memo epilogue, where it costs diamond +6.6% and broad +2.9%.
    Measured (ABBA, 24 pairs, load 1.1–2.5): async 0.969 (19/24);
    repeated 0.999, churn 0.988, unstable 0.992, diamond 0.996 (12–13/24).
    Steady relaunch cost with a reader: 41.5 ns per task per write, against
    85 ns when every effect reader is queued.
32. **A status change alone reaches only the status readers.** A task's
    readers read its value (`$lastReady`) or its status (`$read`, `$pending`,
    `$outcome`, `$behind`). A status read inside an effect stamps the
    effect with its current run (its `version`, kept in its `watchers`
    field, which an effect never uses: it is never a source). When only the
    status moved (a launch from ready, a completion with the same value),
    the task queues only the effects whose latest run read the status;
    when the value moved, all of them. Memo readers are always marked. An
    effect that stops reading the status stops rerunning for it at its
    next run; one that starts reads it again and is stamped. The filter
    sits in the task's own paths (`changed`, and `published` for its local
    writes), not in the generic propagate. Measured against a task that
    queues every effect reader (ABBA, 24 pairs, load 2.4): async 0.865
    (19/24; the scenario is bimodal for every library, q25 0.894 and q75
    0.929), churn 0.953, deep 1.001; status readers alone (phase pairs, 30
    pairs) 0.988 (15/30), value readers 0.934 (14/14). Keeping the stamp in
    the effect's Extra record costs status readers 2.7%.
    Measured with no paired win, so not taken: the relaunch check inlined
    for a single-source input and the launch's restore without `finally`
    (async 1.014); exporting the core's `CLEAN` constant for that check
    (deep 1.027 and churn 1.051: an exported module binding is a cell V8
    loads, not a constant it folds).
33. **Async memos cost the plain memo nothing.** A memo's readiness field
    already tells a value from a pending read, and every read already tests
    it, so an async memo reads pending through the existing path. The
    computation's epilogue settles the common result (a plain value, no
    Extra record, a memo already ready or new) inline; objects, failures,
    custom equality and async results go to a cold `outcome`. Testing for a
    promise and for work in flight inline costs diamond +9% (ABBA, 12
    pairs, 0/12); with the cold path, diamond 1.019, deep 1.014, broad
    1.011, churn 0.984 (12 pairs, 4–7/12, load 2.0–2.6).
34. **Transitions hold applications, not values.** Staging every
    write in a held transition, so reads outside it see the committed state,
    costs every read and write. rrx-core keeps reads immediate and holds
    the one thing a user sees, the applications of Watch and Render: the
    flush already separates computing a value from applying it, so parking
    a queued application is a branch in the drain. The costs sit off the
    hot paths: a field test when an observer runs (a suspended one resumes)
    and when an application queues (parked work joins), and the drain's
    branch while a transition is open. Measured against the core without
    transitions (ABBA, 12 pairs, load 2.5–2.9): deep 0.986, broad 1.008,
    diamond 0.989, churn 0.947, async 1.008, disposal 1.090 median with q25
    0.964 (6/12).
35. **Task readiness is materialized only when read.** The eager memo holds
    a numeric revision (`Job.node`); the Job holds the latest value,
    failure and packed status flags. `$lastReady` tracks the revision and
    returns the value without allocating a readiness record. Readiness
    readers materialize an immutable snapshot, and pending snapshots follow
    their reuse rule. Every publication increments the revision, so several
    completions before a memo read cannot cancel their notification. Task
    work changes the owner once; its callback restores that owner and the
    generic memo epilogue restores tracking, purity and context. Ordinary
    memo computation gets no Task branch. Eager memos with no readers or
    one effect reader skip the pending walk that would notify nobody, and a
    known dirty, live memo computes directly. Cancellation handles keep
    only callable returns.

Measured with no paired win, so not taken: a task's latest value in a
memo of its own, made on first `$lastReady` (so a reader of `$lastReady`
would not rerun for a pending publish): write phase +35% with the
scenario's trivial readers; the task's value and status in two nodes (value
memo, status signal): async +10.5% (ABBA, 16 pairs, 3/16); a free list of
recycled link records: churn neutral even without the safety a recycled
link needs (12/24).

`$update(store, path, fields)` merges fields, and `$mirror(root, derive)`
takes a single tracked computation.

## Tests

```sh
npm install        # pinned devDependencies (typescript); `prepare` generates types/
npm test           # declarations, tsc --checkJs --strict, the TypeScript consumer, node:test
```

- `test/core.test.js` runs every core operation and the documented edge cases
  against both the records core and the column arena.
- `test/task.test.js` and `test/store.test.js` cover tasks, streams,
  optimistic updates, stores, mirrors, overlays, `$keyed`, `$rows`,
  `$positions`, `$select`, `$merge` and `$split`.
- `test/retire.test.js` covers `$retire`, `$sweep` and `$sweeper` on both the
  records core and the column arena (inertness, `$stop` order, time slices,
  failures, `$stop` and `$dispose` of retired scopes, the default and idle-time
  schedulers), and on the records core the lists and tasks: retired rows,
  whole-list retirement, late task completions.
- `test/async.test.js` covers async memos: promises and async iterators in
  the graph, `$lastReady`, `$pending`, `$rerun`, `$behind`, `$outcome`
  and optimistic updates over them.
- `test/transition.test.js` covers transitions: an update that makes a shown
  value pending is held, whole, until the value is ready.
- `test/development.test.js` covers development mode: the reports for
  reads nothing tracks, and none in production.
- `test/differential.test.js` runs the same generated programs (fixed seeds)
  on the records core and the arena, and every step's log and `$stats` must agree.
- `test/timers.test.js` guards the timer calls in the sources, tests and
  scripts: each says why it is one (a `timer: <kind>` comment), and no sleep
  helper stands in for the event that reports work done.
- `test/exports.test.js` locks the public contract: every entry's export
  names and kinds must equal `test/exports.lock.json`.
- `test/types/` is a TypeScript consumer compiled with `tsc --noEmit`, so
  `$read($signal(root, 1))` is checked as `number`.
- The `.d.ts` files in `types/` are generated from the JSDoc by
  `scripts/types.mjs`; there is no handwritten declaration.
- The component layer's suites (components, lists, events, templates, roots,
  worker mode) live in ren-core, which runs them over this package, and the
  renderers' suites in their packages (ren-dom, ren-canv, ren-skia,
  ren-three, ren-html, ren-json).

## Benchmarks

The reactivity and disposal harnesses, the scenario definitions with their
checksums, the pinned competitors and the recorded results live in
[ren-bench](https://github.com/wiltonlazary/ren-bench/blob/main/bench/reactivity/README.md).
Run benchmark commands from that checkout. Open performance work for
rrx-core is in [TODO.md](TODO.md).

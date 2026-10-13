// rrx-core core: the reactive graph algorithm over fixed-shape records.
//
// Every node is one `Node` record and every dependency edge one `Link`
// record, each with a single hidden class, so V8 reads a field with one load.
// arena.js runs the same algorithm over integer columns with the same
// operations; the README records the layout choice and its measured cost.
//
// The algorithm: integer node states (clean, pending,
// dirty), a lane per queued effect, liveness, versions on nodes and `seen`
// versions on links, dependency and subscriber lists linked both ways, a
// `cycle` per tracking run for duplicate reads, watcher counts for
// attachment, iterative marking and checking with explicit stacks, effect
// queues sorted by creation order, and the flush phases.

import { $context, Provided, Layer, checkId, nearest } from './context.js';
import { DEV } from '#mode';

/**
 * @template T
 * @typedef {import('./context.js').Context<T>} Context
 */

export { $context, Provided };

// ---------------------------------------------------------------------------
// Constants
//
// They are module-local on purpose. V8 keeps every exported binding in a
// cell, so optimized code reloads an exported constant and checks it for the
// uninitialized hole at each use; a module-local const folds into the code as
// an immediate. Nothing outside this module needs them.

/** Node kinds. SCOPE is this library's: scopes join the owner lists. */
const SIGNAL = 1;
const MEMO = 2;
const EFFECT = 3;
const PROBE = 4;
const SCOPE = 5;

/** Node states. */
const CLEAN = 0;
const PENDING = 1;
const DIRTY = 2;

/** Queue lanes (an effect's `mode`). */
const URGENT = 1;
const LOW = 2;
/** A signal's `mode` when it may be written during a pure computation (Writable). */
const PERMITTED = -1;

/**
 * Readiness of a memo's result: none yet, a value, a failure, or a pending
 * read, so a clean read tests one integer.
 */
const NONE = 0;
const VALUE = 1;
const FAILURE = 2;
const WAITS = 3;

/** Liveness: a closing node is freed when its running computation returns. */
const FREED = 0;
const CLOSING = 1;
const ALIVE = 2;
/** A node whose computation is on the stack; a closing node is running too, so running is an odd liveness. */
const RUNNING = 3;
/** A scope made inert by `$retire`, waiting for a `$sweep` to free it. */
const RETIRED = 4;

/** `$sweep`: the clock is read every chunk of this many milliseconds of estimated work. */
const GRAIN = 0.05;
/** The most units between two clock reads. */
const CHUNK = 4096;
/** The default drain's slices: at most IDLE ms of an idle period, BUSY ms after waiting WAIT.timeout ms for one. */
const IDLE = 8;
const BUSY = 2;
const WAIT = { timeout: 100 };


/** An effect running more often than this within one flush is a cycle. */
const LIMIT = 100;
/**
 * `$flush` serials step by this much (more than LIMIT), so one field holds a
 * serial and a run count; doubles stay exact for 2^46 flushes.
 */
const STRIDE = 128;
/** `$commit` hooks that keep the graph busy for this many rounds are a cycle. */
const ROUNDS = 100;

/** Values `equals` may hold. @typedef {((previous: any, next: any) => boolean) | false | null | undefined} Equality */

// ---------------------------------------------------------------------------
// Pending reads. A pending read records the state it waits on here, then
// throws the WAITING sentinel, which the boundaries turn back into data.

export class Waiting {
  toString() {
    return 'RRX: pending read';
  }
}
/** The one pending-read sentinel. */
export const WAITING = new Waiting();
/** @type {any} */
let waited = null;
/** Records the state a read waits on and stops the computation. @param {any} state @returns {never} */
export function wait(state) {
  waited = state;
  throw WAITING;
}
/**
 * Takes the state the latest pending read waited on. The slot is a register
 * between one `wait` and the catch that consumes it (the same synchronous
 * throw), and is cleared on taking, so no pending state outlives its read or
 * reaches another root.
 */
export function waitedOn() {
  const state = waited;
  waited = null;
  return state;
}

/**
 * The root whose code is on the stack: set by `$enter`, `$with` and every effect
 * run, and restored after, so `$lookup(Theme)` needs no root argument. Like
 * `waited` it is a register, never state: it names a root only while that
 * root's code runs, so nested roots cannot see each other's contexts.
 * @type {Graph | null}
 */
let running = null;

// ---------------------------------------------------------------------------
// Development mode: diagnostics for the mistakes the model makes easy (a
// reactive read where nothing tracks it). On under the `development` export
// condition, or with $development; checked only off the hot paths.

/** Diagnostics are on. */
let dev = DEV;
/** Where diagnostics go. @type {(message: string) => void} */
let report = message => console.warn(message);

/**
 * Turns development diagnostics on or off at run time, and optionally sends
 * them to `sink` instead of `console.warn` (null restores it).
 * @param {boolean} on @param {((message: string) => void) | null} [sink]
 */
export function $development(on, sink) {
  dev = on;
  if (sink !== undefined) report = sink ?? (message => console.warn(message));
}

/** True while development diagnostics are on (for sibling modules). */
export function development() {
  return dev;
}

/** Reports a diagnostic (for sibling modules). @param {string} message */
export function diagnose(message) {
  report(message);
}

/**
 * Runs `fn` untracked as far as the graph goes, counting the reactive reads
 * it makes where nothing tracks them (for sibling modules, in development:
 * a component body, an application).
 * @template T @param {Graph} R @param {() => T} fn @returns {[T, number]}
 */
export function counted(R, fn) {
  const probe = new Node(PROBE, R, undefined, null);
  const current = R.current;
  const outer = counting;
  const cycle = R.cycle;
  R.cycle = ++R.cycles;
  R.current = probe;
  counting = probe;
  let reads = 0;
  try {
    const value = fn();
    for (let l = probe.deps; l !== null; l = l.nextDep) reads++;
    return [value, reads];
  } finally {
    R.current = current;
    counting = outer;
    R.cycle = cycle;
    free(R, probe);
  }
}

/** The probe `counted` is running under, whose own reads are counted (null: none). @type {Node<any> | null} */
let counting = null;

// One rrx-core per page: two copies make two graphs that cannot see each
// other's reads.
if (DEV) {
  const key = Symbol.for('rrx-core');
  const any = /** @type {any} */ (globalThis);
  if (any[key] === true) report('RRX dev: rrx-core is loaded twice; each copy has its own graphs, and reads across them are not tracked. Install one copy.');
  any[key] = true;
}

// ---------------------------------------------------------------------------
// Records

/**
 * A node of the graph and its public handle: `{ kind, root, id, gen, last }`
 * leads the record. A freed node keeps its final value and reads it
 * untracked, as a collected node would behave in a GC design. Signals, memos
 * and effects share this one class, so every hot site sees one hidden class;
 * the payloads few nodes set live in a lazily allocated Extra record.
 * Records are never reused: `id` is also the creation order.
 * @template [T=unknown]
 */
export class Node {
  /**
   * The constructor does everything that makes a node, so that Signal, Memo
   * and Effect stay small enough for V8's baseline compilers to inline:
   * it refuses a disposed root, derives the owner and the first state from
   * the kind, and joins the end of the owner's list, as adopt does.
   * @param {number} k @param {Graph} root @param {any} value @param {any} run
   */
  constructor(k, root, value, run) {
    if (root.closed) disposed();
    // A probe (`$touch`) belongs to the root; signals and probes start clean.
    const owner = k === PROBE ? root.home : root.owner;
    const last = owner.tail;
    this.root = root;
    this.id = ++root.nextId;
    this.k = k;
    this.state = k === SIGNAL || k === PROBE ? CLEAN : DIRTY;
    /** @type {number} */ this.live = ALIVE;

    /**
     * A source's version. An effect is never a source, so its version holds
     * the flush serial plus its runs within that flush (the cycle guard).
     */
    this.version = 0;
    /** @type {Link | null} */ this.deps = null;
    /** @type {Link | null} */ this.depsTail = null;
    /** @type {Link | null} */ this.subs = null;
    /** @type {Link | null} */ this.subsTail = null;
    /**
     * A source's attached observers. An effect is never a source: there it
     * holds the run (its version then) that last read a task's status
     * (statusRead).
     */
    this.watchers = 0;
    /** @type {ScopeNode} */ this.owner = owner;
    /** @type {Node<any> | ScopeNode | null} */ this.prev = last;
    /** @type {Node<any> | ScopeNode | null} */ this.next = null;
    /** @type {any} */ this.value = value;
    /** @type {any} */ this.run = run;
    /** Rarely set payloads, allocated on first use. @type {Extra | null} */ this.extra = null;
    /**
     * One field, a meaning per kind: a memo's readiness, an effect's queue
     * lane, a signal's write permit (PERMITTED). No kind reads another's.
     */
    this.mode = 0;
    /** @type {any} */ this.cleanup = undefined;
    /** @type {Application | null} */ this.apply = null;
    if (last !== null) last.next = this;
    else owner.head = this;
    owner.tail = this;
  }
  /** The public kind, derived from `k` so records carry one field less. @returns {'signal' | 'memo' | 'effect' | 'probe'} */
  get kind() {
    const k = this.k;
    return k === SIGNAL ? 'signal' : k === MEMO ? 'memo' : k === EFFECT ? 'effect' : 'probe';
  }
  /** The generation of the handle; records are never reused, so always 0. */
  get gen() {
    return 0;
  }
  /** The final value of a freed node, which reads it untracked. */
  get last() {
    return this.value;
  }
  set last(value) {
    this.value = value;
  }
}

/**
 * The payloads few nodes set: a custom equality, an whenUnwatched hook, the
 * write permit and `$derive`'s writability, and a memo's failure (or the state
 * a pending read waits on). Keeping them out of Node makes every record
 * smaller, which is most of the cost of creating one.
 */
class Extra {
  constructor() {
    /** @type {Equality} */ this.equals = undefined;
    /** @type {(() => void) | null | undefined} */ this.hook = undefined;
    this.permit = 0;
    this.writable = 0;
    /** @type {any} */ this.failure = undefined;
    /** An async memo's launch generation: a completion counts only for the latest. */
    this.ticket = 0;
    /** An async memo's work in flight: NONE, LOUD (it reads pending) or QUIET (a rerun keeping its value). */
    this.flight = 0;
    /** The next async run is a rerun: it keeps the value shown ($rerun). */
    this.quiet = 0;
    /** An async memo's running iterator, stopped when a newer run supersedes it. @type {any} */
    this.iterator = null;
    /** Change marks on the memo: while any holds, it reads pending, keeping its value. */
    this.changes = 0;
    /** Those of them that end at the memo's next value. */
    this.nexts = 0;
  }
}

/** A node's rare payloads, allocated on first use. @param {Node<any>} x */
function extra(x) {
  let e = x.extra;
  if (e === null) e = x.extra = new Extra();
  return e;
}

/**
 * The application side of a Watch or Render. A renderer may extend it (see
 * Bind) to keep a binding's state in fields instead of closures.
 */
export class Application {
  /** @param {(value: any) => unknown} run @param {boolean} render */
  constructor(run, render) {
    this.run = run;
    this.render = render;
    /** @type {any} */ this.shown = undefined;
    this.applied = 0;
    this.applying = 0;
    /** The transition this Watch or Render is suspended in: its read waits while its value is shown. @type {Transition | null} */
    this.hold = null;
    /** The transition its queued application is parked in, applied when that one commits. @type {Transition | null} */
    this.parked = null;
    /** The run of its observer that last read a status or a latest value (an indicator, never held). */
    this.peek = 0;
    /** The run of its observer that last suspended: a run that suspends stays suspended when it returns. */
    this.stay = 0;
  }
}

/**
 * A transition (a held async update): the applications of the
 * updates that made shown values pending wait here, so the screen never
 * mixes the new state with the old, and commit together once nothing in it
 * waits. `waits` counts its suspended Watch and Render observers; `parked`
 * holds the observers whose applications wait. A transition merged into
 * another forwards to it (`into`).
 */
class Transition {
  constructor() {
    this.waits = 0;
    /** @type {Node<any>[]} */
    this.parked = [];
    /** @type {Transition | null} */
    this.into = null;
  }
}

/** The transition a merged one forwards to. @param {Transition} t */
function target(t) {
  while (t.into !== null) t = t.into;
  return t;
}

/** A dependency edge, in its subscriber's dependency list and its source's subscriber list. */
class Link {
  /**
   * @param {Node<any>} dep @param {Node<any>} sub @param {Link | null} prevDep @param {Link | null} nextDep
   * @param {Link | null} prevSub @param {number} seen @param {number} cycle
   */
  constructor(dep, sub, prevDep, nextDep, prevSub, seen, cycle) {
    this.dep = dep;
    this.sub = sub;
    this.prevDep = prevDep;
    this.nextDep = nextDep;
    this.prevSub = prevSub;
    /** @type {Link | null} */ this.nextSub = null;
    this.seen = seen;
    this.cycle = cycle;
  }
}

/**
 * An owner: it owns the nodes, scopes and cleanups created inside it. The
 * root's own scope is `root.home`.
 */
export class ScopeNode {
  /** @param {Graph} root @param {ScopeNode | null} owner @param {number} id @param {any} context */
  constructor(root, owner, id, context) {
    this.root = root;
    this.id = id;
    this.k = SCOPE;
    this.live = ALIVE;
    this.owner = /** @type {ScopeNode} */ (owner);
    /** @type {Node<any> | ScopeNode | null} */ this.prev = null;
    /** @type {Node<any> | ScopeNode | null} */ this.next = null;
    /** @type {Node<any> | ScopeNode | null} */ this.head = null;
    /** @type {Node<any> | ScopeNode | null} */ this.tail = null;
    this.context = context;
    /** @type {((() => void) | null)[] | null} */ this.cleanup = null;
    /** A `$catch` handler and `$mount` callbacks, which few scopes have, made on first use. @type {Rare | null} */ this.rare = null;
    /** The root's retire epoch when this scope and its ancestors were last seen live (see inert). */
    this.seen = -1;
  }
  /** The public kind; a getter, so a scope record carries two fields less. @returns {'scope'} */
  get kind() {
    return 'scope';
  }
  /** The generation of the handle; records are never reused, so always 0. */
  get gen() {
    return 0;
  }
}

/** A scope's rare payloads: its `$catch` handler and pending `$mount` callbacks. */
class Rare {
  constructor() {
    /** @type {((error: any) => void) | null} */ this.handler = null;
    /** @type {(() => void)[] | null} */ this.mounts = null;
  }
}

/** A scope's rare payloads, made on first use. @param {ScopeNode} scope */
function rare(scope) {
  let r = scope.rare;
  if (r === null) r = scope.rare = new Rare();
  return r;
}

/** A queue or stack of nodes with a separate count; slots are reused. */
class Queue {
  constructor() {
    /** @type {Node<any>[]} */
    this.a = [];
    this.n = 0;
  }
}

// ---------------------------------------------------------------------------
// The root

/** A root's graph; `$root(id)` returns it. */
export class Graph {
  /** @param {string} id */
  constructor(id) {
    /** @type {'root'} */
    this.kind = 'root';
    /** The root's id: the key everything above the graph derives from (storage, locks, element ids). */
    this.id = id;
    this.nextId = 0;
    /** @type {Node<any> | null} */ this.current = null;
    this.cycle = 0;
    this.cycles = 0;
    this.nextOrder = 0;
    this.versionAll = 0;
    this.pure = false;
    this.auto = false;
    this.closed = false;
    this.closing = false;
    this.flushing = false;
    this.batching = 0;
    this.serialAll = 0;
    this.transition = 0;
    /** The memos `hasten` walked since the low queue last grew (their deferred effects are already urgent). @type {Set<Node<any>>} */
    this.hastened = new Set();
    this.lane = URGENT;
    this.loading = 0;
    /** The transition the current flush belongs to (it made a shown value pending, or touched one's work). @type {Transition | null} */
    this.flow = null;
    // Work stacks, reused: check frames (the link a check descended
    // through), marking, watchers, and whenUnwatched hooks waiting for a safe
    // point.
    /** @type {Link[]} */ this.sLink = [];
    this.sp = 0;
    this.mark = new Queue();
    this.watch = new Queue();
    /** whenUnwatched hooks, and memos whose unwatched hook waits (see unwatchSources). @type {((() => void) | Node<any>)[]} */ this.hooks = [];
    // Queues
    this.urgent = new Queue();
    this.low = new Queue();
    /** Eager memos (task states) to refresh, per lane, ahead of that lane's effects. */
    this.eagerUrgent = new Queue();
    this.eagerLow = new Queue();
    this.spare = new Queue();
    this.renders = new Queue();
    this.applies = new Queue();
    this.spareApply = new Queue();
    /** @type {(() => void)[]} */ this.after = [];
    /** @type {(() => void)[]} */ this.settled = [];
    /** `$commit` hooks; one whose owner stopped has a null `run` until the list is compacted. @type {{ run: (() => void) | null, owner: ScopeNode }[]} */ this.commits = [];
    this.commitCount = 0;
    this.commitKeys = 0;
    /** The current context layer. @type {Layer | null} */ this.ctx = null;
    /** @type {((error: any) => void) | null} */ this.errorHandler = null;
    /** The first unhandled failure of a flush, raised when it ends; HANDLED when none. @type {any} */ this.failed = HANDLED;
    /** The root's own scope, owner of unowned nodes. */
    this.home = new ScopeNode(this, null, 0, null);
    // Background disposal (`$retire`, `$sweep`): the retire epoch, the queue of
    // retired scopes, the scope being swept and its cursor, the cost estimate
    // in milliseconds per unit, and the host's scheduler.
    this.retires = 0;
    /** True while retired scopes wait for a sweep: only then is inertness checked. */
    this.retiring = false;
    /** @type {(ScopeNode | null)[]} */ this.retired = [];
    this.retiredAt = 0;
    /** @type {ScopeNode | null} */ this.sweepTop = null;
    /** @type {ScopeNode | null} */ this.sweepCur = null;
    this.sweepCost = 0.0002;
    /** @type {((root: Graph) => void) | null | undefined} */ this.sweeper = undefined;
    /** The first unhandled failure of the current `$sweep` slice. @type {any} */ this.sweepFailure = null;
    /** The current owner. */
    this.owner = this.home;
  }
}

/**
 * Creates a root: one graph with its own ownership, queues, flush and
 * contexts. `id` (`[a-z][a-z0-9-]{0,31}`) names it; the component layer
 * derives storage keys, lock names and element ids from it. rrx-core does not
 * require ids to be unique; an application host does.
 * @param {string} id
 * @returns {Graph}
 */
export function $root(id) {
  return new Graph(checkId(id));
}

/** @param {Graph} R */
export function live(R) {
  if (R.closed) disposed();
}

/** @returns {never} */
function disposed() {
  throw new Error('RRX: disposed root');
}

/** @param {ScopeNode} owner @param {Node<any> | ScopeNode} x */
function adopt(owner, x) {
  const last = owner.tail;
  x.prev = last;
  x.next = null;
  if (last !== null) last.next = x;
  else owner.head = x;
  owner.tail = x;
}

/** @param {Node<any> | ScopeNode} x */
function leave(x) {
  const o = x.owner;
  const p = x.prev;
  const n = x.next;
  if (p !== null) p.next = n;
  else o.head = n;
  if (n !== null) n.prev = p;
  else o.tail = p;
  x.prev = null;
  x.next = null;
}

/** The node of a live handle, or null once freed. @param {Node<any>} handle */
export function node(handle) {
  live(handle.root);
  return handle.live === FREED ? null : handle;
}

// ---------------------------------------------------------------------------
// Links

/** @param {Link} l */
function removeDep(l) {
  const sub = l.sub;
  const before = l.prevDep;
  const after = l.nextDep;
  if (before === null) sub.deps = after;
  else before.nextDep = after;
  if (after !== null) after.prevDep = before;
  if (sub.depsTail === l) sub.depsTail = before;
}

/** An effect is always attached; a memo while something watches it. @param {Node<any>} x */
function attached(x) {
  const k = x.k;
  return k === EFFECT ? x.live !== FREED : k === MEMO ? x.watchers > 0 : false;
}

// Watchers. An attached observer that links a source adds a watcher to it
// (insert), and a memo gaining its first one watches its own sources. The
// last watcher leaving (purge) releases a memo's sources; a signal runs its
// hook once the lists are consistent again. Sources are released depth
// first, the latest read first, which fixes the order the hooks run in.

/** A memo gained its first watcher: its sources gain one each, in turn. @param {Graph} R @param {Node<any>} dep */
function watchSources(R, dep) {
  // Only memos that gain their first watcher go on the stack, each counted
  // as it is pushed; the counts alone matter, so the order is free.
  const a = R.watch.a;
  const base = R.watch.n;
  let n = base;
  let x = dep;
  for (;;) {
    for (let l = x.deps; l !== null; l = l.nextDep) {
      const d = l.dep;
      if (++d.watchers === 1 && d.k === MEMO) a[n++] = d;
    }
    if (n === base) return;
    x = a[--n];
  }
}

/** A signal lost its last watcher: its whenUnwatched hook waits for a safe point. @param {Graph} R @param {Node<any>} x */
function unhooked(R, x) {
  const e = x.extra;
  if (e !== null && e.hook != null) R.hooks.push(e.hook);
}

/**
 * A memo lost its last watcher: its sources lose one each, in turn. A memo
 * whose `cleanup` (unused by memos) holds a function has it run at the next
 * safe point, with the memo as `this` (for sibling modules: a task wakes
 * its eager observer when its last reader leaves).
 * @param {Graph} R @param {Node<any>} dep
 */
function unwatchSources(R, dep) {
  const w = R.watch;
  const a = w.a;
  const base = w.n;
  let n = base;
  if (dep.cleanup !== undefined) R.hooks.push(dep);
  for (let l = dep.deps; l !== null; l = l.nextDep) a[n++] = l.dep;
  while (n > base) {
    const x = a[--n];
    if (--x.watchers === 0) {
      if (x.k === MEMO) {
        if (x.cleanup !== undefined) R.hooks.push(x);
        for (let l = x.deps; l !== null; l = l.nextDep) a[n++] = l.dep;
      } else if (x.k === SIGNAL) unhooked(R, x);
    }
  }
}

/** @param {Graph} R */
function runHooks(R) {
  const hooks = R.hooks;
  while (hooks.length > 0) {
    const hook = /** @type {(() => void) | Node<any>} */ (hooks.pop());
    if (typeof hook === 'function') hook();
    else hook.cleanup.call(hook);
  }
}

/** Removes the links after the dependency cursor: sources the run stopped reading. @param {Graph} R @param {Node<any>} sub */
function purge(R, sub) {
  const tail = sub.depsTail;
  let l = tail === null ? sub.deps : tail.nextDep;
  if (l === null) return;
  if (tail === null) sub.deps = null;
  else tail.nextDep = null;
  const watched = attached(sub);
  while (l !== null) {
    /** @type {Link | null} */
    const after = l.nextDep;
    // removeSub and unwatch, inline.
    const dep = l.dep;
    const before = l.prevSub;
    const next = l.nextSub;
    if (before === null) dep.subs = next;
    else before.nextSub = next;
    if (next === null) dep.subsTail = before;
    else next.prevSub = before;
    if (watched && --dep.watchers === 0) {
      if (dep.k === MEMO) unwatchSources(R, dep);
      else if (dep.k === SIGNAL) unhooked(R, dep);
    }
    l = after;
  }
  if (R.hooks.length > 0) runHooks(R);
}

/**
 * Tracking: a rerun that reads the same sources in order reuses its links.
 * @param {Graph} R @param {Node<any>} dep @param {Node<any>} sub
 */
function link(R, dep, sub) {
  const tail = sub.depsTail;
  if (tail !== null && tail.dep === dep) return;
  const after = tail === null ? sub.deps : tail.nextDep;
  if (after !== null && after.dep === dep) {
    sub.depsTail = after;
    after.seen = dep.version;
    after.cycle = R.cycle;
    return;
  }
  insert(R, dep, sub, tail, after);
}

/**
 * True when a source that `x`'s running computation already read changed
 * since (a write made inside the run): its result may be stale. A memo
 * source not yet recomputed counts as changed. Reads only this run's links.
 * @param {Node<any>} x
 */
export function moved(x) {
  const tail = x.depsTail;
  const end = tail === null ? x.deps : tail.nextDep;
  if (tail === null) return false;
  for (let l = x.deps; l !== end && l !== null; l = l.nextDep) {
    const d = l.dep;
    if (d.version !== l.seen || (d.k === MEMO && d.state !== CLEAN)) return true;
  }
  return false;
}

/**
 * Inserts a new link after the cursor, unless the source already has this
 * observer's link from this cycle (a repeated read out of order).
 * @param {Graph} R @param {Node<any>} dep @param {Node<any>} sub @param {Link | null} tail @param {Link | null} after
 */
function insert(R, dep, sub, tail, after) {
  const last = dep.subsTail;
  if (last !== null && last.sub === sub && last.cycle === R.cycle) return;
  const made = new Link(dep, sub, tail, after, last, dep.version, R.cycle);
  if (tail === null) sub.deps = made;
  else tail.nextDep = made;
  if (after !== null) after.prevDep = made;
  sub.depsTail = made;
  if (last === null) dep.subs = made;
  else last.nextSub = made;
  dep.subsTail = made;
  // An attached observer (a live effect, or a memo with watchers) watches its new source.
  const k = sub.k;
  if (k === EFFECT ? sub.live !== FREED : k === MEMO && sub.watchers > 0) {
    if (++dep.watchers === 1 && dep.k === MEMO) watchSources(R, dep);
  }
}

// ---------------------------------------------------------------------------
// Marking

/**
 * Queues an effect in the lane of the running change: urgent, unless the
 * change is deferred. An urgent change that reaches an effect already queued
 * deferred moves it to the urgent lane (its entry in the low queue is then
 * skipped); a deferred one leaves a queued effect where it is.
 * @param {Graph} R @param {Node<any>} x
 */
function enqueue(R, x) {
  const lane = x.mode;
  // Work the low lane runs (its effects and applications) writes deferred work too.
  if (R.transition === 0 && R.lane !== LOW) {
    if (lane === 0 || lane === LOW) {
      x.mode = URGENT;
      const q = R.urgent;
      q.a[q.n++] = x;
    }
  } else if (lane === 0) {
    x.mode = LOW;
    const q = R.low;
    q.a[q.n++] = x;
    // A new deferred effect: a region walked before may reach it now.
    if (R.hastened.size > 0) R.hastened.clear();
  }
}

/**
 * Queues an eager memo (a memo with an application: a task's state) that a
 * change reached: the flush refreshes it ahead of its lane's effects, so it
 * recomputes with nothing reading it. Their own queue keeps the effect queue
 * in creation order (no sort for the memos). It is queued once per change, when it
 * leaves CLEAN, so it needs no lane field; a refresh that finds it clean
 * again (a read pulled it first) does nothing.
 * @param {Graph} R @param {Node<any>} x
 * @returns {boolean} whether its readers still need pending propagation
 */
function eager(R, x) {
  const q = R.transition === 0 && R.lane !== LOW ? R.eagerUrgent : R.eagerLow;
  q.a[q.n++] = x;
  // Eager memos notify effects only after their state changes. With no
  // reader, or one effect reader, a second walk would do nothing. Multiple
  // readers keep the general walk, which also finds lazy memo readers.
  const first = x.subs;
  return first !== null && (first.nextSub !== null || first.sub.k !== EFFECT);
}

/**
 * An urgent change met `m`, a memo that is not clean, so it passes nothing
 * on by itself: the effects queued deferred below it (through its non-clean
 * readers) move to the urgent lane. Only while an urgent change runs and the
 * low queue holds something. The root keeps what it walked until the low
 * queue grows or the flush ends, so each region is walked once, not once
 * per urgent write.
 * @param {Graph} R @param {Node<any>} m
 */
function hasten(R, m) {
  if (R.low.n === 0 || R.transition !== 0 || R.lane === LOW) return;
  const seen = R.hastened;
  if (seen.has(m)) return;
  seen.add(m);
  const stack = [m];
  while (stack.length > 0) {
    const cur = /** @type {Node<any>} */ (stack.pop());
    for (let l = cur.subs; l !== null; l = l.nextSub) {
      const x = l.sub;
      if (x.state === CLEAN || seen.has(x)) continue;
      seen.add(x);
      if (x.k === EFFECT) {
        if (x.mode === LOW) enqueue(R, x);
      } else if (x.k === MEMO) stack.push(x);
    }
  }
}

/**
 * Raises a node to `level`; a node that was clean passes the change on: a
 * memo marks its subscribers pending, an effect is queued.
 * @param {Graph} R @param {Node<any>} x @param {number} level
 */
function mark(R, x, level) {
  if (x.live === FREED) return;
  const was = x.state;
  if (was < level) x.state = level;
  if (was === CLEAN) {
    if (x.k === MEMO) {
      if (x.apply === null || eager(R, x)) R.mark.a[R.mark.n++] = x;
    } else if (x.k === EFFECT) enqueue(R, x);
  } else if (x.k === EFFECT) {
    if (x.mode === LOW) enqueue(R, x);
  } else if (x.k === MEMO) hasten(R, x);
}

/**
 * Marks the subscribers of every memo on the mark list pending, in the order
 * the memos were reached (the list grows while it is walked), so effects
 * queue in subscriber order. The first memo a node reaches waits in a local
 * rather than the list: when nothing else is waiting it is simply walked
 * next, so a chain of memos marks without touching the list, in the same
 * order. @param {Graph} R
 */
function propagatePending(R) {
  const m = R.mark;
  const a = m.a;
  // The count stays in a local while the walk runs; nothing else touches it.
  let n = m.n;
  let i = 0;
  if (n === 0) return;
  let cur = a[i++];
  for (;;) {
    /** @type {Node<any> | null} */
    let next = null;
    for (let l = cur.subs; l !== null; l = l.nextSub) {
      const x = l.sub;
      const k = x.k;
      if (x.state !== CLEAN) {
        // Already queued deferred, or behind a memo already reached: an urgent change moves it up.
        if (k === EFFECT) {
          if (x.mode === LOW && cur.apply === null) enqueue(R, x);
        } else if (k === MEMO) hasten(R, x);
        continue;
      }
      // An eager memo (a task's state) marks only its memo readers: it
      // recomputes before its lane's effects and queues its effect readers
      // itself when its value changes (changed), so a relaunch that leaves
      // it equal queues none.
      if (k === EFFECT && cur.apply !== null) continue;
      x.state = PENDING;
      if (k === MEMO) {
        if (x.apply !== null && !eager(R, x)) continue;
        if (next === null) next = x;
        else {
          a[n++] = next;
          a[n++] = x;
          next = null;
        }
      } else if (k === EFFECT) enqueue(R, x);
    }
    if (next !== null) {
      if (i === n) {
        cur = next;
        continue;
      }
      a[n++] = next;
    }
    if (i === n) break;
    cur = a[i++];
  }
  // The walked memos leave the list, so it keeps no freed node alive.
  for (let j = 0; j < n; j++) a[j] = /** @type {any} */ (null);
  m.n = 0;
}

/**
 * Direct subscribers of a changed node become dirty, the rest pending. Every
 * caller writes the source, so a running effect is marked too, which is how
 * a self-triggering effect is found.
 * @param {Graph} R @param {Node<any>} source
 */
function propagate(R, source) {
  const m = R.mark;
  for (let l = source.subs; l !== null; l = l.nextSub) {
    const x = l.sub;
    const was = x.state;
    // Already queued deferred, or behind a memo already reached: an urgent change moves it up.
    if (was !== CLEAN) {
      if (x.k === EFFECT) {
        if (x.mode === LOW) enqueue(R, x);
      } else if (x.k === MEMO) hasten(R, x);
    }
    if (was === DIRTY) continue;
    x.state = DIRTY;
    if (was === CLEAN) {
      if (x.k === MEMO) {
        if (x.apply === null || eager(R, x)) m.a[m.n++] = x;
      } else if (x.k === EFFECT) enqueue(R, x);
    }
  }
  if (m.n > 0) propagatePending(R);
}

// ---------------------------------------------------------------------------
// Memos

/** A custom equality: false notifies every change. @param {Equality} equals @param {any} previous @param {any} next */
function same(equals, previous, next) {
  return typeof equals === 'function' && equals(previous, next);
}

/**
 * Records a result that is not a second value: the first value, a failure,
 * or a pending read. Kept out of compute so its hot path stays small.
 * @param {Node<any>} x @param {boolean} ok @param {any} value
 */
function settle(x, ok, value) {
  if (ok) {
    if (x.mode !== NONE) extra(x).failure = undefined;
    x.mode = VALUE;
    x.value = value;
  } else if (value === WAITING) {
    x.mode = WAITS;
    extra(x).failure = waitedOn();
  } else {
    x.mode = FAILURE;
    extra(x).failure = value;
  }
}

// ---------------------------------------------------------------------------
// Async memos. A memo whose computation returns a promise or an async
// iterator reads pending (keeping its last value for $lastReady) until the work
// completes, then holds the result; a newer run supersedes the work in
// flight (its completion is ignored, an iterator is stopped). rrx-core makes
// no promise of its own: it only subscribes to the one the computation
// returned. The root counts work in flight (`loading`), as for tasks, so
// `$settled` waits for it.

/** An async memo's work in flight: it reads pending, or (a rerun) keeps its value. */
const LOUD = 1;
const QUIET = 2;

/** A promise-like or an async iterator: the result of an async computation. @param {any} value */
function awaited(value) {
  return typeof value.then === 'function' || typeof value[Symbol.asyncIterator] === 'function';
}

/** Stops the work of an async memo that a newer run, or its end, supersedes. @param {Graph} R @param {Node<any>} x */
function superseded(R, x) {
  const e = /** @type {Extra} */ (x.extra);
  e.ticket++;
  if (e.flight === LOUD) R.loading--;
  e.flight = NONE;
  const it = e.iterator;
  e.iterator = null;
  if (it !== null && typeof it.return === 'function') {
    try {
      const done = it.return();
      if (done !== null && typeof done === 'object' && typeof done.then === 'function') done.then(undefined, ignore);
    } catch {
      // An iterator that fails to stop is already superseded.
    }
  }
}

function ignore() {}

/**
 * Starts following an async result: the memo reads pending, keeping its last
 * value, unless the run is a quiet rerun of a memo that has one ($rerun);
 * a promise completes it once, an iterator with each value.
 * @param {Graph} R @param {Node<any>} x @param {any} value
 */
function launched(R, x, value) {
  const e = extra(x);
  const ticket = ++e.ticket;
  const quiet = e.quiet === 1 && x.mode === VALUE;
  e.quiet = 0;
  // Work that reads pending counts as loading (`$settled` waits for it); a quiet rerun does not.
  if (!quiet) R.loading++;
  e.flight = quiet ? QUIET : LOUD;
  if (!quiet) {
    const was = x.mode;
    const held = was === WAITS && e.failure != null;
    const kept = was === VALUE ? x.value : held ? e.failure.value : null;
    // `$behind`: the kept value has been ready once (a pending state it waited on says so too).
    const behind = was === VALUE || (held && e.failure.behind === true);
    x.mode = WAITS;
    e.failure = { status: 'pending', value: kept, error: null, behind };
    x.version++;
  }
  if (typeof value.then === 'function') {
    /** @type {any[] | null} */
    let early = null;
    let inside = true;
    value.then(
      (/** @type {any} */ result) => {
        if (inside) early = [true, result];
        else arrived(R, x, ticket, true, result, true);
      },
      (/** @type {any} */ error) => {
        if (inside) early = [false, error];
        else arrived(R, x, ticket, false, error, true);
      },
    );
    inside = false;
    // A thenable that completed synchronously is the computation's result.
    if (early !== null) immediate(R, x, early[0], early[1]);
  } else pump(R, x, ticket, value[Symbol.asyncIterator]());
}

/** A thenable that completed during the computation: its result is the memo's. @param {Graph} R @param {Node<any>} x @param {boolean} ok @param {any} value */
function immediate(R, x, ok, value) {
  const e = /** @type {Extra} */ (x.extra);
  if (e.flight === LOUD) R.loading--;
  e.flight = NONE;
  if (ok) {
    x.mode = VALUE;
    e.failure = undefined;
    x.value = value;
  } else {
    x.mode = FAILURE;
    e.failure = value;
  }
  x.version++;
}

/**
 * Follows an async iterator: each value is the memo's until the next; the
 * end keeps the last one (or takes a returned value).
 * @param {Graph} R @param {Node<any>} x @param {number} ticket @param {any} it
 */
function pump(R, x, ticket, it) {
  const e = /** @type {Extra} */ (x.extra);
  e.iterator = it;
  const step = () => {
    let next;
    try {
      next = it.next();
    } catch (error) {
      arrived(R, x, ticket, false, error, true);
      return;
    }
    next.then(
      (/** @type {IteratorResult<any>} */ r) => {
        if (e.ticket !== ticket) return;
        if (r.done) {
          if (r.value !== undefined || x.mode !== VALUE) arrived(R, x, ticket, true, r.value, true);
          else arrived(R, x, ticket, true, x.value, true);
        } else if (arrived(R, x, ticket, true, r.value, false)) step();
      },
      (/** @type {any} */ error) => {
        arrived(R, x, ticket, false, error, true);
      },
    );
  };
  step();
}

/**
 * A completion of an async memo's work `ticket`. Superseded work, a freed
 * memo and a closed root ignore it. A memo whose sources changed since the
 * launch brings itself up to date first, which supersedes this work. The
 * result is published as a write is: readers become dirty, and `$auto` flushes.
 * Returns false when ignored.
 * @param {Graph} R @param {Node<any>} x @param {number} ticket @param {boolean} ok @param {any} value @param {boolean} done
 */
function arrived(R, x, ticket, ok, value, done) {
  const e = /** @type {Extra} */ (x.extra);
  if (R.closed || x.live === FREED || e.ticket !== ticket) return false;
  if (x.state !== CLEAN) {
    refresh(R, x);
    if (e.ticket !== ticket) return false;
  }
  if (done) {
    if (e.flight === LOUD) R.loading--;
    e.flight = NONE;
    e.iterator = null;
  } else if (e.flight === LOUD) {
    // A streamed value: the memo is ready, and its stream quiet from here on.
    R.loading--;
    e.flight = QUIET;
  }
  const was = x.mode;
  const previous = x.value;
  let moved = true;
  if (ok && e.changes > 0) ended(e);
  if (ok && e.changes > 0) {
    // Still changing: the new value is the latest, read as pending.
    x.value = value;
    e.failure = { status: 'pending', value, error: null, behind: true };
  } else if (ok) {
    x.mode = VALUE;
    e.failure = undefined;
    if (was === VALUE && (e.equals == null ? Object.is(previous, value) : same(e.equals, previous, value))) moved = false;
    else x.value = value;
  } else {
    x.mode = FAILURE;
    e.failure = value;
  }
  if (moved) {
    x.version++;
    if (x.subs !== null) propagate(R, x);
  }
  if (R.auto) automatic(R);
  return true;
}

/**
 * The flight of an async memo's work: NONE, LOUD or QUIET (for sibling
 * modules: $pending and $behind read it).
 * @param {Node<any>} x
 */
export function flight(x) {
  return x.k === MEMO && x.extra !== null ? x.extra.flight : NONE;
}

/** A new value ends the change marks waiting for one. @param {Extra} e */
function ended(e) {
  e.changes -= e.nexts;
  e.nexts = 0;
}

/**
 * Marks a memo changing (for sibling modules: $changing): it reads pending, keeping
 * its value, until `endChange`, or (`next`) its next value.
 * @param {Node<any>} x @param {boolean} next
 */
export function beginChange(x, next) {
  if (x.k !== MEMO || x.live === FREED) throw new TypeError('RRX: $changing expects a task or memo');
  const R = x.root;
  if (R.pure) impure();
  if (x.state !== CLEAN) refresh(R, x);
  const e = extra(x);
  e.changes++;
  if (next) e.nexts++;
  if (x.mode === VALUE) {
    x.mode = WAITS;
    e.failure = { status: 'pending', value: x.value, error: null, behind: true };
    x.version++;
    if (x.subs !== null) propagate(R, x);
    if (R.auto) automatic(R);
  }
}

/**
 * Ends a change mark on a memo: with none left and no work in flight, its value
 * reads ready again.
 * @param {Node<any>} x @param {boolean} next the mark was one ending at the next value
 */
export function endChange(x, next) {
  const e = x.extra;
  if (e === null || e.changes === 0 || x.live === FREED) return;
  if (next) {
    // Already ended by a value, or still waiting for one.
    if (e.nexts === 0) return;
    e.nexts--;
  }
  e.changes--;
  if (e.changes > 0 || e.flight === LOUD || x.mode !== WAITS) return;
  const R = x.root;
  // Work already settled, or quiet work: the value in hand reads ready (a quiet rerun still lands later).
  x.mode = VALUE;
  e.failure = undefined;
  x.version++;
  if (x.subs !== null) propagate(R, x);
  if (R.auto) automatic(R);
}

/** Marks a memo's next async run a quiet rerun and invalidates it (for sibling modules: $rerun). @param {Node<any>} x */
export function requery(x) {
  if (x.k !== MEMO || x.live === FREED) return;
  // Only a memo that has been async runs quietly; a plain memo just recomputes.
  if (x.extra !== null && x.extra.ticket > 0) x.extra.quiet = 1;
  $invalidate(x);
  // $rerun runs the work now, as a task's does, when something reads the memo.
  const R = x.root;
  if (x.watchers > 0 && R.current === null) {
    refresh(R, x);
    if (R.auto) automatic(R);
  }
}

/**
 * The rarer results of a computation: an object (a promise or an async
 * iterator makes an async memo: async lives in the graph), a
 * failure or a pending read, a custom equality. A newer run supersedes the
 * async work in flight.
 * @param {Graph} R @param {Node<any>} x @param {boolean} ok @param {any} value @param {any} previous
 */
function outcome(R, x, ok, value, previous) {
  const e = x.extra;
  if (e !== null && e.flight !== NONE) superseded(R, x);
  if (ok && typeof value === 'object' && value !== null && awaited(value)) {
    launched(R, x, value);
    return;
  }
  if (e !== null && e.changes > 0 && ok) {
    // A new value ends the change marks that wait for it; the others keep it pending.
    ended(e);
    if (e.changes > 0) {
      x.value = value;
      x.mode = WAITS;
      e.failure = { status: 'pending', value, error: null, behind: true };
      x.version++;
      return;
    }
  }
  const ready = x.mode;
  if (ok && ready === VALUE) {
    if (!(e === null || e.equals == null ? Object.is(previous, value) : same(e.equals, previous, value))) {
      x.value = value;
      x.version++;
    }
  } else {
    if (ok && ready === NONE) {
      x.mode = VALUE;
      x.value = value;
    } else settle(x, ok, value);
    x.version++;
  }
}

/** @param {Graph} R @param {Node<any>} x */
function compute(R, x) {
  if (R.retiring && kept(R, x)) return;
  x.live = RUNNING;
  const current = R.current;
  const pure = R.pure;
  // The run reads contexts as where it was made (its owner's), as an effect does.
  const ctx = R.ctx;
  R.ctx = x.owner.context;
  // Each run gets a fresh cycle; an outer run continues with the latest one,
  // which at worst adds a duplicate link.
  R.cycle = ++R.cycles;
  x.depsTail = null;
  R.current = x;
  R.pure = true;
  const previous = x.value;
  let value;
  let ok = true;
  // Everything thrown is caught, so the epilogue needs no finally.
  try {
    value = x.run(previous);
  } catch (error) {
    ok = false;
    value = error;
  }
  R.current = current;
  R.pure = pure;
  R.ctx = ctx;
  if (x.live === RUNNING) x.live = ALIVE;
  // The run moved the cursor; the cast undoes the narrowing to null above.
  const tail = /** @type {Link | null} */ (x.depsTail);
  if ((tail === null ? x.deps : tail.nextDep) !== null) purge(R, x);
  x.state = CLEAN;
  // Equality is asked only between two computed values. Readers are pending
  // already (a non-clean memo's readers are never clean), so they find a new
  // version when they check (README). The common result, a plain value of a
  // memo without rare payloads, is settled here; anything else (an object,
  // which may be async, a failure, a custom equality) in outcome.
  const ready = x.mode;
  if (ok && x.extra === null && (typeof value !== 'object' || value === null) && ready !== WAITS && ready !== FAILURE) {
    if (ready === NONE) {
      x.mode = VALUE;
      x.value = value;
      x.version++;
    } else if (!Object.is(previous, value)) {
      x.value = value;
      x.version++;
    }
  } else outcome(R, x, ok, value, previous);
  if (x.live === CLOSING) free(R, x);
}

/**
 * An eager memo's computation yields a changed value (for sibling modules:
 * a task's state): its effect readers, which propagatePending left clean,
 * become dirty and queue. One that is running is reading it now and gets
 * the new value.
 * @param {Graph} R @param {Node<any>} x @param {boolean} statusOnly the value is the same
 */
export function changed(R, x, statusOnly) {
  for (let l = x.subs; l !== null; l = l.nextSub) {
    const sub = l.sub;
    if (sub.k !== EFFECT || sub.live === RUNNING) continue;
    // Only the status moved: an effect whose latest run read no status (it
    // read the value alone, $lastReady) sees nothing new.
    if (statusOnly && sub.watchers !== sub.version) continue;
    if (sub.state !== CLEAN) {
      // Already queued deferred: an urgent change moves it up.
      if (sub.mode === LOW) enqueue(R, sub);
      continue;
    }
    sub.state = DIRTY;
    enqueue(R, sub);
  }
}

/**
 * Records that the running effect's current run reads a task's status (for
 * sibling modules): a change of status alone then reaches it (changed).
 * @param {Graph} R
 */
export function statusRead(R) {
  const x = R.current;
  if (x !== null && x.k === EFFECT) x.watchers = x.version;
}

/**
 * A memo of a retired scope keeps its last value, as a stopped one does, so
 * it does not recompute (unless it has none yet). @param {Graph} R @param {Node<any>} x
 */
function kept(R, x) {
  if (x.mode === NONE || !inert(R, x.owner)) return false;
  x.state = CLEAN;
  return true;
}

/**
 * Brings a pending or dirty node up to date without recursion: sources are
 * checked in read order, and a memo source is resolved before its reader
 * continues. Memos recompute here; an effect is only classified. The frame
 * being examined stays in locals; its parents wait on the stack as the link
 * they descended through: the link's `sub` is the parent and the link is its
 * cursor, so one slot holds both. A finished
 * child is compared with that link at once. Only the starting node can be
 * running (descents skip running memos), so the recursion check is made here
 * rather than in every compute.
 * @param {Graph} R @param {Node<any>} start
 */
function refresh(R, start) {
  const stack = R.sLink;
  const base = R.sp;
  let sp = base;
  let x = start;
  let cursor = x.deps;
  for (;;) {
    // A pending node walks its sources in read order until one moved.
    while (x.state === PENDING) {
      if (cursor === null) {
        x.state = CLEAN;
        break;
      }
      const dep = cursor.dep;
      if (dep.k === MEMO && dep.state !== CLEAN && dep.live === ALIVE) {
        if (dep.state === PENDING) {
          // Descend into the pending memo source.
          stack[sp++] = cursor;
          x = dep;
          cursor = dep.deps;
          continue;
        }
        // A dirty memo source recomputes at once, without a frame; its run
        // may have settled this reader (a freed source makes it dirty).
        R.sp = sp;
        compute(R, dep);
        if (x.state !== PENDING) break;
      }
      if (dep.version !== cursor.seen) x.state = DIRTY;
      else cursor = cursor.nextDep;
    }
    if (x.state === DIRTY && x.k === MEMO) {
      if (sp === base && (x.live & 1) !== 0) recursive();
      R.sp = sp;
      compute(R, x);
    }
    if (sp === base) break;
    const child = x;
    const up = stack[--sp];
    x = up.sub;
    // The parent resumes at the link it descended through, unless it was
    // settled meanwhile (a source it reads was freed and it became dirty).
    if (x.state === PENDING) {
      if (child.version !== up.seen) x.state = DIRTY;
      else cursor = up.nextDep;
    }
  }
  R.sp = base;
}

/** @returns {never} */
function recursive() {
  throw new Error('RRX: recursive memo');
}

/** @param {Graph} R @param {Node<any>} x */
function readMemo(R, x) {
  const state = x.state;
  // A dirty memo, such as one read for the first time, needs no check.
  if (state === DIRTY && x.live === ALIVE) compute(R, x);
  else if (state !== CLEAN) refresh(R, x);
  const observer = R.current;
  if (observer !== null) link(R, x, observer);
  return x.mode === VALUE ? x.value : unready(x);
}

/** A memo without a value raises its failure, or waits. @param {Node<any>} x @returns {never} */
function unready(x) {
  const failure = extra(x).failure;
  if (x.mode === WAITS) wait(failure);
  throw failure;
}

/**
 * Brings a Watch up to date now and applies a pending value at once, as a
 * read brings a memo up to date (for sibling modules: a task read pulls its
 * launcher, so a changed input launches before the next flush; a mirror
 * applies its first value inside a batch). The application may write (it
 * runs lifted, even inside a pure computation); the next flush finds the
 * Watch clean and applied, and does not run it again.
 * @param {Graph} R @param {Node<any>} x
 */
export function pull(R, x) {
  if (x.live !== ALIVE) return;
  const app = x.apply;
  if (x.state === CLEAN && (app === null || app.applying === 0)) return;
  const pure = R.pure;
  R.pure = false;
  try {
    if (x.state !== CLEAN) refreshEffect(R, x);
    if (app !== null && app.applying !== 0 && x.live === ALIVE) {
      app.applying = 0;
      commit(R, x);
    }
  } finally {
    R.pure = pure;
  }
}

/** Brings a memo up to date (for sibling modules). @param {Graph} R @param {Node<any>} x */
export function update(R, x) {
  if (x.state === DIRTY && x.live === ALIVE) compute(R, x);
  else if (x.state !== CLEAN) refresh(R, x);
}

/**
 * Stores a value in a memo as a local write, as `$write` does to a `$derive`:
 * pending source changes apply first, and the value holds until a source
 * changes (for sibling modules: a task publishes its state this way, without
 * the Extra record `$derive` needs). A freed memo only keeps the value.
 * @param {Graph} R @param {Node<any>} x @param {any} value @param {boolean} statusOnly the value kept, the status moved
 */
export function assign(R, x, value, statusOnly) {
  if (x.live === FREED) {
    x.value = value;
    return;
  }
  if (R.pure) impure();
  if (x.state !== CLEAN) refresh(R, x);
  if (x.mode !== VALUE) {
    x.mode = VALUE;
    if (x.extra !== null) x.extra.failure = undefined;
  }
  if (Object.is(x.value, value)) return;
  x.value = value;
  x.version++;
  if (x.subs !== null) published(R, x, statusOnly);
  if (R.auto) automatic(R);
}

/**
 * propagate for a task's state written locally: its memo readers become
 * dirty as usual; an effect reader does too unless only the status moved
 * and its latest run read none (changed).
 * @param {Graph} R @param {Node<any>} source @param {boolean} statusOnly
 */
function published(R, source, statusOnly) {
  const m = R.mark;
  for (let l = source.subs; l !== null; l = l.nextSub) {
    const x = l.sub;
    const was = x.state;
    const k = x.k;
    if (k === EFFECT && statusOnly && x.watchers !== x.version) continue;
    // Already queued deferred: an urgent change moves it up.
    if (was !== CLEAN && k === EFFECT && x.mode === LOW) enqueue(R, x);
    if (was === DIRTY) continue;
    x.state = DIRTY;
    if (was === CLEAN) {
      if (k === MEMO) {
        if (x.apply === null || eager(R, x)) m.a[m.n++] = x;
      } else if (k === EFFECT) enqueue(R, x);
    }
  }
  if (m.n > 0) propagatePending(R);
}

// ---------------------------------------------------------------------------
// Public: nodes

/**
 * @template T
 * @typedef {Node<T> & { readonly __read?: T }} Readable
 */
/**
 * @template T
 * @typedef {Readable<T> & { readonly __write: (value: T) => void }} Writable
 */
/** @typedef {Node<any> & { readonly __effect?: true }} Effecting */
/** @typedef {ScopeNode} Owner */

/**
 * Reads a signal, memo, derivation or task, tracking it in the running
 * computation. A freed node reads as its final value.
 * @template T
 * @param {Readable<T> | import('./task.js').Task<T>} source
 * @returns {T}
 */
export function $read(source) {
  const x = /** @type {Node<T>} */ (source);
  const k = x.k;
  if (x.live !== FREED) {
    if (k === SIGNAL) {
      const observer = x.root.current;
      if (observer !== null) link(x.root, x, observer);
      return x.value;
    }
    if (k === MEMO) return readMemo(x.root, x);
  }
  return readOther(source);
}

/** Reads a freed node, a task, or refuses anything else. @param {any} source */
function readOther(source) {
  const k = source.k;
  if (k === SIGNAL || k === MEMO) return stale(source);
  if (source.kind === 'task') return source.read();
  throw new TypeError('RRX: $read expects a signal, memo, derivation, or task');
}

/** @param {Node<any>} x */
function stale(x) {
  live(x.root);
  return x.value;
}

/**
 * Stores a value in a signal or derivation and returns it. A write to a freed
 * node only updates the handle's final value.
 * @template T
 * @param {Writable<T>} signal
 * @param {NoInfer<T>} value
 * @returns {T}
 */
export function $write(signal, value) {
  if (signal.live !== FREED) write(signal.root, signal, value);
  else {
    live(signal.root);
    signal.value = value;
  }
  return value;
}

/** @param {Graph} R @param {Node<any>} x @param {any} value */
function write(R, x, value) {
  const e = x.extra;
  if (R.pure && x.mode !== PERMITTED && (e === null || e.permit === 0)) impure();
  if (x.k !== SIGNAL) derived(R, x);
  const old = x.value;
  if (e === null || e.equals == null ? Object.is(old, value) : same(e.equals, old, value)) return;
  x.value = value;
  x.version++;
  if (x.subs !== null) propagate(R, x);
  if (R.auto) automatic(R);
}

/** @returns {never} */
function impure() {
  throw new Error('RRX: computation cannot write reactive state');
}

/** A write to a derivation: pending source changes apply first, so the write is the latest value. @param {Graph} R @param {Node<any>} x */
function derived(R, x) {
  if (x.k !== MEMO) throw new TypeError('RRX: $write expects a signal or derivation');
  if (x.extra === null || x.extra.writable === 0) throw new Error('RRX: a memo is read-only; use $derive for a writable derivation');
  if (x.state !== CLEAN) refresh(R, x);
  if (x.mode !== VALUE) {
    x.mode = VALUE;
    extra(x).failure = undefined;
  }
}

/** In `$auto` mode a write outside computations, batches and flushes flushes now. @param {Graph} R */
export function automatic(R) {
  if (R.current === null && !R.flushing && R.batching === 0) $flush(R);
}

/**
 * A writable source.
 * @template T
 * @param {Graph} root
 * @param {T} initial
 * @returns {Writable<T>}
 */
export function $signal(root, initial) {
  return /** @type {any} */ (new Node(SIGNAL, root, initial, undefined));
}

/**
 * A lazy derived value; invalidation never evaluates it, and a recomputed
 * value equal to the previous one leaves its observers untouched.
 * @template T
 * @param {Graph} root
 * @param {() => T} run
 * @returns {Readable<T>}
 */
export function $memo(root, run) {
  return /** @type {any} */ (new Node(MEMO, root, undefined, run));
}

/**
 * A memo whose step receives its previous value, or `initial` before the
 * first.
 * @template T
 * @param {Graph} root
 * @param {T} initial
 * @param {(previous: T) => T} step
 * @returns {Readable<T>}
 */
export function $scan(root, initial, step) {
  // The first computation receives `initial` as the previous value.
  return /** @type {any} */ (new Node(MEMO, root, initial, step));
}

/**
 * A writable derivation: compute's value until a local Write, which holds
 * until one of compute's sources changes.
 * @template T
 * @param {Graph} root
 * @param {() => T} run
 * @returns {Writable<T>}
 */
export function $derive(root, run) {
  const memo = $memo(root, run);
  extra(memo).writable = 1;
  return /** @type {any} */ (memo);
}

/**
 * `equals(old, next)` true means observers need not change; false notifies
 * every write; null restores identity (`Object.is`).
 * @template {Node<any>} H
 * @param {H} source
 * @param {Equality} equals
 * @returns {H}
 */
export function $equals(source, equals) {
  if (node(source) !== null) extra(source).equals = equals;
  return source;
}

/**
 * Runs `hook` when the last observer of a signal leaves.
 * @template {Node<any>} H
 * @param {H} signal
 * @param {(() => void) | null} hook
 * @returns {H}
 */
export function $whenUnwatched(signal, hook) {
  if (node(signal) !== null) extra(signal).hook = hook;
  return signal;
}

/**
 * A signal that computations may write, such as state derived from a source.
 * @template {Node<any>} H
 * @param {H} signal
 * @returns {H}
 */
export function $writable(signal) {
  // A signal is never queued: its queue lane holds the permit, so the
  // common writable signal (a list row's item) needs no Extra record.
  if (node(signal) !== null) {
    if (signal.k === SIGNAL) signal.mode = PERMITTED;
    else extra(signal).permit = 1;
  }
  return signal;
}

/** The number of attached subscribers watching a node. @param {Node<any>} source */
export function $observers(source) {
  return node(source) === null || source.k === EFFECT ? 0 : source.watchers;
}

/**
 * The sources a node read in its latest run, in read order.
 * @param {Node<any>} source
 * @returns {Node<any>[]}
 */
export function $sources(source) {
  /** @type {Node<any>[]} */
  const found = [];
  if (node(source) !== null) for (let l = source.deps; l !== null; l = l.nextDep) found.push(l.dep);
  return found;
}

/**
 * Distinguishes nodes, including a new node that reuses a freed id.
 * @param {Node<any> | ScopeNode} source
 * @returns {[number, number]}
 */
export function $identity(source) {
  return [source.id, source.gen];
}

/**
 * The kind of a reactive value: signal, memo, effect, scope, task, store, or
 * null.
 * @param {unknown} value
 * @returns {'signal' | 'memo' | 'effect' | 'scope' | 'task' | 'store' | null}
 */
export function $kind(value) {
  if (value === null || typeof value !== 'object') return null;
  const kind = /** @type {any} */ (value).kind;
  return kind === 'signal' || kind === 'memo' || kind === 'effect' || kind === 'task' || kind === 'store' || kind === 'scope' ? kind : null;
}

/**
 * Counts of a root's live structures. Every live node is in an owner list,
 * so the counts come from walking the owner tree rather than from counters
 * kept on every creation, link and free: live nodes and scopes, their links,
 * and the attached observers (live effects and memos that have watchers).
 * A retired scope's nodes count until a `$sweep` frees them (they still exist);
 * `$sweep` returns how many retired scopes remain.
 * @param {Graph} root
 */
export function $stats(root) {
  let scopes = 0;
  let effects = 0;
  let nodes = 0;
  let links = 0;
  /** @type {ScopeNode[]} */
  const stack = [root.home];
  while (stack.length > 0) {
    const scope = /** @type {ScopeNode} */ (stack.pop());
    for (let c = scope.head; c !== null; c = c.next) {
      if (c.k === SCOPE) {
        scopes++;
        stack.push(/** @type {ScopeNode} */ (c));
        continue;
      }
      const x = /** @type {Node<any>} */ (c);
      nodes++;
      for (let l = x.deps; l !== null; l = l.nextDep) links++;
      if (x.k === EFFECT ? x.live >= ALIVE : x.k === MEMO && x.watchers > 0) effects++;
    }
  }
  return { scopes, effects, commits: root.commitCount, nodes, links };
}

/**
 * The running observer, or null outside computations.
 * @param {Graph} root
 * @returns {Node<any> | null}
 */
export function $observer(root) {
  return root.current;
}

/**
 * Runs `callback` without tracking its reads.
 * @template T
 * @param {Graph} root
 * @param {() => T} callback
 * @returns {T}
 */
export function $untrack(root, callback) {
  live(root);
  const previous = root.current;
  root.current = null;
  try {
    return callback();
  } finally {
    root.current = previous;
  }
}

// ---------------------------------------------------------------------------
// Freeing

/**
 * Frees a node: its own links are purged; each subscriber loses the link and
 * becomes dirty, so it recomputes without the vanished source.
 * @param {Graph} R @param {Node<any>} x
 */
function free(R, x) {
  // An eager memo (a task's state) ends with it: its cleanup is its teardown.
  if (x.apply !== null && x.k === MEMO) {
    const end = x.cleanup;
    x.cleanup = undefined;
    if (end !== undefined) end.call(x);
  }
  // An async memo's work in flight ends with it.
  if (x.extra !== null && x.extra.flight !== NONE) superseded(R, x);
  if (!R.closing) {
    x.depsTail = null;
    if (x.deps !== null) purge(R, x);
    if (x.subs !== null) orphan(R, x);
  }
  leave(x);
  x.live = FREED;
  if (R.hooks.length > 0 && !R.closing) runHooks(R);
}

/** Every reader of a freed node loses its link and becomes dirty. @param {Graph} R @param {Node<any>} x */
function orphan(R, x) {
  let l = x.subs;
  while (l !== null) {
    const after = l.nextSub;
    removeDep(l);
    mark(R, l.sub, DIRTY);
    l = after;
  }
  if (R.mark.n > 0) propagatePending(R);
  x.subs = null;
  x.subsTail = null;
}

/**
 * Frees a signal or memo before its owner stops.
 * @param {Node<any>} source
 */
export function $discard(source) {
  const R = source.root;
  if (!R.closed && source.live >= ALIVE) {
    if (source.live === RUNNING) source.live = CLOSING;
    else free(R, source);
  }
}

/**
 * Recomputes a memo at its next read and has its observers check it.
 * @param {Readable<any>} memo
 */
export function $invalidate(memo) {
  const R = memo.root;
  if (node(memo) !== null && memo.k === MEMO && memo.state === CLEAN) {
    memo.state = DIRTY;
    for (let l = memo.subs; l !== null; l = l.nextSub) mark(R, l.sub, PENDING);
    propagatePending(R);
  }
}

/**
 * Notifies the observers of everything `read` reads, as if each had changed.
 * @param {Graph} root
 * @param {() => unknown} read
 */
export function $touch(root, read) {
  const R = root;
  const probe = new Node(PROBE, R, undefined, null);
  const current = R.current;
  const cycle = R.cycle;
  R.cycle = ++R.cycles;
  R.current = probe;
  let failure;
  let ok = true;
  try {
    read();
  } catch (error) {
    ok = false;
    failure = error;
  }
  R.current = current;
  R.cycle = cycle;
  for (let l = probe.deps; l !== null; l = l.nextDep) {
    const dep = l.dep;
    dep.version++;
    propagate(R, dep);
  }
  free(R, probe);
  if (!ok) throw failure;
  if (R.auto && current === null && !R.flushing && R.batching === 0) $flush(R);
}

// ---------------------------------------------------------------------------
// Effects

/**
 * Stops an effect, Watch or Render and frees it.
 * @param {Effecting} observer
 */
export function $cancel(observer) {
  cancel(observer);
}

/** @param {Node<any>} observer */
function cancel(observer) {
  const R = observer.root;
  if (observer.live >= ALIVE) {
    observer.mode = 0;
    const app = observer.apply;
    if (app !== null && app.hold !== null) resume(observer, true);
    const cleanup = observer.cleanup;
    observer.cleanup = undefined;
    if (observer.live === RUNNING) observer.live = CLOSING;
    else free(R, observer);
    if (typeof cleanup === 'function') untracked(R, cleanup);
  }
}

/** @param {Graph} R @param {() => any} fn */
function untracked(R, fn) {
  const previous = R.current;
  R.current = null;
  try {
    return fn();
  } finally {
    R.current = previous;
  }
}

// Effects. execute is the hot path of every effect run; its rare branches
// (cleanups, a closing effect, Watch and Render values) live in helpers so
// the common path stays small enough for V8 to inline and optimize whole.

/** @returns {never} */
function cycled() {
  throw new Error(`RRX: reactive cycle; an effect ran more than ${LIMIT} times in one flush`);
}

/** Runs and forgets an effect's cleanup before it reruns. @param {Graph} R @param {Node<any>} x */
function rerun(R, x) {
  const cleanup = x.cleanup;
  x.cleanup = undefined;
  if (typeof cleanup === 'function') untracked(R, cleanup);
}

/**
 * An effect cancelled during its own run is freed once the run returns.
 * @param {Graph} R @param {Node<any>} x @param {boolean} ok @param {any} result @param {Application | null} app
 */
function closed(R, x, ok, result, app) {
  free(R, x);
  if (!ok) throw result;
  if (typeof result === 'function' && app === null) untracked(R, result);
}

/** A Watch or Render: the value applies after the computations settle. @param {Graph} R @param {Node<any>} x @param {Application} app */
function applies(R, x, app) {
  // Nothing new to apply (a binding that applied its first value itself).
  if (app.applying === 0 && app.applied === 1 && Object.is(x.value, app.shown)) return;
  // A new value for parked work belongs to its transition.
  if (app.parked !== null) join(R, app.parked);
  if (R.flushing || R.batching !== 0) {
    if (app.applying === 0) {
      const q = app.render ? R.renders : R.applies;
      q.a[q.n++] = x;
    }
    app.applying = R.lane;
  } else commit(R, x);
}

/** @param {Graph} R @param {Node<any>} x */
function execute(R, x) {
  if (x.live < ALIVE) return;
  // One field counts the runs of this flush: serials step by STRIDE, more
  // than LIMIT, so a count left from an earlier flush is below the serial.
  const serial = R.serialAll;
  let runs = x.version;
  if (runs < serial) runs = serial;
  if (++runs - serial > LIMIT) cycled();
  x.version = runs;
  x.state = CLEAN;
  const app = x.apply;
  if (app === null && x.cleanup !== undefined) rerun(R, x);
  const current = R.current;
  const owner = R.owner;
  const pure = R.pure;
  R.cycle = ++R.cycles;
  x.depsTail = null;
  R.current = x;
  x.live = RUNNING;
  // The run enters its owner, as $enter(owner, callback) does.
  const scope = x.owner;
  const entered = scope !== owner;
  const ctx = R.ctx;
  if (entered) {
    R.owner = scope;
    R.ctx = scope.context;
  }
  if (app !== null) R.pure = true;
  let result;
  let ok = true;
  try {
    result = x.run();
  } catch (error) {
    ok = false;
    result = error;
  }
  R.current = current;
  if (entered) {
    R.owner = owner;
    R.ctx = ctx;
  }
  R.pure = pure;
  // The run may have cancelled the effect.
  if (x.live === CLOSING) return closed(R, x, ok, result, app);
  x.live = ALIVE;
  // The run moved the cursor; the cast undoes the narrowing to null above.
  const tail = /** @type {Link | null} */ (x.depsTail);
  if ((tail === null ? x.deps : tail.nextDep) !== null) purge(R, x);
  if (!ok) {
    // A pending read of a Watch or Render whose value is shown suspends it.
    if (result === WAITING && app !== null && app.applied === 1) {
      waitedOn();
      suspend(x);
      return;
    }
    // A failure ends the wait: the boundary that handles it shows now.
    if (app !== null && app.hold !== null) resume(x, false);
    throw result;
  }
  if (app === null) {
    if (typeof result === 'function') x.cleanup = result;
  } else {
    // A run that returned without suspending again reads a value: it resumes.
    if (app.hold !== null && app.stay !== x.version) resume(x, false);
    x.value = result;
    applies(R, x, app);
  }
}

/** Applies a Watch or Render value that differs from the last one applied. @param {Graph} R @param {Node<any>} x */
function commit(R, x) {
  const value = x.value;
  const app = /** @type {Application} */ (x.apply);
  if (app.applied === 1 && Object.is(value, app.shown)) return;
  app.applied = 1;
  app.shown = value;
  const cleanup = x.cleanup;
  x.cleanup = undefined;
  const current = R.current;
  const owner = R.owner;
  const ctx = R.ctx;
  const scope = x.owner;
  R.current = null;
  R.owner = scope;
  R.ctx = scope.context;
  let result;
  try {
    if (typeof cleanup === 'function') cleanup();
    if (dev) result = inspected(R, x, app, value);
    else result = app.run(value);
  } finally {
    R.current = current;
    R.owner = owner;
    R.ctx = ctx;
  }
  if (typeof result === 'function') {
    if (x.live >= ALIVE) x.cleanup = result;
    else untracked(R, /** @type {() => unknown} */ (result));
  }
}

/**
 * An application in development: reactive reads in it are untracked and
 * never rerun it; they belong in the read.
 * @param {Graph} R @param {Node<any>} x @param {Application} app @param {any} value
 */
function inspected(R, x, app, value) {
  const [result, reads] = counted(R, () => app.run(value));
  if (reads > 0) report(`RRX dev: a Watch or Render application (observer ${x.id}) read reactive state; the application is untracked, so it will not run again when that changes. Read it in the read function.`);
  return result;
}

/** @param {Graph} R @param {Node<any>} x */
function refreshEffect(R, x) {
  if (x.live < ALIVE) return;
  if (R.retiring && inert(R, x.owner)) return;
  if (x.state === PENDING) {
    // The common case, an effect whose first source is a dirty memo, is
    // settled here without entering refresh; refresh goes on from there.
    const l = x.deps;
    if (l !== null) {
      const dep = l.dep;
      if (dep.k === MEMO && dep.state === DIRTY && dep.live === ALIVE) {
        compute(R, dep);
        if (x.state === PENDING && dep.version !== l.seen) x.state = DIRTY;
      }
    }
    if (x.state === PENDING) refresh(R, x);
  }
  if (x.state === DIRTY) execute(R, x);
}

/**
 * The first run of a new effect. It is execute without the checks a new node
 * cannot need (it is alive, owned by the current owner, has no cleanup, no
 * sources to purge, and no earlier run in this flush), and without a second
 * try around the run: a failing run is delivered here instead of thrown to a
 * catching caller. The rare paths that run more user code (a Watch applying
 * at once, an effect that cancelled itself) keep their own try.
 * Returns the effect.
 * @template {Node<any>} N @param {N} x @returns {N}
 */
function start(x) {
  const R = x.root;
  const was = running;
  running = R;
  x.version = R.serialAll + 1;
  x.state = CLEAN;
  const app = x.apply;
  const current = R.current;
  const pure = R.pure;
  R.cycle = ++R.cycles;
  R.current = x;
  x.live = RUNNING;
  if (app !== null) R.pure = true;
  let result;
  let ok = true;
  try {
    result = x.run();
  } catch (error) {
    ok = false;
    result = error;
  }
  R.current = current;
  R.pure = pure;
  running = was;
  // The run may have cancelled the effect.
  if (x.live === CLOSING) startClosed(R, x, ok, result, app);
  else {
    x.live = ALIVE;
    if (!ok) startFailed(R, x, result);
    else if (app === null) {
      if (typeof result === 'function') x.cleanup = result;
    } else startApplies(R, x, app, result);
  }
  return x;
}

/** A first run that cancelled its effect. @param {Graph} R @param {Node<any>} x @param {boolean} ok @param {any} result @param {Application | null} app */
function startClosed(R, x, ok, result, app) {
  try {
    closed(R, x, ok, result, app);
  } catch (error) {
    startFailed(R, x, error);
  }
}

/** A Watch or Render whose first value applies now, or after the flush. @param {Graph} R @param {Node<any>} x @param {Application} app @param {any} result */
function startApplies(R, x, app, result) {
  x.value = result;
  try {
    applies(R, x, app);
  } catch (error) {
    startFailed(R, x, error);
  }
}

/**
 * A failed first run goes to the owner's `$catch`; unhandled, the effect is
 * cancelled and the failure raised.
 * @param {Graph} R @param {Node<any>} x @param {any} error
 */
function startFailed(R, x, error) {
  const failure = deliver(R, x.owner, error);
  if (failure !== HANDLED) {
    try {
      $cancel(x);
    } catch {
      // The original failure is raised.
    }
    throw failure;
  }
}

/**
 * An eager observer. It runs now and again, in creation order, at each flush
 * after a source changed. A returned function runs before the next run and
 * when the effect stops.
 * @param {Graph} root
 * @param {() => unknown} callback
 * @returns {Effecting}
 */
export function $effect(root, callback) {
  return start(new Node(EFFECT, root, undefined, callback));
}

/**
 * @template T
 * @typedef {{ observer: Effecting, stop: () => void }} Watching
 */

/**
 * @template T
 * @param {Graph} R @param {() => T} read @param {(value: T) => unknown} apply @param {boolean} render
 * @returns {Watching<T>}
 */
function watching(R, read, apply, render) {
  const x = new Node(EFFECT, R, undefined, read);
  x.apply = new Application(apply, render);
  start(x);
  return { observer: x, stop: () => $cancel(x) };
}

/**
 * A Render whose read and apply are shared functions: `read` runs with the
 * node as `this` (its Application is `this.apply`), and the application's
 * `run` receives each value with the application as `this`. A renderer's
 * bindings allocate one node and one application and no closures (ren-core).
 * @param {Graph} root @param {Application} app @param {(this: Node<any>) => any} read
 * @returns {Node<any>}
 */
export function $bind(root, app, read) {
  const x = new Node(EFFECT, root, undefined, read);
  x.apply = app;
  start(x);
  return x;
}

/**
 * A tracked, pure `read` whose value `apply` receives after the computations
 * settle. `apply` may return a cleanup.
 * @template T
 * @param {Graph} root
 * @param {() => T} read
 * @param {(value: T) => unknown} apply
 * @returns {Watching<T>}
 */
export function $watch(root, read, apply) {
  return watching(root, read, apply, false);
}

/**
 * Like Watch, applied in the earlier Render phase, before the Watch phase.
 * @template T
 * @param {Graph} root
 * @param {() => T} read
 * @param {(value: T) => unknown} apply
 * @returns {Watching<T>}
 */
export function $render(root, read, apply) {
  return watching(root, read, apply, true);
}

// ---------------------------------------------------------------------------
// Transitions (the graph holds a consistent state while async
// work runs). A Watch or Render whose value is shown and whose read goes
// pending suspends: it keeps its value, and the flush joins a transition.
// Every application such a flush would make is parked in the transition
// instead, except those of observers whose latest run read a status
// ($pending, $behind, $lastReady: the indicators), which apply at once. A later
// flush that touches parked work, or resumes a suspended observer, joins the
// same transition (two it touches merge). When nothing in the transition
// waits any more, its parked applications and the flush's own apply
// together, in creation order. Updates that touch none of this apply as
// usual. Plain effects are never held.

/** The current flush belongs to transition `t` (merging the one it had). @param {Graph} R @param {Transition} t */
function join(R, t) {
  const into = target(t);
  const had = R.flow;
  if (had === null) {
    R.flow = into;
    return;
  }
  const kept = target(had);
  R.flow = kept;
  if (kept === into) return;
  // Merge: the joined transition forwards to the flush's.
  kept.waits += into.waits;
  into.waits = 0;
  const moved = into.parked;
  into.parked = [];
  for (let i = 0; i < moved.length; i++) {
    const x = moved[i];
    if (x.apply !== null && x.apply.parked === into) {
      x.apply.parked = kept;
      kept.parked.push(x);
    }
  }
  into.into = kept;
}

/**
 * A Watch or Render (the running observer `x`, for sibling modules: a
 * renderer's binding) read a pending value while its own is shown: it
 * suspends, keeping that value, and the flush joins its transition. A
 * binding that suspends itself returns the value it shows; the run stays
 * suspended.
 * @param {Node<any>} x
 */
export function suspend(x) {
  const app = x.apply;
  if (app === null) return;
  const R = x.root;
  app.stay = x.version;
  if (app.hold !== null) {
    join(R, app.hold);
    return;
  }
  if (R.flow === null) R.flow = new Transition();
  const t = target(R.flow);
  R.flow = t;
  app.hold = t;
  t.waits++;
}

/**
 * A suspended observer reads a value again, or stops: it no longer waits,
 * and (unless stopping) the flush joins its transition, so what it applies
 * next commits with the rest (for sibling modules).
 * @param {Node<any>} x @param {boolean} stopping
 */
export function resume(x, stopping) {
  const app = x.apply;
  if (app === null || app.hold === null) return;
  const t = target(app.hold);
  app.hold = null;
  t.waits--;
  // A stopping observer joins only to release parked work nothing else waits for.
  if (!stopping || (t.waits === 0 && t.parked.length > 0)) {
    const R = x.root;
    join(R, t);
    if (stopping && R.auto) automatic(R);
  }
}

/**
 * The running Watch or Render read a status or a latest value (for sibling
 * modules: $pending, $behind, $lastReady): it is an indicator, applied at once
 * even while a transition holds the rest.
 */
export function peeked() {
  const R = running;
  if (R === null) return;
  peeks++;
  const x = R.current;
  if (x !== null && x.apply !== null && x.k === EFFECT) x.apply.peek = x.version;
}

/** Indicator reads so far: a renderer compares it around a computation of its own (for sibling modules). */
let peeks = 0;
/** @returns {number} */
export function peekCount() {
  return peeks;
}

/**
 * The flush's transition, committed when nothing in it waits: its parked
 * applications queue again (and the flush leaves it). Returns true when that
 * queued work.
 * @param {Graph} R
 */
function released(R) {
  const t = target(/** @type {Transition} */ (R.flow));
  if (t.waits > 0) {
    R.flow = t;
    return false;
  }
  R.flow = null;
  const parked = t.parked;
  t.parked = [];
  let queued = false;
  for (let i = 0; i < parked.length; i++) {
    const x = parked[i];
    const app = x.apply;
    if (app === null || app.parked !== t) continue;
    app.parked = null;
    if (x.live < ALIVE || app.applying === 0) continue;
    const q = app.render ? R.renders : R.applies;
    q.a[q.n++] = x;
    queued = true;
  }
  if (queued) {
    sortBatch(R.renders.a, R.renders.n);
    sortBatch(R.applies.a, R.applies.n);
  }
  return queued;
}

// ---------------------------------------------------------------------------
// Flush

/** @param {Graph} R */
function busy(R) {
  return R.eagerUrgent.n + R.urgent.n + R.eagerLow.n + R.low.n + R.renders.n + R.applies.n > 0;
}

/** Sorts a batch of effects by creation order. @param {Node<any>[]} a @param {number} n */
function sortBatch(a, n) {
  for (let i = 1; i < n; i++) {
    const v = a[i];
    const k = v.id;
    if (a[i - 1].id <= k) continue;
    if (n > 32) return sortMany(a, n);
    let j = i - 1;
    while (j >= 0 && a[j].id > k) {
      a[j + 1] = a[j];
      j--;
    }
    a[j + 1] = v;
  }
}

/** A long unsorted batch sorts in one pass. @param {Node<any>[]} a @param {number} n */
function sortMany(a, n) {
  const sorted = a.slice(0, n).sort((x, y) => x.id - y.id);
  for (let j = 0; j < n; j++) a[j] = sorted[j];
}

/** @type {any} */
const HANDLED = Symbol('handled');

/**
 * Offers an error to the nearest handling owner from `scope` upward, then the
 * root handler. Returns HANDLED, or the error that remains unhandled.
 * @param {Graph} R @param {ScopeNode} scope @param {any} error
 */
function deliver(R, scope, error) {
  let failure = error;
  for (let s = scope; s !== R.home && s !== null; s = s.owner) {
    const r = s.rare;
    const handler = r === null ? null : r.handler;
    if (typeof handler === 'function') {
      try {
        handler(failure);
        return HANDLED;
      } catch (next) {
        failure = next;
      }
    }
  }
  const handler = R.errorHandler;
  if (typeof handler === 'function') {
    try {
      handler(failure);
      return HANDLED;
    } catch (next) {
      failure = next;
    }
  }
  return failure;
}

/** @param {Graph} R @param {number} lane */
function runLane(R, lane) {
  const batch = lane === URGENT ? R.urgent : R.low;
  if (lane === URGENT) R.urgent = R.spare;
  else R.low = R.spare;
  R.spare = batch;
  const a = batch.a;
  const n = batch.n;
  batch.n = 0;
  if (n > 1) sortBatch(a, n);
  // Work from the low lane counts as deferred for the adapters it reaches.
  R.lane = lane;
  /** @type {Node<any> | null} */
  let last = null;
  for (let i = 0; i < n; i++) {
    const x = a[i];
    a[i] = /** @type {any} */ (null);
    if (x === last) continue;
    last = x;
    if (x.mode !== lane) continue;
    x.mode = 0;
    try {
      refreshEffect(R, x);
    } catch (error) {
      failed(R, x.owner, error);
    }
  }
  R.lane = URGENT;
}

/**
 * Refreshes the eager memos queued for `lane`, in the order they were
 * reached; one a read already refreshed is clean and skipped. Refreshing may
 * queue more (a launch that writes), which the flush picks up next.
 * @param {Graph} R @param {number} lane
 */
function runEager(R, lane) {
  const batch = lane === URGENT ? R.eagerUrgent : R.eagerLow;
  const a = batch.a;
  const n = batch.n;
  batch.n = 0;
  // Effect readers a deferred memo reaches queue deferred too (changed).
  if (lane !== URGENT) R.transition++;
  for (let i = 0; i < n; i++) {
    const x = a[i];
    a[i] = /** @type {any} */ (null);
    if (x.state === CLEAN || x.live === FREED) continue;
    try {
      if (x.state === DIRTY && x.live === ALIVE) compute(R, x);
      else refresh(R, x);
    } catch (error) {
      failed(R, x.owner, error);
    }
  }
  if (lane !== URGENT) R.transition--;
}

/** Delivers a failure from a flush; the first unhandled one is raised when the flush ends. @param {Graph} R @param {ScopeNode} owner @param {any} error */
export function failed(R, owner, error) {
  const left = deliver(R, owner, error);
  if (left !== HANDLED && R.failed === HANDLED) R.failed = left;
}

/**
 * Keeps an unhandled failure (already offered to the boundaries) to raise
 * when the current flush ends, or at the next `$flush`, so the computation that
 * met it can still commit its value (a list whose removed row failed to
 * stop). The first one wins, as in a flush. @param {Graph} R @param {any} error
 */
export function later(R, error) {
  if (R.failed === HANDLED) R.failed = error;
}

/** @param {Graph} R @param {boolean} renders */
function drainApplies(R, renders) {
  // A transition nothing waits on any more commits: its parked work joins this drain.
  if (R.flow !== null) released(R);
  const flow = R.flow;
  const batch = renders ? R.renders : R.applies;
  if (renders) R.renders = R.spareApply;
  else R.applies = R.spareApply;
  R.spareApply = batch;
  const a = batch.a;
  const n = batch.n;
  batch.n = 0;
  for (let i = 0; i < n; i++) {
    const x = a[i];
    a[i] = /** @type {any} */ (null);
    if (x.live < ALIVE || x.apply === null) continue;
    const app = x.apply;
    const lane = app.applying;
    if (lane === 0) continue;
    // Held: parked in the transition, unless its latest run read a status (an indicator).
    if (flow !== null && app.peek !== x.version) {
      if (app.parked !== flow) {
        app.parked = flow;
        flow.parked.push(x);
      }
      continue;
    }
    app.applying = 0;
    if (R.retiring && inert(R, x.owner)) continue;
    R.lane = lane;
    try {
      commit(R, x);
    } catch (error) {
      failed(R, x.owner, error);
    }
    R.lane = URGENT;
  }
}

/** @param {Graph} R @param {(() => void)[]} callbacks */
function drainOnce(R, callbacks) {
  for (let i = 0; i < callbacks.length; i++) {
    try {
      callbacks[i]();
    } catch (error) {
      failed(R, R.home, error);
    }
  }
}

/** @param {Graph} R */
function rounds(R) {
  // The flush runs this root's code: `$lookup` without a root finds this root.
  // One store per flush, not per effect run.
  const was = running;
  running = R;
  try {
    turns(R);
  } finally {
    running = was;
  }
}

/** The rounds of a flush. @param {Graph} R */
function turns(R) {
  let round = 0;
  for (;;) {
    if (++round > ROUNDS) throw new Error('RRX: reactive cycle; commit hooks kept the graph busy');
    for (;;) {
      if (R.eagerUrgent.n > 0) runEager(R, URGENT);
      else if (R.urgent.n > 0) runLane(R, URGENT);
      else if (R.eagerLow.n > 0) runEager(R, LOW);
      else if (R.low.n > 0) runLane(R, LOW);
      else if (R.renders.n > 0) drainApplies(R, true);
      else if (R.applies.n > 0) drainApplies(R, false);
      else if (R.flow !== null && released(R)) continue;
      else break;
    }
    // Nothing is queued now; only the callbacks can queue more, so a round
    // that calls none of them ends the flush.
    if (R.commitCount === 0 && R.after.length === 0 && R.settled.length === 0) return;
    if (!callbacks(R) || !busy(R)) return;
  }
}

/**
 * Drops the entries of `$commit` hooks whose owner stopped, once they are
 * most of the list, so a round walks the live ones only.
 * @param {Graph} R
 */
function compactCommits(R) {
  const commits = R.commits;
  if (commits.length <= 2 * R.commitCount + 16) return;
  let k = 0;
  for (let i = 0; i < commits.length; i++) {
    const entry = commits[i];
    if (entry.run !== null) commits[k++] = entry;
  }
  commits.length = k;
}

/** `$commit` hooks, `$after` and `$settled` callbacks of a round; true when any ran. @param {Graph} R */
function callbacks(R) {
  let called = false;
  compactCommits(R);
  if (R.commitCount > 0) {
    called = true;
    const commits = R.commits;
    for (let i = 0; i < commits.length; i++) {
      const entry = commits[i];
      const run = entry.run;
      if (run === null) continue;
      if (R.retiring && inert(R, entry.owner)) continue;
      try {
        run();
      } catch (error) {
        failed(R, R.home, error);
      }
    }
  }
  if (R.after.length > 0) {
    called = true;
    const once = R.after;
    R.after = [];
    drainOnce(R, once);
  }
  if (R.settled.length > 0 && R.loading === 0 && !busy(R)) {
    called = true;
    const once = R.settled;
    R.settled = [];
    drainOnce(R, once);
  }
  return called;
}

/**
 * Runs queued work until the graph settles: urgent effects, low-lane effects,
 * `$render` then `$watch` applications, `$commit` hooks, `$after` callbacks, and `$settled`
 * callbacks once no task is pending. A failure does not abandon the flush;
 * the first unhandled one is raised afterwards. A flush with nothing to do
 * returns at once.
 * @param {Graph} root
 */
export function $flush(root) {
  const R = root;
  if (R.closed || R.flushing) refuse(R);
  if (!busy(R) && R.flow === null && R.commitCount === 0 && R.after.length === 0 && R.settled.length === 0) {
    if (R.failed !== HANDLED) raise(R);
    return;
  }
  R.flushing = true;
  R.serialAll += STRIDE;
  try {
    rounds(R);
  } finally {
    R.flushing = false;
    R.lane = URGENT;
    if (R.hastened.size > 0) R.hastened.clear();
    // The next flush joins a transition only by touching it again.
    R.flow = null;
  }
  if (R.failed !== HANDLED) raise(R);
}

/** A flush of a disposed root, or inside a flush, is refused. @param {Graph} R @returns {never} */
function refuse(R) {
  live(R);
  throw new Error('RRX: recursive flush');
}

/** Raises the first unhandled failure of a flush. @param {Graph} R @returns {never} */
function raise(R) {
  const failure = R.failed;
  R.failed = HANDLED;
  throw failure;
}

/**
 * Builds with Watch applications queued, then flushes them once. Nested
 * batches join the outer one.
 * @template T
 * @param {Graph} root
 * @param {() => T} build
 * @returns {T}
 */
export function $batch(root, build) {
  live(root);
  if (root.batching > 0 || root.flushing) return build();
  root.batching = 1;
  let value;
  try {
    value = build();
  } catch (error) {
    root.batching = 0;
    failedBatch(root, error);
  }
  root.batching = 0;
  // A build that disposed its own root leaves nothing to flush.
  if (!root.closed) $flush(root);
  return value;
}

/**
 * A batch whose build threw still flushes what it wrote, then raises the
 * build's failure. A failure the flush leaves unhandled (already offered to
 * the boundaries) is kept and raised by the next `$flush`.
 * @param {Graph} R @param {any} error @returns {never}
 */
function failedBatch(R, error) {
  if (!R.closed) {
    try {
      $flush(R);
    } catch (failure) {
      later(R, failure);
    }
  }
  throw error;
}

/** True while a `$batch` builds. @param {Graph} root */
export function $batching(root) {
  return root.batching > 0;
}

/** Joins a batch without flushing at its end (internal). @param {Graph} R */
export function hold(R) {
  R.batching++;
}
/** Leaves a hold; under `$auto`, flushes (a root disposed meanwhile has nothing to flush). @param {Graph} R */
export function unhold(R) {
  R.batching--;
  if (R.auto && !R.closed) automatic(R);
}

/**
 * With `on`, a write outside `$batch` and computations flushes at once; the
 * default coalesces writes until `$flush`.
 * @param {Graph} root
 * @param {boolean} on
 */
export function $auto(root, on) {
  root.auto = on;
}

/**
 * Runs once after the graph of the current, or next, flush settles.
 * @param {Graph} root
 * @param {() => void} callback
 */
export function $after(root, callback) {
  live(root);
  root.after.push(callback);
}

/**
 * Runs once after a flush leaves nothing queued and no task pending. One
 * registered by a `$settled` callback runs in a later round of the same
 * flush when that callback queued work, else in the next flush.
 * @param {Graph} root
 * @param {() => void} callback
 */
export function $settled(root, callback) {
  live(root);
  root.settled.push(callback);
}

/**
 * Runs `callback` with the effects it invalidates queued in the low lane.
 * @template T
 * @param {Graph} root
 * @param {() => T} callback
 * @returns {T}
 */
export function $defer(root, callback) {
  live(root);
  root.transition++;
  try {
    return callback();
  } finally {
    root.transition--;
  }
}

/** True inside `$defer` and while the flush runs work that `$defer` queued. @param {Graph} root */
export function $deferred(root) {
  return root.transition > 0 || root.lane === LOW;
}

/**
 * Shuts a root down: every scope stops, every effect is cancelled, and every
 * handle becomes stale, so reads find the closed root.
 * @param {Graph} root
 */
export function $dispose(root) {
  const R = root;
  if (R.closed) return;
  R.closing = true;
  R.urgent.n = R.low.n = R.eagerUrgent.n = R.eagerLow.n = R.renders.n = R.applies.n = 0;
  /** @type {any} */
  let failure = HANDLED;
  try {
    stopScope(R, R.home);
  } catch (error) {
    failure = error;
  }
  R.closed = true;
  R.retired = [];
  R.retiredAt = 0;
  R.sweepTop = R.sweepCur = null;
  R.retiring = false;
  R.commits.length = 0;
  R.commitCount = 0;
  R.after = [];
  R.settled = [];
  if (failure !== HANDLED) throw failure;
}

/** True once a root is disposed. @param {Graph} root */
export function $closed(root) {
  return root.closed;
}

// ---------------------------------------------------------------------------
// Scopes. A scope's owned nodes and child scopes form one list.

/**
 * A scope that owns the nodes, scopes and cleanups created inside it. A null
 * parent attaches it to the root.
 * @param {Graph} root
 * @param {Owner | null} parent
 * @returns {Owner}
 */
export function $scope(root, parent) {
  live(root);
  const p = parent == null ? root.home : parent;
  if (p.live !== ALIVE) throw new Error('RRX: cannot attach to a disposed scope');
  const scope = new ScopeNode(root, p, ++root.nextId, root.ctx);
  adopt(p, scope);
  return scope;
}

/**
 * Runs `callback` with `owner` as the current owner.
 * @template T
 * @param {Owner} owner
 * @param {() => T} callback
 * @returns {T}
 */
export function $enter(owner, callback) {
  const R = owner.root;
  live(R);
  if (owner.live !== ALIVE) throw new Error('RRX: disposed scope');
  const previous = R.owner;
  const context = R.ctx;
  const was = running;
  R.owner = owner;
  R.ctx = owner.context;
  running = R;
  try {
    return callback();
  } finally {
    R.owner = previous;
    R.ctx = context;
    running = was;
  }
}

/**
 * Calls `fn(a, b)` untracked with `owner` as the current owner: `$enter` and
 * `$untrack` in one call, without closures (list rows).
 * @template A, B, T
 * @param {Owner} owner @param {(a: A, b: B) => T} fn @param {A} a @param {B} b
 * @returns {T}
 */
export function $within(owner, fn, a, b) {
  const R = owner.root;
  live(R);
  if (owner.live !== ALIVE) throw new Error('RRX: disposed scope');
  const previous = R.owner;
  const context = R.ctx;
  const current = R.current;
  const was = running;
  R.owner = owner;
  R.ctx = owner.context;
  R.current = null;
  running = R;
  try {
    return fn(a, b);
  } finally {
    R.owner = previous;
    R.ctx = context;
    R.current = current;
    running = was;
  }
}

/**
 * Attaches a scope made directly (a ScopeNode subclass, such as a list row
 * that carries its own fields) to its owner, as `$scope` does.
 * @param {ScopeNode} scope
 * @returns {ScopeNode}
 */
export function $attach(scope) {
  const root = scope.root;
  live(root);
  const p = scope.owner;
  if (p.live !== ALIVE) throw new Error('RRX: cannot attach to a disposed scope');
  adopt(p, scope);
  return scope;
}

/**
 * Runs `callback` with no owner, for nodes whose lifetime is managed
 * explicitly.
 * @template T
 * @param {Graph} root
 * @param {() => T} callback
 * @returns {T}
 */
export function $unowned(root, callback) {
  const previous = root.owner;
  root.owner = root.home;
  try {
    return callback();
  } finally {
    root.owner = previous;
  }
}

/**
 * Stops a scope: frees what it owns, newest first (child scopes stop the same
 * way), then runs its cleanups in reverse order. Under `$auto`, stopping the
 * last pending task flushes, so the `$settled` callbacks run.
 * @param {Owner} owner
 */
export function $stop(owner) {
  const R = owner.root;
  if (R.closed || owner === R.home) return;
  const live = owner.live;
  // A retired scope's teardown finishes now.
  if (live === ALIVE || live === RETIRED) {
    const loading = R.loading;
    try {
      stopScope(R, owner);
    } catch (failure) {
      try {
        unloaded(R, loading);
      } catch (error) {
        later(R, error);
      }
      throw failure;
    }
    unloaded(R, loading);
  }
}

/**
 * Work in flight ended outside a flush (a stop or a sweep freed the last
 * pending task or async memo): under `$auto`, the `$settled` callbacks that
 * waited for it run now, as a completion's would.
 * @param {Graph} R @param {number} loading the pending count before
 */
function unloaded(R, loading) {
  if (loading > 0 && R.loading === 0 && R.auto && !R.closed && R.settled.length > 0) automatic(R);
}

// ---------------------------------------------------------------------------
// Background disposal. `$retire` makes a scope inert at once; `$sweep` frees it
// later, in slices bounded by time, so a large teardown does not stall a
// frame. Inertness is checked where work would run (an effect, an
// application, a `$commit` hook, a memo recompute) only while retired scopes
// wait for a sweep (R.retiring), through a per-scope cache of the root's
// retire epoch: one comparison while nothing was retired since the scope was
// last seen live, an ancestor walk once per scope per epoch.

/**
 * True when `scope` or an ancestor was retired or stopped. A live answer is
 * cached on the scope until the next `$retire`.
 * @param {Graph} R @param {ScopeNode} scope
 */
function inert(R, scope) {
  const epoch = R.retires;
  for (let s = scope; s !== null; s = s.owner) {
    if (s.live !== ALIVE) return true;
    if (s.seen === epoch || s === R.home) break;
  }
  scope.seen = epoch;
  return false;
}

/**
 * Makes a scope and everything it owns inert at once, in O(1): no effect,
 * memo, Watch or Render reruns and no `$commit` hook of it runs again, whatever
 * is written later, and `$alive(scope)` is false. The nodes are freed and the
 * cleanups run later, in the same order as `$stop` (owned nodes newest first,
 * then the scope's own cleanups newest first), by `$sweep`. `$stop(scope)` or
 * `$dispose(root)` finishes the teardown at once. A retired memo read from
 * outside keeps its last value until it is freed.
 * @param {Owner} owner
 */
export function $retire(owner) {
  const R = owner.root;
  if (R.closed || owner.live !== ALIVE || owner === R.home) return;
  owner.live = RETIRED;
  R.retires++;
  R.retiring = true;
  const idle = R.sweepTop === null && R.retiredAt === R.retired.length;
  R.retired.push(owner);
  if (idle) schedule(R);
}

/** Retired work appeared: the host's scheduler, or the default one, is told once. @param {Graph} R */
function schedule(R) {
  const sweeper = R.sweeper;
  if (sweeper === undefined) idle(R);
  else if (sweeper !== null) sweeper(R);
}

/**
 * The default drain. Where the host has idle callbacks (a browser's
 * requestIdleCallback), each slice takes the idle time left, at most IDLE
 * ms; one that waited WAIT.timeout ms without an idle period takes BUSY ms,
 * so the work still finishes on a page that is never idle. Elsewhere, 4 ms
 * slices on macrotasks (setTimeout).
 * @param {Graph} R
 */
function idle(R) {
  const host = /** @type {any} */ (globalThis);
  if (typeof host.requestIdleCallback === 'function') {
    /** @param {{ didTimeout: boolean, timeRemaining: () => number }} deadline */
    const slice = deadline => {
      // A slice that raises a cleanup failure still schedules the next one.
      let left = 1;
      try {
        left = $sweep(R, deadline.didTimeout ? BUSY : Math.min(IDLE, deadline.timeRemaining()));
      } finally {
        // timer: feature, the next idle slice while work is left.
        if (left > 0) host.requestIdleCallback(slice, WAIT);
      }
    };
    // timer: feature, sweeping in the host's idle time is the behaviour.
    host.requestIdleCallback(slice, WAIT);
  } else if (typeof setTimeout === 'function') {
    // Macrotask slices where the host has no idle callbacks.
    const slice = () => {
      let left = 1;
      try {
        left = $sweep(R, 4);
      } finally {
        if (left > 0) setTimeout(slice, 0); // timer: fallback
      }
    };
    setTimeout(slice, 0); // timer: fallback
  }
}

/**
 * Installs the host's sweep scheduler: `schedule(root)` is called when
 * retired work appears after none was left, and the host then calls
 * `$sweep(root, budget)` from idle time or between frames until it returns 0.
 * Null leaves the sweeping to explicit `$sweep` calls. Without a scheduler,
 * rrx-core sweeps in idle periods where the host has idle callbacks, else in
 * 4 ms macrotask slices.
 * @param {Graph} root
 * @param {((root: Graph) => void) | null} scheduler
 */
export function $sweeper(root, scheduler) {
  root.sweeper = scheduler;
}

/** The number of retired scopes not yet freed. @param {Graph} R */
function pending(R) {
  let n = R.retired.length - R.retiredAt;
  if (R.sweepTop !== null) n++;
  return n;
}

/**
 * Frees retired work for about `budget` milliseconds (default 4) and returns
 * how many retired scopes remain (0 when done). The clock is read after every
 * chunk of about 0.05 ms of work, sized from the sweep's running cost
 * estimate (an average over recent chunks), so a slice overshoots its budget
 * by about one chunk. Every call frees at least one chunk (no starvation); a
 * root with effects or applications queued gets only that, and a call inside
 * a flush does nothing. A cleanup that throws goes to the nearest `$catch` and
 * the sweep goes on; the first unhandled failure is raised when the slice
 * ends. Under `$auto`, a slice that frees the last pending task flushes, so
 * the `$settled` callbacks run (as `$stop` does).
 * @param {Graph} root
 * @param {number} [budget]
 * @returns {number}
 */
export function $sweep(root, budget = 4) {
  const R = root;
  if (R.closed) return 0;
  if (R.flushing) return pending(R);
  const clock = performance;
  let t = clock.now();
  // Queued work preempts the sweep: one chunk, then back to the host.
  const deadline = t + (busy(R) ? 0 : budget);
  const loading = R.loading;
  let cost = R.sweepCost;
  R.sweepFailure = HANDLED;
  for (;;) {
    const size = cost > 0 ? Math.max(1, Math.min(CHUNK, Math.floor(GRAIN / cost))) : CHUNK;
    let done = 0;
    let unit = 1;
    while (done < size && (unit = step(R)) > 0) done += unit;
    const now = clock.now();
    if (done > 0) cost = 0.75 * cost + 0.25 * ((now - t) / done);
    t = now;
    if (unit === 0 || now >= deadline) break;
  }
  R.sweepCost = cost;
  const failure = R.sweepFailure;
  R.sweepFailure = HANDLED;
  if (failure !== HANDLED) {
    try {
      unloaded(R, loading);
    } catch (error) {
      later(R, error);
    }
    throw failure;
  }
  unloaded(R, loading);
  return pending(R);
}

/**
 * One unit of sweeping, in `$stop`'s order: the scope being swept frees its
 * owned nodes newest first, descending into child scopes, then runs its own
 * cleanups. Returns the units done (a released scope counts its cleanups),
 * or 0 when nothing is left. A scope stopped meanwhile is dropped.
 * @param {Graph} R
 */
function step(R) {
  let top = R.sweepTop;
  if (top !== null && top.live !== RETIRED) top = R.sweepTop = R.sweepCur = null;
  if (top === null) {
    const q = R.retired;
    while (R.retiredAt < q.length) {
      const s = q[R.retiredAt];
      q[R.retiredAt++] = null;
      if (s !== null && s.live === RETIRED) {
        top = s;
        break;
      }
    }
    if (top === null) {
      q.length = 0;
      R.retiredAt = 0;
      R.retiring = false;
      return 0;
    }
    R.sweepTop = R.sweepCur = top;
  }
  const cur = /** @type {ScopeNode} */ (R.sweepCur);
  const child = cur.tail;
  if (child !== null) {
    if (child.k === SCOPE) {
      const scope = /** @type {ScopeNode} */ (child);
      scope.live = CLOSING;
      if (scope.tail !== null) {
        R.sweepCur = scope;
        return 1;
      }
      return 1 + sweepRelease(R, scope, scope);
    }
    return 1 + sweepRelease(R, child, cur);
  }
  if (cur === top) R.sweepTop = R.sweepCur = null;
  else R.sweepCur = cur.owner;
  return 1 + sweepRelease(R, cur, cur);
}

/**
 * Releases one swept node or scope; a failure goes to the nearest `$catch` from
 * `at`, and the first unhandled one is kept for the end of the slice.
 * Returns the number of cleanups a scope ran.
 * @param {Graph} R @param {Node<any> | ScopeNode} x @param {ScopeNode} at
 */
function sweepRelease(R, x, at) {
  let cleanups = 0;
  if (x.k === SCOPE) {
    const list = /** @type {ScopeNode} */ (x).cleanup;
    if (list !== null) cleanups = list.length;
  }
  try {
    release(R, x);
  } catch (error) {
    const left = deliver(R, at, error);
    if (left !== HANDLED && R.sweepFailure === HANDLED) R.sweepFailure = left;
  }
  return cleanups;
}

/** @param {Graph} R @param {ScopeNode} top */
function stopScope(R, top) {
  /** @type {any} */
  let failure = HANDLED;
  top.live = CLOSING;
  /** @type {ScopeNode} */
  let cur = top;
  for (;;) {
    const child = cur.tail;
    if (child !== null) {
      if (child.k === SCOPE) {
        const scope = /** @type {ScopeNode} */ (child);
        scope.live = CLOSING;
        if (scope.tail !== null) {
          cur = scope;
          continue;
        }
      } else if (child.live === ALIVE && child.cleanup === undefined && !R.closing) {
        // A plain node (not running, no cleanup to run) is freed here, as the
        // tail of its owner's list; release would do the same through calls.
        const n = /** @type {Node<any>} */ (child);
        if (n.extra !== null && n.extra.flight !== NONE) superseded(R, n);
        n.mode = 0;
        n.depsTail = null;
        if (n.deps !== null) purge(R, n);
        if (n.subs !== null) orphan(R, n);
        const p = n.prev;
        cur.tail = p;
        if (p !== null) p.next = null;
        else cur.head = null;
        n.prev = null;
        n.live = FREED;
        continue;
      }
      try {
        release(R, child);
      } catch (error) {
        if (failure === HANDLED) failure = error;
      }
      continue;
    }
    if (cur === top) break;
    const parent = cur.owner;
    try {
      release(R, cur);
    } catch (error) {
      if (failure === HANDLED) failure = error;
    }
    cur = parent;
  }
  try {
    release(R, top);
  } catch (error) {
    if (failure === HANDLED) failure = error;
  }
  if (failure !== HANDLED) throw failure;
}

/**
 * Releases one owned node whose own children are gone: a scope runs its
 * cleanups, an effect is cancelled, a signal or memo is freed. A running node
 * moves to the root's list and is freed when its run returns.
 * @param {Graph} R @param {Node<any> | ScopeNode} x
 */
function release(R, x) {
  if (x.k === SCOPE) return releaseScope(R, /** @type {ScopeNode} */ (x));
  const n = /** @type {Node<any>} */ (x);
  if ((n.live & 1) !== 0) closing(R, n);
  else if (n.k === EFFECT && n.live === ALIVE) cancel(n);
  else free(R, n);
}

/**
 * A node released during its own run moves to the root and is freed when the
 * run returns. While the root itself is disposed it only leaves its list:
 * the root's list is being emptied, and moving it there would be walked
 * again, for ever; its run's epilogue still frees it (from the empty root).
 * @param {Graph} R @param {Node<any>} n
 */
function closing(R, n) {
  leave(n);
  n.owner = R.home;
  if (!R.closing) adopt(R.home, n);
  if (n.k === EFFECT) $cancel(n);
  else n.live = CLOSING;
}

/** A scope whose children are gone runs its cleanups, newest first. @param {Graph} R @param {ScopeNode} scope */
function releaseScope(R, scope) {
  const list = scope.cleanup;
  scope.cleanup = null;
  if (scope.rare !== null) scope.rare.mounts = null;
  /** @type {any} */
  let failure = HANDLED;
  if (list !== null) {
    for (let i = list.length - 1; i >= 0; i--) {
      const fn = list[i];
      if (fn == null) continue;
      try {
        fn();
      } catch (error) {
        if (failure === HANDLED) failure = error;
      }
    }
  }
  if (scope === R.home) scope.live = ALIVE;
  else {
    leave(scope);
    scope.live = FREED;
  }
  if (failure !== HANDLED) throw failure;
}

// An owner's cleanup list: `$release` empties a slot; once released slots
// outnumber live ones (past a floor), the list is compacted, keeping order.
// A key is a slot's index until the list is first compacted; from then on
// the list's book keeps each slot's key (increasing, so found by search),
// so a key given out before stays good.

/** The book of a cleanup list that had a slot released. @type {WeakMap<any[], { released: number, keys: number[] | null, next: number }>} */
const books = new WeakMap();

/** Appends a cleanup; returns its key. @param {any[]} list @param {() => void} callback */
function cleanupKey(list, callback) {
  list.push(callback);
  const book = books.get(list);
  if (book === undefined || book.keys === null) return list.length - 1;
  book.keys.push(book.next);
  return book.next++;
}

/** Empties the slot of `key`, and compacts the list once it is mostly empty. @param {any[]} list @param {number} key */
function dropCleanup(list, key) {
  let book = books.get(list);
  let at = key;
  if (book !== undefined && book.keys !== null) {
    const keys = book.keys;
    let lo = 0;
    let hi = keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (keys[mid] < key) lo = mid + 1;
      else hi = mid;
    }
    if (lo === keys.length || keys[lo] !== key) return;
    at = lo;
  }
  if (!(at >= 0 && at < list.length) || list[at] == null) return;
  list[at] = null;
  if (book === undefined) books.set(list, (book = { released: 0, keys: null, next: 0 }));
  if (++book.released <= 16 || book.released * 2 <= list.length) return;
  if (book.keys === null) {
    book.keys = Array.from({ length: list.length }, (_, i) => i);
    book.next = list.length;
  }
  const keys = book.keys;
  let k = 0;
  for (let i = 0; i < list.length; i++) {
    if (list[i] == null) continue;
    list[k] = list[i];
    keys[k] = keys[i];
    k++;
  }
  list.length = k;
  keys.length = k;
  book.released = 0;
}

/**
 * Registers a cleanup run when `owner` stops; returns its key, or null when
 * the owner already stopped.
 * @param {Owner} owner
 * @param {() => void} callback
 * @returns {number | null}
 */
export function $cleanup(owner, callback) {
  if (owner.live !== ALIVE) return null;
  let list = owner.cleanup;
  if (list === null) list = owner.cleanup = [];
  return cleanupKey(list, callback);
}

/**
 * Forgets a registered cleanup, for a resource that ended before its owner.
 * @param {Owner} owner
 * @param {number | null} key
 */
export function $release(owner, key) {
  if (key == null || (owner.live !== ALIVE && owner.live !== RETIRED)) return;
  const list = owner.cleanup;
  if (list !== null) dropCleanup(list, key);
}

/** The number of live cleanups of an owner. @param {Owner} owner */
export function $cleanups(owner) {
  let n = 0;
  if ((owner.live === ALIVE || owner.live === RETIRED) && owner.cleanup !== null) for (const fn of owner.cleanup) if (fn != null) n++;
  return n;
}

/**
 * Registers deferred initialization that the host drains with `$mounts`.
 * @param {Owner} owner
 * @param {() => void} callback
 */
export function $mount(owner, callback) {
  if (owner.live !== ALIVE) return;
  const r = rare(owner);
  if (r.mounts === null) r.mounts = [];
  r.mounts.push(callback);
}

/** The host schedules this at its post-mount boundary. @param {Owner} owner */
export function $mounts(owner) {
  const r = owner.rare;
  if (r === null) return;
  const list = r.mounts;
  r.mounts = null;
  if (list !== null) for (const callback of list) if (owner.live === ALIVE) $enter(owner, callback);
}

/** True while a scope and its ancestors are live. @param {Owner | null} owner */
export function $alive(owner) {
  if (owner == null || owner.root.closed) return false;
  for (let s = owner; s !== owner.root.home; s = s.owner) if (s.live !== ALIVE) return false;
  return true;
}

/**
 * The current owner, or null at the root.
 * @param {Graph} root
 * @returns {Owner | null}
 */
export function $owner(root) {
  return root.owner === root.home ? null : root.owner;
}

/**
 * A hook run after each flush round until its owner stops. Returns its key.
 * @param {Graph} root
 * @param {Owner | null} owner
 * @param {() => void} callback
 */
export function $commit(root, owner, callback) {
  live(root);
  /** @type {{ run: (() => void) | null, owner: ScopeNode }} */
  const entry = { run: callback, owner: owner ?? root.home };
  root.commits.push(entry);
  root.commitCount++;
  if (owner != null)
    $cleanup(owner, () => {
      if (entry.run !== null) {
        entry.run = null;
        root.commitCount--;
        // Outside a flush no round may come to compact the list: do it here (amortized, once most are dead).
        if (!root.flushing) compactCommits(root);
      }
    });
  return ++root.commitKeys;
}

/**
 * Installs the error boundary of a scope, or of the root.
 * @param {Owner | Graph} owner
 * @param {((error: any) => void) | null} handler
 */
export function $catch(owner, handler) {
  if (owner instanceof Graph) owner.errorHandler = handler;
  else if (owner.live === ALIVE) rare(owner).handler = handler;
}

/**
 * Offers an error to the nearest handling owner, then the root handler.
 * Returns null when handled, otherwise the error that remains unhandled.
 * @param {Graph} root
 * @param {Owner | null} owner
 * @param {any} error
 */
export function $deliver(root, owner, error) {
  const left = deliver(root, owner == null ? root.home : owner, error);
  return left === HANDLED ? null : left;
}

/**
 * Delivers an error to the current owner's boundary; raises it if unhandled.
 * @param {Graph} root
 * @param {any} error
 */
export function $throw(root, error) {
  const left = deliver(root, root.owner, error);
  if (left !== HANDLED) throw left;
}

// ---------------------------------------------------------------------------
// Contexts (the key, descriptor and layer types live in context.js)

/** @param {Graph | null | undefined} root @returns {Layer | null} */
function chain(root) {
  const R = root ?? running;
  return R === null ? null : R.ctx;
}

/** A provider's value: a writable derivation of an accessor or node, or a signal. @param {Graph} root @param {any} value */
function cell(root, value) {
  if (typeof value === 'function') return $derive(root, value);
  if (value instanceof Node) return $derive(root, () => $read(value));
  return $signal(root, value);
}

/**
 * Provides `value` for `context` to everything `build` creates, across
 * scopes, lists, branches and lazy components; nested providers shadow outer
 * ones. The value lives in a signal (a writable derivation when `value` is an
 * accessor or node), owned by the current owner, so `$update` under this
 * provider writes it and only consumers below it follow.
 * @template T, U
 * @param {Graph} root
 * @param {Context<T>} context
 * @param {T | (() => T) | Readable<T>} value
 * @param {() => U} build
 * @returns {U}
 */
export function $provide(root, context, value, build) {
  live(root);
  const layer = new Layer(root.ctx, context, cell(root, value), undefined, true);
  const previous = root.ctx;
  root.ctx = layer;
  let scope;
  try {
    scope = $scope(root, $owner(root));
  } finally {
    root.ctx = previous;
  }
  return $enter(scope, build);
}

/**
 * The nearest provided value of `context`, or its fallback. Inside a
 * computation the read tracks the provider, so `$update` reaches it.
 * `$lookup(root, context)` names the root; `$lookup(context)` uses the root
 * whose code is running (inside `$enter`, `$with`, a flush or a component).
 * @template T
 * @overload
 * @param {Graph} root
 * @param {Context<T>} context
 * @returns {T}
 */
/**
 * @template T
 * @overload
 * @param {Context<T>} context
 * @returns {T}
 */
/**
 * @param {any} a @param {any} [b]
 * @returns {any}
 */
export function $lookup(a, b) {
  const rooted = a instanceof Graph;
  const context = rooted ? b : a;
  const layer = nearest(chain(rooted ? a : null), context);
  if (layer === null) return context.fallback;
  const node = layer.node;
  if (node === null) return layer.value;
  // Where nothing tracks (a component body that counted runs), a lookup
  // reads the value it is built with: not a reactive read gone stale.
  if (counting !== null && node.root.current === counting) return $untrack(node.root, () => $read(node));
  return $read(node);
}

/**
 * Writes the nearest provider of `context` (a value, or a function of the
 * previous one); consumers below that provider update at the next flush, and
 * consumers elsewhere are unaffected. Throws where nothing provides it, or
 * when the nearest layer is a built-in, read-only one.
 * `$update(root, context, value)` names the root; `$update(context, value)`
 * uses the running root.
 * @template T
 * @overload
 * @param {Graph} root
 * @param {Context<T>} context
 * @param {T | ((previous: T) => T)} value
 * @returns {T}
 */
/**
 * @template T
 * @overload
 * @param {Context<T>} context
 * @param {T | ((previous: T) => T)} value
 * @returns {T}
 */
/**
 * @param {any} a @param {any} b @param {any} [c]
 * @returns {any}
 */
export function $update(a, b, c) {
  const rooted = a instanceof Graph;
  const context = rooted ? b : a;
  const value = rooted ? c : b;
  const layer = nearest(chain(rooted ? a : null), context);
  if (layer === null) throw new Error('RRX: nothing provides this context here');
  const node = layer.node;
  if (!layer.writable || node === null) throw new Error('RRX: this context is read-only here');
  const next = typeof value === 'function' ? value($untrack(node.root, () => $read(node))) : value;
  return $write(node, next);
}

/**
 * The visible context, nearest first: one `[context, value]` pair per
 * context (shadowed providers are left out), read untracked. For tools and
 * debugging. `$entries(root)` names the root; `$entries()` uses the running root.
 * @param {Graph | null} [root]
 * @returns {[Context<any>, any][]}
 */
export function $entries(root) {
  const R = root ?? running;
  /** @type {[Context<any>, any][]} */
  const out = [];
  if (R === null) return out;
  const seen = new Set();
  for (let c = R.ctx; c != null; c = c.parent) {
    if (seen.has(c.key)) continue;
    seen.add(c.key);
    const node = c.node;
    out.push([c.key, node === null ? c.value : $untrack(R, () => $read(node))]);
  }
  return out;
}

/**
 * Runs `callback` with a captured context layer (from `$current`) current, as
 * for a callback that runs later; scopes created inside capture it.
 * @template T
 * @param {Graph} root
 * @param {Layer | null} context
 * @param {() => T} callback
 * @returns {T}
 */
export function $with(root, context, callback) {
  const previous = root.ctx;
  const was = running;
  root.ctx = context;
  running = root;
  try {
    return callback();
  } finally {
    root.ctx = previous;
    running = was;
  }
}

/** The current context layer, for `$with`. @param {Graph} root @returns {Layer | null} */
export function $current(root) {
  return root.ctx;
}

/** Runs `fn` with computations allowed to write (derived internal state). @template T @param {Graph} R @param {() => T} fn @returns {T} */
export function lift(R, fn) {
  const pure = R.pure;
  R.pure = false;
  try {
    return fn();
  } finally {
    R.pure = pure;
  }
}


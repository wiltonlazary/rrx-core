// rrx-core arena: the core algorithm of core.js over a column layout, with the
// same operations and contract.
//
// A root stores its graph as columns indexed by integer node id, with
// dependency and subscriber lists linked through integer link ids. Node state
// lives in small integer columns (kinds, state, queued, live, running, ready,
// writable, permit), and ids are recycled with a generation. Handles are
// fixed-shape records { kind, root, id, gen, last }; no graph state lives on
// a handle. Marking and checking are iterative with explicit stacks, effect
// queues are integer vectors sorted by creation order, and a rerun that reads
// the same sources in the same order allocates nothing.

// ---------------------------------------------------------------------------
// Constants

/** Node kinds. SCOPE is this library's: scopes are arena nodes (README). */
export const SIGNAL = /** @type {number} */ (1);
export const MEMO = /** @type {number} */ (2);
export const EFFECT = /** @type {number} */ (3);
export const PROBE = /** @type {number} */ (4);
export const SCOPE = /** @type {number} */ (5);

/** Node states. */
export const CLEAN = /** @type {number} */ (0);
export const PENDING = /** @type {number} */ (1);
export const DIRTY = /** @type {number} */ (2);

/** Queue lanes. */
export const URGENT = /** @type {number} */ (1);
export const LOW = /** @type {number} */ (2);

/**
 * Readiness of a memo's result: none yet, a value, a failure, or a pending
 * read, so a clean read tests one integer.
 */
export const NONE = /** @type {number} */ (0);
export const VALUE = /** @type {number} */ (1);
export const FAILURE = /** @type {number} */ (2);
export const WAITS = /** @type {number} */ (3);

/** Liveness: a closing node is freed when its running computation returns. */
export const FREED = /** @type {number} */ (0);
export const ALIVE = /** @type {number} */ (1);
export const CLOSING = /** @type {number} */ (2);
/** A scope made inert by `$retire`, waiting for a `$sweep` to free it. */
export const RETIRED = /** @type {number} */ (3);

/** `$sweep`: the clock is read every chunk of this many milliseconds of estimated work. */
const GRAIN = 0.05;
/** The most units between two clock reads. */
const CHUNK = 4096;
/** The default drain's slices: at most IDLE ms of an idle period, BUSY ms after waiting WAIT.timeout ms for one. */
const IDLE = 8;
const BUSY = 2;
const WAIT = { timeout: 100 };

/** Watch phases (this library's column-based Watch and Render). */
const RENDERING = 1;
const WATCHING = 2;

/** An effect running more often than this within one flush is a cycle. */
const LIMIT = 100;
/** `$commit` hooks that keep the graph busy for this many rounds are a cycle. */
const ROUNDS = 100;

const names = /** @type {const} */ (['', 'signal', 'memo', 'effect', 'probe', 'scope']);

/** Values `equals` may hold. @typedef {((previous: any, next: any) => boolean) | false | null | undefined} Equality */

import { $context, Provided, Layer, checkId, nearest } from './context.js';

/**
 * @template T
 * @typedef {import('./context.js').Context<T>} Context
 */

export { $context, Provided };

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
/** Takes the state the latest pending read waited on, clearing the register (core.js). */
export function waitedOn() {
  const state = waited;
  waited = null;
  return state;
}

/** The root whose code is on the stack (core.js). @type {Graph | null} */
let running = null;

// ---------------------------------------------------------------------------
// Handles

/**
 * A reference to a node. `gen` is the generation of the id the handle was made
 * for; once the node is freed, `last` holds its final value and the handle
 * reads it untracked, as a collected node would behave in a GC design.
 * @template [T=unknown]
 */
export class Handle {
  /**
   * @param {'signal' | 'memo' | 'effect' | 'probe' | 'scope'} kind
   * @param {Graph} root
   * @param {number} id
   * @param {number} gen
   */
  constructor(kind, root, id, gen) {
    this.kind = kind;
    this.root = root;
    this.id = id;
    this.gen = gen;
    /** @type {any} */
    this.last = null;
  }
}

/** @param {Int32Array} a @param {number} n */
const widen = (a, n) => {
  const b = new Int32Array(n);
  b.set(a);
  return b;
};
/** @param {Float64Array} a @param {number} n */
const widen64 = (a, n) => {
  const b = new Float64Array(n);
  b.set(a);
  return b;
};

/** A stack or queue of ints with a separate count; slots are reused. */
class Vec {
  constructor(size = 16) {
    this.a = new Int32Array(size);
    this.n = 0;
  }
  /** @param {number} v */
  push(v) {
    if (this.n === this.a.length) this.a = widen(this.a, this.n * 2);
    this.a[this.n++] = v;
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
    /** The root's id. */
    this.id = id;
    const n = 64;
    this.cap = n;
    this.count = 1;
    // Node columns; index 0 is the absent node (and the root's own scope).
    this.kinds = new Int32Array(n);
    this.state = new Int32Array(n);
    this.queued = new Int32Array(n);
    this.running = new Int32Array(n);
    this.live = new Int32Array(n);
    this.writable = new Int32Array(n);
    this.permit = new Int32Array(n);
    this.ready = new Int32Array(n);
    this.version = new Int32Array(n);
    this.deps = new Int32Array(n);
    this.depsTail = new Int32Array(n);
    this.subs = new Int32Array(n);
    this.subsTail = new Int32Array(n);
    this.watchers = new Int32Array(n);
    this.gen = new Int32Array(n);
    this.order = new Float64Array(n);
    this.runs = new Int32Array(n);
    this.serial = new Int32Array(n);
    // Ownership: the owning scope and each scope's child list (README).
    this.owners = new Int32Array(n);
    this.head = new Int32Array(n);
    this.tail = new Int32Array(n);
    this.prev = new Int32Array(n);
    this.next = new Int32Array(n);
    // Column-based Watch and Render (README): phase, queued application lane
    // and whether it applied once.
    this.phase = new Int32Array(n);
    this.applying = new Int32Array(n);
    this.applied = new Int32Array(n);
    // The root's retire epoch when a scope and its ancestors were last seen live (see inert).
    this.seen = new Int32Array(n);
    // Boxed columns
    /** @type {any[]} */ this.value = new Array(n);
    /** @type {any[]} */ this.run = new Array(n);
    /** @type {Equality[]} */ this.equals = new Array(n);
    /** @type {any[]} */ this.failure = new Array(n);
    /** @type {any[]} */ this.waits = new Array(n);
    /** @type {any[]} */ this.cleanup = new Array(n);
    /** @type {any[]} */ this.hook = new Array(n);
    /** @type {any[]} */ this.apply = new Array(n);
    /** @type {any[]} */ this.shown = new Array(n);
    /** @type {any[]} */ this.context = new Array(n);
    /** @type {any[]} */ this.handler = new Array(n);
    /** @type {any[]} */ this.mounts = new Array(n);
    /** @type {(Handle<any> | null)[]} */ this.handle = new Array(n);
    this.freeNodes = new Vec();
    /**
     * Freed ids held back while a queue may still hold them (an effect,
     * render or apply queue is not empty, or a walk of one is on): reused
     * only once no stale entry can name them, so a new node never runs in a
     * gone one's queue place.
     */
    this.parked = new Vec();
    // Link columns; index 0 is the absent link.
    const m = 128;
    this.lcap = m;
    this.lcount = 1;
    this.ldep = new Int32Array(m);
    this.lsub = new Int32Array(m);
    this.lprevDep = new Int32Array(m);
    this.lnextDep = new Int32Array(m);
    this.lprevSub = new Int32Array(m);
    this.lnextSub = new Int32Array(m);
    this.lseen = new Int32Array(m);
    this.lcycle = new Int32Array(m);
    this.freeLinks = new Vec();
    this.liveNodes = 0;
    this.liveLinks = 0;
    // Work stacks, reused: check frames (node, cursor), marking, watchers,
    // and whenUnwatched hooks waiting for a safe point.
    this.sNode = new Int32Array(64);
    this.sLink = new Int32Array(64);
    this.sp = 0;
    this.mark = new Vec();
    this.watch = new Vec();
    /** @type {(() => void)[]} */ this.hooks = [];
    // Queues
    this.urgent = new Vec();
    this.low = new Vec();
    this.spare = new Vec();
    /** How many entries of the batch a lane walk holds in `spare` (0 when none). */
    this.walking = 0;
    this.renders = new Vec();
    this.applies = new Vec();
    this.spareApply = new Vec();
    /** @type {(() => void)[]} */ this.after = [];
    /** @type {(() => void)[]} */ this.settled = [];
    this.loading = 0;
    // Tracking and scheduling
    this.current = 0;
    this.cycle = 0;
    this.cycles = 0;
    this.nextOrder = 0;
    this.versionAll = 0;
    this.effects = 0;
    this.pure = false;
    this.auto = false;
    this.closed = false;
    this.closing = false;
    this.flushing = false;
    this.batching = 0;
    this.serialAll = 0;
    this.transition = 0;
    /** The memos `hasten` walked since the low queue last grew, as core.js. @type {Set<number>} */
    this.hastened = new Set();
    this.lane = URGENT;
    this.scopes = 0;
    /** `$commit` hooks; one whose owner stopped has a null `run` until the list is compacted. @type {{ run: (() => void) | null, owner: number }[]} */ this.commits = [];
    this.commitCount = 0;
    this.commitKeys = 0;
    this.owner = 0;
    /** The current context layer. @type {Layer | null} */ this.ctx = null;
    /** @type {((error: any) => void) | null} */ this.errorHandler = null;
    /** The first unhandled failure of the running flush, raised when it ends; HANDLED when none. @type {any} */ this.failed = HANDLED;
    // Background disposal (`$retire`, `$sweep`): the retire epoch, the queue of
    // retired scopes as (id, generation) pairs, the scope being swept (its
    // generation) and its cursor, the cost estimate in milliseconds per
    // unit, and the host's scheduler.
    this.retires = 0;
    /** True while retired scopes wait for a sweep: only then is inertness checked. */
    this.retiring = false;
    /** @type {number[]} */ this.retired = [];
    this.retiredAt = 0;
    this.sweepTop = 0;
    this.sweepGen = 0;
    this.sweepCur = 0;
    this.sweepCost = 0.0002;
    /** @type {((root: Graph) => void) | null | undefined} */ this.sweeper = undefined;
    /** The first unhandled failure of the current `$sweep` slice. @type {any} */ this.sweepFailure = HANDLED;
    this.kinds[0] = SCOPE;
    this.live[0] = ALIVE;
  }
}

/** @param {Graph} R */
function grow(R) {
  const n = R.cap * 2;
  R.cap = n;
  R.kinds = widen(R.kinds, n);
  R.state = widen(R.state, n);
  R.queued = widen(R.queued, n);
  R.running = widen(R.running, n);
  R.live = widen(R.live, n);
  R.writable = widen(R.writable, n);
  R.permit = widen(R.permit, n);
  R.ready = widen(R.ready, n);
  R.version = widen(R.version, n);
  R.deps = widen(R.deps, n);
  R.depsTail = widen(R.depsTail, n);
  R.subs = widen(R.subs, n);
  R.subsTail = widen(R.subsTail, n);
  R.watchers = widen(R.watchers, n);
  R.gen = widen(R.gen, n);
  R.order = widen64(R.order, n);
  R.runs = widen(R.runs, n);
  R.serial = widen(R.serial, n);
  R.owners = widen(R.owners, n);
  R.head = widen(R.head, n);
  R.tail = widen(R.tail, n);
  R.prev = widen(R.prev, n);
  R.next = widen(R.next, n);
  R.phase = widen(R.phase, n);
  R.applying = widen(R.applying, n);
  R.applied = widen(R.applied, n);
  R.seen = widen(R.seen, n);
}

/** @param {Graph} R */
function growLinks(R) {
  const n = R.lcap * 2;
  R.lcap = n;
  R.ldep = widen(R.ldep, n);
  R.lsub = widen(R.lsub, n);
  R.lprevDep = widen(R.lprevDep, n);
  R.lnextDep = widen(R.lnextDep, n);
  R.lprevSub = widen(R.lprevSub, n);
  R.lnextSub = widen(R.lnextSub, n);
  R.lseen = widen(R.lseen, n);
  R.lcycle = widen(R.lcycle, n);
}

/** Creates a root named `id` (`[a-z][a-z0-9-]{0,31}`): one graph with its own ownership, queues, flush and contexts. @param {string} id @returns {Graph} */
export function $root(id) {
  return new Graph(checkId(id));
}

/** @param {Graph} R */
export function live(R) {
  if (R.closed) throw new Error('RRX: disposed root');
}

/**
 * A node starts in the given kind and state, owned by `owner`.
 * @param {Graph} R @param {number} kind @param {number} state @param {number} owner
 */
function allocate(R, kind, state, owner) {
  live(R);
  let id;
  if (R.freeNodes.n > 0) id = R.freeNodes.a[--R.freeNodes.n];
  else {
    id = R.count++;
    if (id >= R.cap) grow(R);
  }
  R.kinds[id] = kind;
  R.state[id] = state;
  R.live[id] = ALIVE;
  R.order[id] = ++R.nextOrder;
  R.owners[id] = owner;
  const last = R.tail[owner];
  R.prev[id] = last;
  R.next[id] = 0;
  if (last !== 0) R.next[last] = id;
  else R.head[owner] = id;
  R.tail[owner] = id;
  R.liveNodes++;
  R.handle[id] = new Handle(/** @type {Exclude<typeof names[number], "">} */ (names[kind]), R, id, R.gen[id]);
  return id;
}

/** The live id of a handle, or 0 once its node was freed. @param {Handle<any>} handle */
export function node(handle) {
  const R = handle.root;
  live(R);
  const id = handle.id;
  return R.gen[id] !== handle.gen || R.live[id] === FREED ? 0 : id;
}

// ---------------------------------------------------------------------------
// Links

/** @param {Graph} R @param {number} dep @param {number} sub */
function newLink(R, dep, sub) {
  let l;
  if (R.freeLinks.n > 0) l = R.freeLinks.a[--R.freeLinks.n];
  else {
    l = R.lcount++;
    if (l >= R.lcap) growLinks(R);
  }
  R.ldep[l] = dep;
  R.lsub[l] = sub;
  R.lseen[l] = R.version[dep];
  R.lcycle[l] = R.cycle;
  R.liveLinks++;
  return l;
}

/** @param {Graph} R @param {number} l */
function freeLink(R, l) {
  R.ldep[l] = 0;
  R.lsub[l] = 0;
  R.liveLinks--;
  R.freeLinks.push(l);
}

/** Subscriber lists: every link is in its source's list, observed or not. @param {Graph} R @param {number} dep @param {number} l */
function appendSub(R, dep, l) {
  const last = R.subsTail[dep];
  R.lprevSub[l] = last;
  R.lnextSub[l] = 0;
  if (last === 0) R.subs[dep] = l;
  else R.lnextSub[last] = l;
  R.subsTail[dep] = l;
}

/** @param {Graph} R @param {number} l */
function removeSub(R, l) {
  const dep = R.ldep[l];
  const before = R.lprevSub[l];
  const after = R.lnextSub[l];
  if (before === 0) R.subs[dep] = after;
  else R.lnextSub[before] = after;
  if (after === 0) R.subsTail[dep] = before;
  else R.lprevSub[after] = before;
}

/** @param {Graph} R @param {number} l */
function removeDep(R, l) {
  const sub = R.lsub[l];
  const before = R.lprevDep[l];
  const after = R.lnextDep[l];
  if (before === 0) R.deps[sub] = after;
  else R.lnextDep[before] = after;
  if (after !== 0) R.lprevDep[after] = before;
  if (R.depsTail[sub] === l) R.depsTail[sub] = before;
}

/** An effect is always attached; a memo while something watches it. @param {Graph} R @param {number} id */
function attached(R, id) {
  const kind = R.kinds[id];
  return kind === EFFECT ? R.live[id] !== FREED : kind === MEMO ? R.watchers[id] > 0 : false;
}

/** A memo gaining its first watcher adds one to each of its sources in turn. @param {Graph} R @param {number} dep */
function watch(R, dep) {
  const w = R.watch;
  const base = w.n;
  w.push(dep);
  while (w.n > base) {
    const id = w.a[--w.n];
    const count = ++R.watchers[id];
    if (count === 1 && R.kinds[id] === MEMO) {
      R.effects++;
      for (let l = R.deps[id]; l !== 0; l = R.lnextDep[l]) w.push(R.ldep[l]);
    }
  }
}

/**
 * The last watcher leaving releases a memo's sources; a signal runs its hook
 * once the lists are consistent again. @param {Graph} R @param {number} dep
 */
function unwatch(R, dep) {
  const w = R.watch;
  const base = w.n;
  w.push(dep);
  while (w.n > base) {
    const id = w.a[--w.n];
    const count = --R.watchers[id];
    if (count === 0) {
      const kind = R.kinds[id];
      if (kind === MEMO) {
        R.effects--;
        for (let l = R.deps[id]; l !== 0; l = R.lnextDep[l]) w.push(R.ldep[l]);
      } else if (kind === SIGNAL) {
        const hook = R.hook[id];
        if (hook != null) R.hooks.push(hook);
      }
    }
  }
}

/** @param {Graph} R */
function runHooks(R) {
  const hooks = R.hooks;
  while (hooks.length > 0) /** @type {() => void} */ (hooks.pop())();
}

/** Removes the links after the dependency cursor: sources the run stopped reading. @param {Graph} R @param {number} sub */
function purge(R, sub) {
  const tail = R.depsTail[sub];
  let l = tail === 0 ? R.deps[sub] : R.lnextDep[tail];
  if (l === 0) return;
  if (tail === 0) R.deps[sub] = 0;
  else R.lnextDep[tail] = 0;
  const watched = attached(R, sub);
  while (l !== 0) {
    const after = R.lnextDep[l];
    const dep = R.ldep[l];
    removeSub(R, l);
    freeLink(R, l);
    if (watched) unwatch(R, dep);
    l = after;
  }
  if (R.hooks.length > 0) runHooks(R);
}

/**
 * Tracking: a rerun that reads the same sources in order reuses its links.
 * @param {Graph} R @param {number} dep @param {number} sub
 */
function link(R, dep, sub) {
  const tail = R.depsTail[sub];
  if (tail !== 0 && R.ldep[tail] === dep) return;
  const after = tail === 0 ? R.deps[sub] : R.lnextDep[tail];
  if (after !== 0 && R.ldep[after] === dep) {
    R.depsTail[sub] = after;
    R.lseen[after] = R.version[dep];
    R.lcycle[after] = R.cycle;
    return;
  }
  const last = R.subsTail[dep];
  if (last !== 0 && R.lsub[last] === sub && R.lcycle[last] === R.cycle) return;
  const made = newLink(R, dep, sub);
  R.lprevDep[made] = tail;
  R.lnextDep[made] = after;
  if (tail === 0) R.deps[sub] = made;
  else R.lnextDep[tail] = made;
  if (after !== 0) R.lprevDep[after] = made;
  R.depsTail[sub] = made;
  appendSub(R, dep, made);
  if (attached(R, sub)) watch(R, dep);
}

// ---------------------------------------------------------------------------
// Marking

/**
 * Queues an effect in the lane of the running change, as core.js: an urgent
 * change that reaches one already queued deferred moves it to the urgent
 * lane (its low entry is then skipped).
 * @param {Graph} R @param {number} id
 */
function enqueue(R, id) {
  const lane = R.queued[id];
  // Work the low lane runs (its effects and applications) writes deferred work too.
  if (R.transition === 0 && R.lane !== LOW) {
    if (lane === 0 || lane === LOW) {
      R.queued[id] = URGENT;
      R.urgent.push(id);
    }
  } else if (lane === 0) {
    R.queued[id] = LOW;
    R.low.push(id);
    // A new deferred effect: a region walked before may reach it now.
    if (R.hastened.size > 0) R.hastened.clear();
  }
}

/**
 * An urgent change met `memo`, not clean, so it passes nothing on by itself:
 * the effects queued deferred below it (through its non-clean readers) move
 * to the urgent lane, as core.js. The root keeps what it walked until the
 * low queue grows, a node is freed (its id may come back) or the flush ends.
 * @param {Graph} R @param {number} memo
 */
function hasten(R, memo) {
  if (R.low.n === 0 || R.transition !== 0 || R.lane === LOW) return;
  const seen = R.hastened;
  if (seen.has(memo)) return;
  seen.add(memo);
  const stack = [memo];
  while (stack.length > 0) {
    const cur = /** @type {number} */ (stack.pop());
    for (let l = R.subs[cur]; l !== 0; l = R.lnextSub[l]) {
      const id = R.lsub[l];
      if (R.state[id] === CLEAN || seen.has(id)) continue;
      seen.add(id);
      const kind = R.kinds[id];
      if (kind === EFFECT) {
        if (R.queued[id] === LOW) enqueue(R, id);
      } else if (kind === MEMO) stack.push(id);
    }
  }
}

/**
 * Raises a node to `level`; a node that was clean passes the change on: a
 * memo marks its subscribers pending, an effect is queued.
 * @param {Graph} R @param {number} id @param {number} level
 */
function mark(R, id, level) {
  if (R.live[id] === FREED) return;
  const was = R.state[id];
  if (was < level) R.state[id] = level;
  if (was === CLEAN) {
    const kind = R.kinds[id];
    if (kind === MEMO) R.mark.push(id);
    else if (kind === EFFECT) enqueue(R, id);
  } else if (R.queued[id] === LOW) enqueue(R, id);
  else if (R.kinds[id] === MEMO) hasten(R, id);
}

/**
 * Marks the subscribers of every memo on the mark list pending, in the order
 * the memos were reached (the list grows while it is walked), so effects
 * queue in subscriber order. @param {Graph} R
 */
function propagatePending(R) {
  const m = R.mark;
  const subs = R.subs;
  const lnextSub = R.lnextSub;
  const lsub = R.lsub;
  const state = R.state;
  const kinds = R.kinds;
  for (let i = 0; i < m.n; i++) {
    const memo = m.a[i];
    for (let l = subs[memo]; l !== 0; l = lnextSub[l]) {
      const id = lsub[l];
      if (state[id] !== CLEAN) {
        // Already queued deferred, or behind a memo already reached: an urgent change moves it up.
        if (R.queued[id] === LOW) enqueue(R, id);
        else if (kinds[id] === MEMO) hasten(R, id);
        continue;
      }
      state[id] = PENDING;
      const kind = kinds[id];
      if (kind === MEMO) m.push(id);
      else if (kind === EFFECT) enqueue(R, id);
    }
  }
  m.n = 0;
}

/**
 * Direct subscribers of a changed node become dirty, the rest pending. A
 * written signal also marks a running effect, which is how a self-triggering
 * effect is found; a recomputed memo skips the computation reading it now.
 * @param {Graph} R @param {number} source @param {boolean} written
 */
function propagate(R, source, written) {
  const lnextSub = R.lnextSub;
  const lsub = R.lsub;
  const state = R.state;
  for (let l = R.subs[source]; l !== 0; l = lnextSub[l]) {
    const sub = lsub[l];
    if (!written && R.running[sub] !== 0) continue;
    const was = state[sub];
    // Already queued deferred, or behind a memo already reached: an urgent change moves it up.
    if (was !== CLEAN) {
      if (R.queued[sub] === LOW) enqueue(R, sub);
      else if (R.kinds[sub] === MEMO) hasten(R, sub);
    }
    if (was === DIRTY) continue;
    state[sub] = DIRTY;
    if (was === CLEAN) {
      const kind = R.kinds[sub];
      if (kind === MEMO) R.mark.push(sub);
      else if (kind === EFFECT) enqueue(R, sub);
    }
  }
  if (R.mark.n > 0) propagatePending(R);
}

/** @param {Graph} R @param {number} id */
function bump(R, id) {
  R.version[id]++;
  R.versionAll++;
}

/** @param {Graph} R @param {number} id @param {any} old @param {any} next */
function same(R, id, old, next) {
  const equals = R.equals[id];
  return equals == null ? Object.is(old, next) : typeof equals === 'function' ? equals(old, next) : false;
}

// ---------------------------------------------------------------------------
// Memos

/** @param {Graph} R @param {number} id */
function compute(R, id) {
  if (R.running[id] !== 0) throw new Error('RRX: recursive memo');
  if (R.retiring && kept(R, id)) return;
  R.running[id] = 1;
  const current = R.current;
  const pure = R.pure;
  const cycle = R.cycle;
  // The run reads contexts as where it was made (its owner's), as an effect does.
  const ctx = R.ctx;
  const scope = R.owners[id];
  R.ctx = scope === 0 ? null : R.context[scope];
  R.cycle = R.cycles = (R.cycles + 1) | 0; // Int32 columns hold it: it wraps, never leaves their range
  R.depsTail[id] = 0;
  R.current = id;
  R.pure = true;
  const previous = R.value[id];
  let value;
  let ok = true;
  // Everything thrown is caught, so the epilogue needs no finally.
  try {
    value = R.run[id](previous);
  } catch (error) {
    ok = false;
    value = error;
  }
  R.current = current;
  R.pure = pure;
  R.cycle = cycle;
  R.ctx = ctx;
  R.running[id] = 0;
  const tail = R.depsTail[id];
  if ((tail === 0 ? R.deps[id] : R.lnextDep[tail]) !== 0) purge(R, id);
  R.state[id] = CLEAN;
  const ready = R.ready[id];
  if (ok) {
    // Equality is asked only between two computed values.
    if (ready === VALUE) {
      const equals = R.equals[id];
      if (equals == null ? Object.is(previous, value) : typeof equals === 'function' && equals(previous, value)) {
        if (R.live[id] === CLOSING) free(R, id);
        return;
      }
    } else {
      R.ready[id] = VALUE;
      if (ready !== NONE) {
        R.failure[id] = undefined;
        R.waits[id] = null;
      }
    }
    R.value[id] = value;
  } else if (value === WAITING) {
    R.ready[id] = WAITS;
    R.waits[id] = waitedOn();
    R.failure[id] = value;
  } else {
    R.ready[id] = FAILURE;
    R.waits[id] = null;
    R.failure[id] = value;
  }
  // Readers are pending already (a non-clean memo's readers are never
  // clean), so they find the new version when they check (README).
  R.version[id]++;
  R.versionAll++;
  if (R.live[id] === CLOSING) free(R, id);
}

/**
 * A memo of a retired scope keeps its last value, as a stopped one does, so
 * it does not recompute (unless it has none yet). @param {Graph} R @param {number} id
 */
function kept(R, id) {
  if (R.ready[id] === NONE || !inert(R, R.owners[id])) return false;
  R.state[id] = CLEAN;
  return true;
}

/**
 * Brings a pending or dirty node up to date without recursion: sources are
 * checked in read order, and a memo source is resolved before its reader
 * continues. Memos recompute here; an effect is only classified. The frame
 * being examined stays in locals; its parents wait on the stack with the
 * cursor of the source they descended into, and a finished child is compared
 * with that cursor at once.
 * @param {Graph} R @param {number} start
 */
function refresh(R, start) {
  const base = R.sp;
  let sp = base;
  let x = start;
  let cursor = R.deps[x];
  for (;;) {
    if (R.state[x] === PENDING) {
      while (cursor !== 0) {
        const dep = R.ldep[cursor];
        if (R.kinds[dep] === MEMO && R.state[dep] !== CLEAN && R.running[dep] === 0) break;
        if (R.version[dep] !== R.lseen[cursor]) {
          R.state[x] = DIRTY;
          break;
        }
        cursor = R.lnextDep[cursor];
      }
      if (R.state[x] === PENDING) {
        if (cursor !== 0) {
          // Descend into the stale memo source.
          if (sp === R.sNode.length) {
            R.sNode = widen(R.sNode, sp * 2);
            R.sLink = widen(R.sLink, sp * 2);
          }
          R.sNode[sp] = x;
          R.sLink[sp] = cursor;
          sp++;
          x = R.ldep[cursor];
          cursor = R.deps[x];
          continue;
        }
        R.state[x] = CLEAN;
      }
    }
    if (R.state[x] === DIRTY && R.kinds[x] === MEMO) {
      R.sp = sp;
      compute(R, x);
    }
    if (sp === base) break;
    const child = x;
    sp--;
    x = R.sNode[sp];
    cursor = R.sLink[sp];
    if (R.state[x] === PENDING && cursor !== 0 && R.ldep[cursor] === child) {
      if (R.version[child] !== R.lseen[cursor]) R.state[x] = DIRTY;
      else cursor = R.lnextDep[cursor];
    }
  }
  R.sp = base;
}

/** @param {Graph} R @param {number} id */
function readMemo(R, id) {
  if (R.state[id] !== CLEAN) refresh(R, id);
  const observer = R.current;
  if (observer !== 0) link(R, id, observer);
  const ready = R.ready[id];
  if (ready === VALUE) return R.value[id];
  if (ready === WAITS) wait(R.waits[id]);
  throw R.failure[id];
}

/** Brings a memo up to date (for sibling modules). @param {Graph} R @param {number} id */
export function update(R, id) {
  if (R.state[id] !== CLEAN) refresh(R, id);
}

// ---------------------------------------------------------------------------
// Public: nodes

/**
 * @template T
 * @typedef {Handle<T> & { readonly __read?: T }} Readable
 */
/**
 * @template T
 * @typedef {Readable<T> & { readonly __write: (value: T) => void }} Writable
 */
/** @typedef {Handle<any> & { readonly __effect?: true }} Effecting */
/** @typedef {Handle<any> & { readonly __scope?: true }} Owner */

/**
 * Reads a signal, memo, derivation or task, tracking it in the running
 * computation. A freed node reads as its final value.
 * @template T
 * @param {Readable<T> | import('./task.js').Task<T>} source
 * @returns {T}
 */
export function $read(source) {
  const h = /** @type {Handle<T>} */ (source);
  const R = h.root;
  const id = h.id;
  if (R.gen[id] === h.gen) {
    const kind = R.kinds[id];
    if (kind === SIGNAL) {
      const observer = R.current;
      if (observer !== 0) link(R, id, observer);
      return R.value[id];
    }
    if (kind === MEMO) return readMemo(R, id);
  }
  if (source.kind === 'task') return /** @type {any} */ (source).read();
  return stale(h);
}

/** @param {Handle<any>} h */
function stale(h) {
  if (!(h instanceof Handle) || (h.kind !== 'signal' && h.kind !== 'memo')) throw new TypeError('RRX: $read expects a signal, memo, derivation, or task');
  live(h.root);
  return h.last;
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
  const R = signal.root;
  const id = signal.id;
  if (R.gen[id] === signal.gen) write(R, id, value);
  else {
    live(R);
    signal.last = value;
  }
  return value;
}

/** @param {Graph} R @param {number} id @param {any} value */
function write(R, id, value) {
  if (R.pure && R.permit[id] === 0) throw new Error('RRX: computation cannot write reactive state');
  const kind = R.kinds[id];
  if (kind === MEMO) {
    if (R.writable[id] === 0) throw new Error('RRX: a memo is read-only; use $derive for a writable derivation');
    // Pending source changes apply first, so this write is the latest value.
    if (R.state[id] !== CLEAN) refresh(R, id);
    if (R.ready[id] !== VALUE) {
      R.ready[id] = VALUE;
      R.failure[id] = undefined;
      R.waits[id] = null;
    }
  } else if (kind !== SIGNAL) throw new TypeError('RRX: $write expects a signal or derivation');
  const equals = R.equals[id];
  const old = R.value[id];
  const unchanged = equals == null ? Object.is(old, value) : typeof equals === 'function' ? equals(old, value) : false;
  if (!unchanged) {
    R.value[id] = value;
    bump(R, id);
    if (R.subs[id] !== 0) propagate(R, id, true);
    if (R.auto) automatic(R);
  }
}

/** In `$auto` mode a write outside computations, batches and flushes flushes now. @param {Graph} R */
export function automatic(R) {
  if (R.current === 0 && !R.flushing && R.batching === 0) $flush(R);
}

/**
 * A writable source.
 * @template T
 * @param {Graph} root
 * @param {T} initial
 * @returns {Writable<T>}
 */
export function $signal(root, initial) {
  const id = allocate(root, SIGNAL, CLEAN, root.owner);
  root.value[id] = initial;
  root.ready[id] = VALUE;
  return /** @type {any} */ (root.handle[id]);
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
  const id = allocate(root, MEMO, DIRTY, root.owner);
  root.run[id] = run;
  return /** @type {any} */ (root.handle[id]);
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
  const id = allocate(root, MEMO, DIRTY, root.owner);
  root.run[id] = step;
  root.value[id] = initial;
  return /** @type {any} */ (root.handle[id]);
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
  root.writable[memo.id] = 1;
  return /** @type {any} */ (memo);
}

/**
 * `equals(old, next)` true means observers need not change; false notifies
 * every write; null restores identity (`Object.is`).
 * @template {Handle<any>} H
 * @param {H} source
 * @param {Equality} equals
 * @returns {H}
 */
export function $equals(source, equals) {
  const id = node(source);
  if (id !== 0) source.root.equals[id] = equals;
  return source;
}

/**
 * Runs `hook` when the last observer of a signal leaves.
 * @template {Handle<any>} H
 * @param {H} signal
 * @param {(() => void) | null} hook
 * @returns {H}
 */
export function $whenUnwatched(signal, hook) {
  const id = node(signal);
  if (id !== 0) signal.root.hook[id] = hook;
  return signal;
}

/**
 * A signal that computations may write, such as state derived from a source.
 * @template {Handle<any>} H
 * @param {H} signal
 * @returns {H}
 */
export function $writable(signal) {
  const id = node(signal);
  if (id !== 0) signal.root.permit[id] = 1;
  return signal;
}

/** The number of attached subscribers watching a node. @param {Handle<any>} source */
export function $observers(source) {
  return source.root.watchers[node(source)];
}

/**
 * The sources a node read in its latest run, in read order.
 * @param {Handle<any>} source
 * @returns {Handle<any>[]}
 */
export function $sources(source) {
  const R = source.root;
  /** @type {Handle<any>[]} */
  const found = [];
  for (let l = R.deps[node(source)]; l !== 0; l = R.lnextDep[l]) found.push(/** @type {Handle<any>} */ (R.handle[R.ldep[l]]));
  return found;
}

/**
 * Distinguishes nodes, including a new node that reuses a freed id.
 * @param {Handle<any>} source
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

/** Counts of a root's live structures; scopes, arena nodes here, count as scopes only (as in core.js). @param {Graph} root */
export function $stats(root) {
  return { scopes: root.scopes, effects: root.effects, commits: root.commitCount, nodes: root.liveNodes - root.scopes, links: root.liveLinks };
}

/**
 * The running observer, or null outside computations.
 * @param {Graph} root
 * @returns {Handle<any> | null}
 */
export function $observer(root) {
  const id = root.current;
  return id === 0 ? null : root.handle[id];
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
  root.current = 0;
  try {
    return callback();
  } finally {
    root.current = previous;
  }
}

// ---------------------------------------------------------------------------
// Freeing

/** Removes a node from its owner's child list. @param {Graph} R @param {number} id */
function leave(R, id) {
  const o = R.owners[id];
  const p = R.prev[id];
  const x = R.next[id];
  if (p !== 0) R.next[p] = x;
  else R.head[o] = x;
  if (x !== 0) R.prev[x] = p;
  else R.tail[o] = p;
  R.prev[id] = 0;
  R.next[id] = 0;
}

/**
 * Frees a node: its own links are purged; each subscriber loses the link and
 * becomes dirty, so it recomputes without the vanished source.
 * @param {Graph} R @param {number} id
 */
function free(R, id) {
  if (!R.closing) {
    R.depsTail[id] = 0;
    purge(R, id);
    let l = R.subs[id];
    while (l !== 0) {
      const after = R.lnextSub[l];
      const sub = R.lsub[l];
      removeDep(R, l);
      freeLink(R, l);
      mark(R, sub, DIRTY);
      l = after;
    }
    if (R.mark.n > 0) propagatePending(R);
    R.subs[id] = 0;
    R.subsTail[id] = 0;
  }
  leave(R, id);
  const handle = R.handle[id];
  if (handle != null) handle.last = R.value[id];
  if (R.watchers[id] > 0 && R.kinds[id] === MEMO) R.effects--;
  R.state[id] = 0;
  if (R.hastened.size > 0) R.hastened.delete(id);
  R.queued[id] = 0;
  R.running[id] = 0;
  R.writable[id] = 0;
  R.permit[id] = 0;
  R.ready[id] = 0;
  R.watchers[id] = 0;
  R.runs[id] = 0;
  R.phase[id] = 0;
  R.applying[id] = 0;
  R.applied[id] = 0;
  R.head[id] = 0;
  R.tail[id] = 0;
  R.value[id] = undefined;
  R.run[id] = undefined;
  R.equals[id] = undefined;
  R.failure[id] = undefined;
  R.waits[id] = undefined;
  R.cleanup[id] = undefined;
  R.hook[id] = undefined;
  R.apply[id] = undefined;
  R.shown[id] = undefined;
  R.context[id] = undefined;
  R.handler[id] = undefined;
  R.mounts[id] = undefined;
  R.handle[id] = null;
  R.live[id] = FREED;
  R.gen[id]++;
  R.liveNodes--;
  // A queue entry of this id may remain (skipped: queued is 0); the id waits until none can.
  if (busy(R) || R.walking !== 0) R.parked.push(id);
  else R.freeNodes.push(id);
  if (R.hooks.length > 0 && !R.closing) runHooks(R);
}

/**
 * Frees a signal or memo before its owner stops.
 * @param {Handle<any>} source
 */
export function $discard(source) {
  const R = source.root;
  const id = source.id;
  if (!R.closed && R.gen[id] === source.gen && R.live[id] === ALIVE) {
    if (R.running[id] !== 0) R.live[id] = CLOSING;
    else free(R, id);
  }
}

/**
 * Recomputes a memo at its next read and has its observers check it.
 * @param {Readable<any>} memo
 */
export function $invalidate(memo) {
  const R = memo.root;
  const id = node(memo);
  if (id !== 0 && R.kinds[id] === MEMO && R.state[id] === CLEAN) {
    R.state[id] = DIRTY;
    for (let l = R.subs[id]; l !== 0; l = R.lnextSub[l]) mark(R, R.lsub[l], PENDING);
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
  const id = allocate(R, PROBE, CLEAN, 0);
  const current = R.current;
  const cycle = R.cycle;
  R.cycle = R.cycles = (R.cycles + 1) | 0; // Int32 columns hold it: it wraps, never leaves their range
  R.current = id;
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
  for (let l = R.deps[id]; l !== 0; l = R.lnextDep[l]) {
    const dep = R.ldep[l];
    bump(R, dep);
    propagate(R, dep, true);
  }
  free(R, id);
  if (!ok) throw failure;
  if (R.auto && current === 0 && !R.flushing && R.batching === 0) $flush(R);
}

// ---------------------------------------------------------------------------
// Effects

/**
 * Stops an effect, Watch or Render and frees it.
 * @param {Effecting} observer
 */
export function $cancel(observer) {
  const R = observer.root;
  const id = observer.id;
  if (R.gen[id] === observer.gen && R.live[id] === ALIVE) {
    R.queued[id] = 0;
    R.effects--;
    const cleanup = R.cleanup[id];
    R.cleanup[id] = undefined;
    if (R.running[id] !== 0) R.live[id] = CLOSING;
    else free(R, id);
    if (typeof cleanup === 'function') untracked(R, cleanup);
  }
}

/** Parked ids go back to the free list once both queues are empty and no walk is on. @param {Graph} R */
function unpark(R) {
  const p = R.parked;
  if (p.n === 0 || busy(R) || R.walking !== 0) return;
  for (let i = 0; i < p.n; i++) R.freeNodes.push(p.a[i]);
  p.n = 0;
}

/** @param {Graph} R @param {() => any} fn */
function untracked(R, fn) {
  const previous = R.current;
  R.current = 0;
  try {
    return fn();
  } finally {
    R.current = previous;
  }
}

/** @param {Graph} R @param {number} id */
function count(R, id) {
  const serial = R.serialAll;
  if (R.serial[id] !== serial) {
    R.serial[id] = serial;
    R.runs[id] = 0;
  }
  if (++R.runs[id] > LIMIT) throw new Error(`RRX: reactive cycle; an effect ran more than ${LIMIT} times in one flush`);
}

/** @param {Graph} R @param {number} id */
function execute(R, id) {
  if (R.live[id] !== ALIVE) return;
  count(R, id);
  R.state[id] = CLEAN;
  const phase = R.phase[id];
  if (phase === 0) {
    const cleanup = R.cleanup[id];
    if (cleanup !== undefined) {
      R.cleanup[id] = undefined;
      if (typeof cleanup === 'function') untracked(R, cleanup);
    }
  }
  const current = R.current;
  const cycle = R.cycle;
  const owner = R.owner;
  const ctx = R.ctx;
  const pure = R.pure;
  R.cycle = R.cycles = (R.cycles + 1) | 0; // Int32 columns hold it: it wraps, never leaves their range
  R.depsTail[id] = 0;
  R.current = id;
  R.running[id] = 1;
  // The run enters its owner, as $enter(owner, callback) does.
  const scope = R.owners[id];
  R.owner = scope;
  R.ctx = scope === 0 ? null : R.context[scope];
  if (phase !== 0) R.pure = true;
  let result;
  let ok = true;
  try {
    result = R.run[id]();
  } catch (error) {
    ok = false;
    result = error;
  }
  R.current = current;
  R.cycle = cycle;
  R.owner = owner;
  R.ctx = ctx;
  R.pure = pure;
  R.running[id] = 0;
  if (R.live[id] === CLOSING) {
    free(R, id);
    if (!ok) throw result;
    if (typeof result === 'function' && phase === 0) untracked(R, result);
    return;
  }
  purge(R, id);
  if (!ok) throw result;
  if (phase === 0) {
    if (typeof result === 'function') R.cleanup[id] = result;
    return;
  }
  // A Watch or Render: the value applies after the computations settle.
  R.value[id] = result;
  if (R.flushing || R.batching !== 0) {
    if (R.applying[id] === 0) (phase === RENDERING ? R.renders : R.applies).push(id);
    R.applying[id] = R.lane;
  } else commit(R, id);
}

/** Applies a Watch or Render value that differs from the last one applied. @param {Graph} R @param {number} id */
function commit(R, id) {
  const value = R.value[id];
  if (R.applied[id] === 1 && Object.is(value, R.shown[id])) return;
  R.applied[id] = 1;
  R.shown[id] = value;
  const cleanup = R.cleanup[id];
  R.cleanup[id] = undefined;
  const current = R.current;
  const owner = R.owner;
  const ctx = R.ctx;
  const scope = R.owners[id];
  // The application may free this node, and a node it makes may take the id: the generation tells them apart.
  const gen = R.gen[id];
  R.current = 0;
  R.owner = scope;
  R.ctx = scope === 0 ? null : R.context[scope];
  let result;
  try {
    if (typeof cleanup === 'function') cleanup();
    result = R.apply[id](value);
  } finally {
    R.current = current;
    R.owner = owner;
    R.ctx = ctx;
  }
  if (typeof result === 'function') {
    if (R.gen[id] === gen && R.live[id] === ALIVE) R.cleanup[id] = result;
    else untracked(R, result);
  }
}

/** @param {Graph} R @param {number} id */
function refreshEffect(R, id) {
  if (R.live[id] !== ALIVE) return;
  if (R.retiring && inert(R, R.owners[id])) return;
  if (R.state[id] === PENDING) refresh(R, id);
  if (R.state[id] === DIRTY) execute(R, id);
}

/** @param {Graph} R @param {number} id */
function start(R, id) {
  const was = running;
  running = R;
  try {
    execute(R, id);
  } catch (error) {
    running = was;
    const failure = deliver(R, R.owners[id], error);
    if (failure !== HANDLED) {
      try {
        $cancel(/** @type {Handle<any>} */ (R.handle[id]));
      } catch {
        // The original failure is raised.
      }
      throw failure;
    }
  }
  running = was;
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
  live(root);
  const id = allocate(root, EFFECT, DIRTY, root.owner);
  root.run[id] = callback;
  root.effects++;
  const handle = /** @type {Handle<any>} */ (root.handle[id]);
  start(root, id);
  return handle;
}

/**
 * @template T
 * @typedef {{ observer: Effecting, stop: () => void }} Watching
 */

/**
 * @template T
 * @param {Graph} R @param {() => T} read @param {(value: T) => unknown} apply @param {number} phase
 * @returns {Watching<T>}
 */
function watching(R, read, apply, phase) {
  live(R);
  const id = allocate(R, EFFECT, DIRTY, R.owner);
  R.run[id] = read;
  R.apply[id] = apply;
  R.phase[id] = phase;
  R.effects++;
  const observer = /** @type {Handle<any>} */ (R.handle[id]);
  start(R, id);
  return { observer, stop: () => $cancel(observer) };
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
  return watching(root, read, apply, WATCHING);
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
  return watching(root, read, apply, RENDERING);
}

// ---------------------------------------------------------------------------
// Flush

/** @param {Graph} R */
function busy(R) {
  return R.urgent.n + R.low.n + R.renders.n + R.applies.n > 0;
}

/** Sorts a batch of effect ids by creation order. @param {Graph} R @param {Int32Array} a @param {number} n */
function sortBatch(R, a, n) {
  const o = R.order;
  for (let i = 1; i < n; i++) {
    const v = a[i];
    const k = o[v];
    if (o[a[i - 1]] <= k) continue;
    if (n > 32) {
      a.subarray(0, n).sort((x, y) => o[x] - o[y]);
      return;
    }
    let j = i - 1;
    while (j >= 0 && o[a[j]] > k) {
      a[j + 1] = a[j];
      j--;
    }
    a[j + 1] = v;
  }
}

/** @type {any} */
const HANDLED = Symbol('handled');

/**
 * Offers an error to the nearest handling owner from `scope` upward, then the
 * root handler. Returns HANDLED, or the error that remains unhandled.
 * @param {Graph} R @param {number} scope @param {any} error
 */
function deliver(R, scope, error) {
  let failure = error;
  for (let s = scope; s !== 0; s = R.owners[s]) {
    const handler = R.handler[s];
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

/** @param {Graph} R @param {number} lane @param {Graph} note */
function runLane(R, lane, note) {
  const batch = lane === URGENT ? R.urgent : R.low;
  if (lane === URGENT) R.urgent = R.spare;
  else R.low = R.spare;
  R.spare = batch;
  const a = batch.a;
  const n = batch.n;
  batch.n = 0;
  sortBatch(R, a, n);
  // Work from the low lane counts as deferred for the adapters it reaches.
  R.lane = lane;
  R.walking = n;
  let last = 0;
  for (let i = 0; i < n; i++) {
    const id = a[i];
    if (id === last || id === 0) continue;
    last = id;
    if (R.queued[id] !== lane) continue;
    R.queued[id] = 0;
    try {
      refreshEffect(R, id);
    } catch (error) {
      const left = deliver(R, R.owners[id], error);
      if (left !== HANDLED && note.failed === HANDLED) note.failed = left;
    }
  }
  R.walking = 0;
  R.lane = URGENT;
  unpark(R);
}

/** @param {Graph} R @param {boolean} renders @param {Graph} note */
function drainApplies(R, renders, note) {
  const batch = renders ? R.renders : R.applies;
  if (renders) R.renders = R.spareApply;
  else R.applies = R.spareApply;
  R.spareApply = batch;
  const a = batch.a;
  const n = batch.n;
  batch.n = 0;
  // The batch walked holds ids too: a freed one stays parked until it ends.
  R.walking = n;
  for (let i = 0; i < n; i++) {
    const id = a[i];
    const lane = R.applying[id];
    if (lane === 0) continue;
    R.applying[id] = 0;
    if (R.live[id] !== ALIVE) continue;
    if (R.retiring && inert(R, R.owners[id])) continue;
    R.lane = lane;
    try {
      commit(R, id);
    } catch (error) {
      const left = deliver(R, R.owners[id], error);
      if (left !== HANDLED && note.failed === HANDLED) note.failed = left;
    }
    R.lane = URGENT;
  }
  R.walking = 0;
  unpark(R);
}

/** @param {Graph} R @param {(() => void)[]} callbacks @param {Graph} note */
function drainOnce(R, callbacks, note) {
  for (let i = 0; i < callbacks.length; i++) {
    try {
      callbacks[i]();
    } catch (error) {
      const left = deliver(R, 0, error);
      if (left !== HANDLED && note.failed === HANDLED) note.failed = left;
    }
  }
}

/** @param {Graph} R @param {Graph} note */
function rounds(R, note) {
  // The flush runs this root's code: `$lookup` without a root finds this root.
  // One store per flush, not per effect run.
  const was = running;
  running = R;
  try {
    turns(R, note);
  } finally {
    running = was;
  }
}

/** The rounds of a flush. @param {Graph} R @param {any} note */
function turns(R, note) {
  let round = 0;
  let active = true;
  while (active) {
    if (++round > ROUNDS) throw new Error('RRX: reactive cycle; commit hooks kept the graph busy');
    while (busy(R)) {
      if (R.urgent.n > 0) runLane(R, URGENT, note);
      else if (R.low.n > 0) runLane(R, LOW, note);
      else if (R.renders.n > 0) drainApplies(R, true, note);
      else drainApplies(R, false, note);
    }
    compactCommits(R);
    if (R.commitCount > 0) {
      const commits = R.commits;
      for (let i = 0; i < commits.length; i++) {
        const entry = commits[i];
        const run = entry.run;
        if (run === null) continue;
        if (R.retiring && inert(R, entry.owner)) continue;
        try {
          run();
        } catch (error) {
          const left = deliver(R, 0, error);
          if (left !== HANDLED && note.failed === HANDLED) note.failed = left;
        }
      }
    }
    if (R.after.length > 0) {
      const once = R.after;
      R.after = [];
      drainOnce(R, once, note);
    }
    if (!busy(R) && R.loading === 0 && R.settled.length > 0) {
      const once = R.settled;
      R.settled = [];
      drainOnce(R, once, note);
    }
    active = busy(R);
  }
}

/** Raises the first unhandled failure of a flush. @param {Graph} R @returns {never} */
function raise(R) {
  const failure = R.failed;
  R.failed = HANDLED;
  throw failure;
}

/**
 * Keeps an unhandled failure (already offered to the boundaries) to raise
 * when the current flush ends, or at the next `$flush`. @param {Graph} R @param {any} error
 */
function later(R, error) {
  if (R.failed === HANDLED) R.failed = error;
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
  live(R);
  if (R.flushing) throw new Error('RRX: recursive flush');
  if (!busy(R) && R.commitCount === 0 && R.after.length === 0 && R.settled.length === 0) {
    if (R.failed !== HANDLED) raise(R);
    return;
  }
  R.flushing = true;
  // The runs column compares it as an Int32: it wraps, never leaves that range.
  R.serialAll = (R.serialAll + 1) | 0;
  try {
    rounds(R, R);
  } finally {
    R.flushing = false;
    R.lane = URGENT;
    if (R.hastened.size > 0) R.hastened.clear();
    unpark(R);
  }
  if (R.failed !== HANDLED) raise(R);
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
  R.urgent.n = R.low.n = R.renders.n = R.applies.n = 0;
  /** @type {any} */
  let failure = HANDLED;
  try {
    stopScope(R, 0);
  } catch (error) {
    failure = error;
  }
  R.closed = true;
  for (let id = 1; id < R.count; id++) R.gen[id]++;
  R.retired = [];
  R.retiredAt = 0;
  R.sweepTop = R.sweepCur = 0;
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
// Scopes. Scopes are arena nodes whose owned nodes and child scopes
// form an intrusive list; the README records this choice.

/**
 * A scope that owns the nodes, scopes and cleanups created inside it. A null
 * parent attaches it to the root.
 * @param {Graph} root
 * @param {Owner | null} parent
 * @returns {Owner}
 */
export function $scope(root, parent) {
  live(root);
  let p = 0;
  if (parent != null) {
    p = parent.id;
    if (root.gen[p] !== parent.gen || root.live[p] !== ALIVE) throw new Error('RRX: cannot attach to a disposed scope');
  }
  const id = allocate(root, SCOPE, CLEAN, p);
  root.seen[id] = -1;
  root.context[id] = root.ctx;
  root.cleanup[id] = null;
  root.scopes++;
  return /** @type {any} */ (root.handle[id]);
}

/** @param {Graph} R @param {Owner} owner */
function scopeId(R, owner) {
  const id = owner.id;
  return id !== 0 && R.gen[id] === owner.gen && R.live[id] === ALIVE ? id : 0;
}

/** The id of a live or retired scope, else 0. @param {Graph} R @param {Owner} owner */
function heldId(R, owner) {
  const id = owner.id;
  return id !== 0 && R.gen[id] === owner.gen && (R.live[id] === ALIVE || R.live[id] === RETIRED) ? id : 0;
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
  const id = scopeId(R, owner);
  if (id === 0) throw new Error('RRX: disposed scope');
  const previous = R.owner;
  const context = R.ctx;
  const was = running;
  R.owner = id;
  R.ctx = R.context[id];
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
 * Runs `callback` with no owner, for nodes whose lifetime is managed
 * explicitly.
 * @template T
 * @param {Graph} root
 * @param {() => T} callback
 * @returns {T}
 */
export function $unowned(root, callback) {
  const previous = root.owner;
  root.owner = 0;
  try {
    return callback();
  } finally {
    root.owner = previous;
  }
}

/**
 * Stops a scope: frees what it owns, newest first (child scopes stop the same
 * way), then runs its cleanups in reverse order.
 * @param {Owner} owner
 */
export function $stop(owner) {
  const R = owner.root;
  if (R.closed) return;
  // A retired scope's teardown finishes now.
  const id = heldId(R, owner);
  if (id !== 0) stopScope(R, id);
}

/** @param {Graph} R @param {number} top */
function stopScope(R, top) {
  /** @type {any} */
  let failure = HANDLED;
  R.live[top] = CLOSING;
  let cur = top;
  for (;;) {
    const child = R.tail[cur];
    if (child !== 0) {
      if (R.kinds[child] === SCOPE) {
        R.live[child] = CLOSING;
        if (R.tail[child] !== 0) {
          cur = child;
          continue;
        }
      }
      try {
        release(R, child);
      } catch (error) {
        if (failure === HANDLED) failure = error;
      }
      continue;
    }
    if (cur === top) break;
    const parent = R.owners[cur];
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
 * leaves the list now and is freed when its run returns.
 * @param {Graph} R @param {number} id
 */
function release(R, id) {
  const kind = R.kinds[id];
  if (kind === SCOPE) {
    const list = R.cleanup[id];
    R.cleanup[id] = null;
    R.mounts[id] = undefined;
    if (id !== 0) R.scopes--;
    /** @type {any} */
    let failure = HANDLED;
    if (list != null) {
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
    if (id === 0) R.live[0] = ALIVE;
    else free(R, id);
    if (failure !== HANDLED) throw failure;
    return;
  }
  if (R.running[id] !== 0) {
    // Leaves the list now; its run's epilogue frees it. It moves to the
    // root's list, except while the root itself is disposed: that list is
    // being emptied, and a node moved there would be walked again, for ever.
    leave(R, id);
    R.owners[id] = 0;
    if (!R.closing) {
      const prev = R.tail[0];
      R.prev[id] = prev;
      if (prev !== 0) R.next[prev] = id;
      else R.head[0] = id;
      R.tail[0] = id;
    }
    if (kind === EFFECT) $cancel(/** @type {Handle<any>} */ (R.handle[id]));
    else R.live[id] = CLOSING;
    return;
  }
  if (kind === EFFECT && R.live[id] === ALIVE) $cancel(/** @type {Handle<any>} */ (R.handle[id]));
  else free(R, id);
}

// ---------------------------------------------------------------------------
// Background disposal, as in core.js: `$retire` makes a scope inert at once;
// `$sweep` frees it later in time-bounded slices. Inertness is checked where
// work would run (an effect, an application, a `$commit` hook, a memo
// recompute) only while retired scopes wait (R.retiring), through a
// per-scope cache of the root's retire epoch (the `seen` column).

/**
 * True when `scope` or an ancestor was retired or stopped. A live answer is
 * cached on the scope until the next `$retire`.
 * @param {Graph} R @param {number} scope
 */
function inert(R, scope) {
  const epoch = R.retires;
  for (let s = scope; ; s = R.owners[s]) {
    if (R.live[s] !== ALIVE) return true;
    if (s === 0 || R.seen[s] === epoch) break;
  }
  R.seen[scope] = epoch;
  return false;
}

/**
 * Makes a scope and everything it owns inert at once, in O(1); `$sweep`
 * frees it later in `$stop`'s order (core.js has the full contract).
 * @param {Owner} owner
 */
export function $retire(owner) {
  const R = owner.root;
  if (R.closed) return;
  const id = scopeId(R, owner);
  if (id === 0) return;
  R.live[id] = RETIRED;
  R.retires++;
  R.retiring = true;
  const idle = R.sweepTop === 0 && R.retiredAt === R.retired.length;
  R.retired.push(id, R.gen[id]);
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
 * Installs the host's sweep scheduler (see core.js); null leaves sweeping to
 * explicit `$sweep` calls.
 * @param {Graph} root
 * @param {((root: Graph) => void) | null} scheduler
 */
export function $sweeper(root, scheduler) {
  root.sweeper = scheduler;
}

/** The number of retired scopes not yet freed. @param {Graph} R */
function pending(R) {
  let n = (R.retired.length - R.retiredAt) >> 1;
  if (R.sweepTop !== 0) n++;
  return n;
}

/**
 * Frees retired work for about `budget` milliseconds (default 4) and returns
 * how many retired scopes remain (0 when done); see core.js.
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
  const deadline = t + (busy(R) ? 0 : budget);
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
  if (failure !== HANDLED) throw failure;
  return pending(R);
}

/**
 * One unit of sweeping, in `$stop`'s order; returns the units done, or 0 when
 * nothing is left. A scope stopped meanwhile is dropped.
 * @param {Graph} R
 */
function step(R) {
  let top = R.sweepTop;
  if (top !== 0 && (R.live[top] !== RETIRED || R.gen[top] !== R.sweepGen)) top = R.sweepTop = R.sweepCur = 0;
  if (top === 0) {
    const q = R.retired;
    while (R.retiredAt < q.length) {
      const s = q[R.retiredAt];
      const g = q[R.retiredAt + 1];
      R.retiredAt += 2;
      if (R.gen[s] === g && R.live[s] === RETIRED) {
        top = s;
        R.sweepGen = g;
        break;
      }
    }
    if (top === 0) {
      q.length = 0;
      R.retiredAt = 0;
      R.retiring = false;
      return 0;
    }
    R.sweepTop = R.sweepCur = top;
  }
  const cur = R.sweepCur;
  const child = R.tail[cur];
  if (child !== 0) {
    if (R.kinds[child] === SCOPE) {
      R.live[child] = CLOSING;
      if (R.tail[child] !== 0) {
        R.sweepCur = child;
        return 1;
      }
      return 1 + sweepRelease(R, child, child);
    }
    return 1 + sweepRelease(R, child, cur);
  }
  if (cur === top) R.sweepTop = R.sweepCur = 0;
  else R.sweepCur = R.owners[cur];
  return 1 + sweepRelease(R, cur, cur);
}

/**
 * Releases one swept node or scope; a failure goes to the nearest `$catch` from
 * `at`, and the first unhandled one is kept for the end of the slice.
 * Returns the number of cleanups a scope ran.
 * @param {Graph} R @param {number} id @param {number} at
 */
function sweepRelease(R, id, at) {
  let cleanups = 0;
  if (R.kinds[id] === SCOPE) {
    const list = R.cleanup[id];
    if (list != null) cleanups = list.length;
  }
  try {
    release(R, id);
  } catch (error) {
    const left = deliver(R, at, error);
    if (left !== HANDLED && R.sweepFailure === HANDLED) R.sweepFailure = left;
  }
  return cleanups;
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
  const R = owner.root;
  const id = scopeId(R, owner);
  if (id === 0) return null;
  let list = R.cleanup[id];
  if (list == null) list = R.cleanup[id] = [];
  return cleanupKey(list, callback);
}

/**
 * Forgets a registered cleanup, for a resource that ended before its owner.
 * @param {Owner} owner
 * @param {number | null} key
 */
export function $release(owner, key) {
  if (key == null) return;
  const R = owner.root;
  const id = heldId(R, owner);
  if (id === 0) return;
  const list = R.cleanup[id];
  if (list != null) dropCleanup(list, key);
}

/** The number of live cleanups of an owner. @param {Owner} owner */
export function $cleanups(owner) {
  const R = owner.root;
  const id = heldId(R, owner);
  const list = id === 0 ? null : R.cleanup[id];
  let n = 0;
  if (list != null) for (const fn of list) if (fn != null) n++;
  return n;
}

/**
 * Registers deferred initialization that the host drains with `$mounts`.
 * @param {Owner} owner
 * @param {() => void} callback
 */
export function $mount(owner, callback) {
  const R = owner.root;
  const id = scopeId(R, owner);
  if (id === 0) return;
  let list = R.mounts[id];
  if (list == null) list = R.mounts[id] = [];
  list.push(callback);
}

/** The host schedules this at its post-mount boundary. @param {Owner} owner */
export function $mounts(owner) {
  const R = owner.root;
  const id = scopeId(R, owner);
  if (id === 0) return;
  const list = R.mounts[id];
  R.mounts[id] = null;
  if (list != null) for (const callback of list) if (scopeId(R, owner) !== 0) $enter(owner, callback);
}

/** True while a scope and its ancestors are live. @param {Owner | null} owner */
export function $alive(owner) {
  if (owner == null) return false;
  const R = owner.root;
  if (R.closed) return false;
  let s = scopeId(R, owner);
  if (s === 0) return false;
  for (; s !== 0; s = R.owners[s]) if (R.live[s] !== ALIVE) return false;
  return true;
}

/**
 * The current owner, or null at the root.
 * @param {Graph} root
 * @returns {Owner | null}
 */
export function $owner(root) {
  return root.owner === 0 ? null : /** @type {Owner} */ (root.handle[root.owner]);
}

/**
 * A hook run after each flush round until its owner stops. Returns its key.
 * @param {Graph} root
 * @param {Owner | null} owner
 * @param {() => void} callback
 */
export function $commit(root, owner, callback) {
  live(root);
  /** @type {{ run: (() => void) | null, owner: number }} */
  const entry = { run: callback, owner: owner == null ? 0 : scopeId(root, owner) };
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
 * Drops the entries of `$commit` hooks whose owner stopped, once they are
 * most of the list, so a round walks the live ones only (as core.js).
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

/**
 * Installs the error boundary of a scope, or of the root.
 * @param {Owner | Graph} owner
 * @param {((error: any) => void) | null} handler
 */
export function $catch(owner, handler) {
  if (owner instanceof Graph) owner.errorHandler = handler;
  else {
    const id = scopeId(owner.root, owner);
    if (id !== 0) owner.root.handler[id] = handler;
  }
}

/**
 * Offers an error to the nearest handling owner, then the root handler.
 * Returns null when handled, otherwise the error that remains unhandled.
 * @param {Graph} root
 * @param {Owner | null} owner
 * @param {any} error
 */
export function $deliver(root, owner, error) {
  const left = deliver(root, owner == null ? 0 : scopeId(root, owner), error);
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
  if (value instanceof Handle) return $derive(root, () => $read(value));
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
  return layer.node === null ? layer.value : $read(layer.node);
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

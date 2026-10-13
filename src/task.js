// Tasks, readiness and optimistic updates.
//
// A pending read throws the WAITING sentinel after recording the state it
// waits on; Outcome turns it back into data.

import {
  WAITING, waitedOn, wait, $signal, $read, $write, $memo, $watch, $untrack, $equals,
  $discard, $alive, $after, moved, requery, peeked, beginChange, endChange, changed, statusRead, failed, live, update, assign, $deliver,
  hold, unhold,
} from './core.js';

/**
 * A task's readiness. A pending state says whether its value has been ready
 * (`behind`, what `$behind` reads); a ready one whether it still streams.
 * @template T
 * @typedef {{ status: 'pending' | 'ready' | 'failed', value: T | null, error: any, streaming?: boolean, behind?: boolean }} State
 */
/**
 * @template T
 * @typedef {{ ok: true, value: T } | { ok: false, error: any }} Result
 */
/**
 * A readable source: a task, signal, memo, or a function reading any of them.
 * @template T
 * @typedef {import('./core.js').Readable<T> | Task<T> | (() => T)} Source
 */

/** Launches one computation of a task's state may make before its work is reported as a cycle. */
const RELAUNCHES = 100;

/** The status of the input before the first launch: never equal to one. */
const UNSET = '';

/** Task flags and the readiness field within them. */
const STREAM = 1;
const STREAMING = 2;
const INSIDE = 4;
const QUIET = 8;
const FLYING = 16;
const ENDED = 32;
const PHASE = 192;
const PENDING = 0;
const READY = 64;
const FAILED = 128;
/** A value has been ready once: a pending state keeps a shown value ($behind). */
const SHOWN = 256;

/**
 * An asynchronous value: `$read(task)` is its value when ready, raises its
 * failure, or pends.
 *
 * A task is one eager memo and this record. The memo tracks its input and
 * carries a revision; the record keeps readiness and the latest value.
 * Readiness snapshots are immutable and made on demand, so value-only
 * readers allocate none. Readers track that one memo, so a source change
 * reaches them through it, and a read after the
 * change recomputes it, which launches the new work at once (the stale-read
 * pull). The state is an eager memo (it has an application): a change
 * queues it, and the flush refreshes it with nothing reading it, so the
 * work launches for every input. It holds a watch of its own, so its
 * sources count it as a reader. The teardown is its state memo's cleanup.
 * @template T
 */
export class Job {
  /**
   * @param {import('./core.js').Graph} root
   * @param {() => any} source
   * @param {(input: any, next: any, complete: any) => unknown} start
   * @param {boolean} stream `start` takes `next` (Stream)
   */
  constructor(root, source, start, stream) {
    /** @type {'task'} */
    this.kind = 'task';
    this.root = root;
    /** The owner the task was made in; its work runs there. */
    this.owner = root.owner;
    this.context = root.ctx;
    this.source = source;
    this.start = start;
    /** Stream, streaming, inside, quiet, flying, ended, two phase bits, then shown. */
    this.flags = stream ? STREAM : 0;
    /** The last ready value, kept while newer work is pending or failed. @type {T | null} */
    this.lastReady = null;
    /** @type {any} */ this.failure = null;
    this.revision = 0;
    /** @type {State<T> | null} */ this.snapshot = null;
    /** The input's last outcome, split into fields (no record per run). @type {string} */
    this.status = UNSET;
    /** @type {any} */ this.input = null;
    /** @type {any} */ this.error = null;
    /** The launch generation: a completion counts only for the latest. */
    this.generation = 0;
    /** @type {(() => void) | null} */ this.cancel = null;
    /** The last pending state made, kept while the value it keeps is the same. @type {State<T> | null} */
    this.waiting = null;
    /** Change marks on the task: while any holds, it reads pending, keeping its value. */
    this.changes = 0;
    /** Those of them that end at the task's next value. */
    this.nexts = 0;
    /** @type {import('./core.js').Writable<number>} */
    this.node = /** @type {any} */ (null);
  }
  /** Whether start takes an emission callback. */
  get stream() { return (this.flags & STREAM) !== 0; }
  /** @param {boolean} value */
  set stream(value) { this.flags = value ? this.flags | STREAM : this.flags & ~STREAM; }
  /** Whether the last ready value is still streaming. */
  get streaming() { return (this.flags & STREAMING) !== 0; }
  /** @param {boolean} value */
  set streaming(value) { this.flags = value ? this.flags | STREAMING : this.flags & ~STREAMING; }
  /** A publication is the running computation's result, not a local write. */
  get inside() { return (this.flags & INSIDE) !== 0; }
  /** @param {boolean} value */
  set inside(value) { this.flags = value ? this.flags | INSIDE : this.flags & ~INSIDE; }
  /** A rerun keeps the ready value visible. */
  get quiet() { return (this.flags & QUIET) !== 0; }
  /** @param {boolean} value */
  set quiet(value) { this.flags = value ? this.flags | QUIET : this.flags & ~QUIET; }
  /** Work has launched and has not completed. */
  get flying() { return (this.flags & FLYING) !== 0; }
  /** @param {boolean} value */
  set flying(value) { this.flags = value ? this.flags | FLYING : this.flags & ~FLYING; }
  /** The task or its owner has stopped. */
  get ended() { return (this.flags & ENDED) !== 0; }
  /** @param {boolean} value */
  set ended(value) { this.flags = value ? this.flags | ENDED : this.flags & ~ENDED; }
  /** @returns {'pending' | 'ready' | 'failed'} */
  get phase() {
    const phase = this.flags & PHASE;
    return phase === PENDING ? 'pending' : phase === READY ? 'ready' : 'failed';
  }
  /** @param {'pending' | 'ready' | 'failed'} value */
  set phase(value) {
    this.flags = (this.flags & ~PHASE) | (value === 'pending' ? PENDING : value === 'ready' ? READY : FAILED);
  }
  /**
   * The state, launching first for an input that changed since the last
   * launch; tracked in the running computation.
   * @returns {State<T>}
   */
  current() {
    $read(this.node);
    statusRead(this.root);
    return this.shown;
  }
  /** Materializes an immutable snapshot only for readiness readers. @returns {State<T>} */
  get shown() {
    if (this.snapshot !== null) return this.snapshot;
    if ((this.flags & PHASE) === PENDING) {
      let waiting = this.waiting;
      const behind = (this.flags & SHOWN) !== 0;
      if (waiting === null || !Object.is(waiting.value, this.lastReady) || !Object.is(waiting.error, this.failure) || waiting.behind !== behind) {
        waiting = this.waiting = { status: 'pending', value: this.lastReady, error: this.failure, behind };
      }
      return this.snapshot = waiting;
    }
    /** @type {State<T>} */
    const snapshot = { status: this.phase, value: this.lastReady, error: this.failure };
    if (this.streaming) snapshot.streaming = true;
    return this.snapshot = snapshot;
  }
  /** @returns {T} */
  read() {
    const state = this.current();
    if (state.status === 'ready') return /** @type {T} */ (state.value);
    if (state.status === 'failed') throw state.error;
    return wait(state);
  }
  /** Stops the work and its input tracking. */
  stop() {
    this.end();
    $discard(this.node);
  }
  /**
   * Runs the work again for the current input. It is quiet: a ready state
   * stays shown, not pending, until the new result replaces it;
   * $changing makes a reload read as pending.
   */
  retry() {
    if (this.ended) throw new Error('RRX: disposed task');
    // An input that changed launches first (a change reads pending); the same input launches again.
    const generation = this.generation;
    update(this.root, this.node);
    if (this.generation !== generation) return;
    this.quiet = true;
    try {
      this.lifted(false);
    } finally {
      this.quiet = false;
    }
  }
  /**
   * The state's computation: reads the input and launches when its outcome
   * (status, value and error by identity) changed.
   * @returns {number}
   */
  compute() {
    const owner = this.root.owner;
    let value;
    try {
      value = this.source();
    } catch (error) {
      return this.blocked(error, owner);
    }
    if (this.status !== 'ready' || !Object.is(value, this.input)) {
      this.status = 'ready';
      this.input = value;
      this.error = null;
      this.launchComputed();
      if (moved(this.node)) this.relaunch(1, owner);
    }
    return this.revision;
  }
  /** A source that pends or fails, kept off the ready-input path. @param {any} error @param {import('./core.js').Owner} owner @returns {number} */
  blocked(error, owner) {
    let status = 'failed';
    let value = null;
    if (error === WAITING) {
      const state = waitedOn();
      status = 'pending';
      value = state == null ? null : state.value;
      error = null;
    }
    if (status !== this.status || !Object.is(value, this.input) || !Object.is(error, this.error)) {
      this.status = status;
      this.input = value;
      this.error = error;
      this.launchComputed();
      // The launch may have written the task's own input (its work starts
      // synchronously): that write relaunches now instead of being lost.
      if (moved(this.node)) this.relaunch(1, owner);
    }
    return this.revision;
  }
  /**
   * The input moved during the launch: reads it again and launches for the
   * new outcome, until a launch leaves it alone; the slow path of compute.
   * @param {number} launches @param {import('./core.js').Owner} owner
   */
  relaunch(launches, owner) {
    for (;;) {
      if (launches === RELAUNCHES) throw new Error(`RRX: a task relaunched ${RELAUNCHES} times in one computation: its work keeps writing its own input`);
      const R = this.root;
      R.current = this.node;
      R.pure = true;
      R.owner = owner;
      R.ctx = this.owner.context;
      const outcome = $outcome(this.source);
      if (outcome.status === this.status && Object.is(outcome.value, this.input) && Object.is(outcome.error, this.error)) return;
      this.status = outcome.status;
      this.input = outcome.value;
      this.error = outcome.error;
      this.launchComputed();
      if (!moved(this.node)) return;
      launches++;
    }
  }

  /** Launches inside the eager computation; the memo epilogue restores the root. */
  launchComputed() {
    const R = this.root;
    R.current = null;
    R.pure = false;
    R.owner = this.owner;
    R.ctx = this.context;
    this.inside = true;
    try {
      this.launch();
    } finally {
      this.inside = false;
    }
  }

  /**
   * Launches untracked, allowed to write, in the task's owner and context.
   * @param {boolean} inside called from the state's computation
   */
  lifted(inside) {
    const R = this.root;
    // Inside the computation the owner is alive: a retired one keeps its value.
    if (!inside && !$alive(this.owner)) throw new Error('RRX: disposed task');
    const current = R.current;
    const pure = R.pure;
    const owner = R.owner;
    const context = R.ctx;
    R.current = null;
    R.pure = false;
    R.owner = this.owner;
    R.ctx = this.context;
    this.inside = inside;
    try {
      this.launch();
    } finally {
      this.inside = false;
      R.current = current;
      R.pure = pure;
      R.owner = owner;
      R.ctx = context;
    }
  }
  /** Cancels the running work and starts it for the current input. */
  launch() {
    const ticket = ++this.generation;
    const cancel = this.cancel;
    if (cancel !== null) {
      this.cancel = null;
      this.cancelled(cancel);
    }
    if (this.status !== 'ready') {
      // The input pends or fails: the task shows that, keeping its value.
      const phase = this.status === 'pending' ? PENDING : FAILED;
      if ((this.flags & PHASE) !== phase || this.failure !== this.error || (this.flags & STREAMING) !== 0) this.publish(phase, this.lastReady, this.error);
      return;
    }
    // Pending, keeping the value. A task already pending keeps its state (the
    // same snapshot: nothing a reader sees changed, so none reruns); the
    // pending state of a value is made once and kept for the next launch.
    const phase = this.flags & PHASE;
    if (phase !== PENDING && !(this.quiet && phase === READY)) this.publish(PENDING, this.lastReady, null);
    this.flying = true;
    /** @param {Result<T>} result */
    const complete = result => {
      if (!this.accepts(ticket)) return;
      this.flying = false;
      // Nothing marked changing (the common case): the value is ready at once.
      if (!result.ok) this.publish(FAILED, this.lastReady, result.error);
      else if (this.changes === 0) this.publish(READY, result.value, null);
      else this.arrive(result.value, false);
    };
    try {
      if (this.stream) {
        /** @param {T} value */
        const next = value => {
          if (this.accepts(ticket)) this.arrive(value, true);
        };
        this.started(ticket, this.start(this.input, next, complete));
      } else this.started(ticket, this.start(this.input, complete, null));
    } catch (error) {
      complete({ ok: false, error });
    }
  }
  /**
   * Runs the superseded work's cancel; what it throws goes to the task
   * owner's boundary (raised at the flush's end if none handles it).
   * @param {() => void} cancel
   */
  cancelled(cancel) {
    try {
      cancel();
    } catch (error) {
      failed(this.root, this.owner, error);
    }
  }
  /**
   * Keeps the cancel `start` returned for launch `ticket`. Work that
   * relaunched the task while it started is already superseded: its cancel
   * runs at once.
   * @param {number} ticket @param {any} made
   */
  started(ticket, made) {
    if (typeof made !== 'function') return;
    if (ticket === this.generation) this.cancel = /** @type {() => void} */ (made);
    else made();
  }
  /**
   * True when a completion of launch `ticket` still counts. A completion that
   * finds the input changed since the launch brings the state up to date
   * first, which launches the new work and supersedes this one.
   * @param {number} ticket
   */
  accepts(ticket) {
    const R = this.root;
    if (ticket !== this.generation || this.ended || R.closed || !$alive(this.owner)) return false;
    if (!this.inside) update(R, this.node);
    return ticket === this.generation;
  }
  /**
   * A new value: it ends the change marks that wait for one; while others hold,
   * it is the latest value, read as pending.
   * @param {T} value @param {boolean} streaming
   */
  arrive(value, streaming) {
    this.changes -= this.nexts;
    this.nexts = 0;
    if (this.changes > 0) this.publish(PENDING, value, null);
    else this.publish(READY, value, null, streaming);
  }
  /** Marks the task changing: it reads pending, keeping its value. @param {boolean} next ends at the next value */
  beginChange(next) {
    if (this.ended) throw new Error('RRX: disposed task');
    update(this.root, this.node);
    this.changes++;
    if (next) this.nexts++;
    if ((this.flags & PHASE) === READY) this.publish(PENDING, this.lastReady, null);
  }
  /** Ends a change mark: with none left and no work in flight, the value reads ready. @param {boolean} next */
  endChange(next) {
    if (this.ended || this.changes === 0) return;
    // A changed input may launch new work, complete, or stop the owner.
    // Settle it before deciding whether releasing this mark makes us ready.
    if (!this.inside) update(this.root, this.node);
    if (this.ended || this.changes === 0) return;
    if (next) {
      if (this.nexts === 0) return;
      this.nexts--;
    }
    this.changes--;
    if (this.changes === 0 && !this.flying && this.status === 'ready' && (this.flags & PHASE) === PENDING) this.publish(READY, this.lastReady, null);
  }
  /** @param {0 | 64 | 128} phase @param {T | null} value @param {any} error @param {boolean} [streaming] */
  publish(phase, value, error, streaming = false) {
    const R = this.root;
    const previous = this.lastReady;
    loading(R, this.flags & PHASE, phase);
    this.flags = (this.flags & ~(PHASE | STREAMING)) | phase | (streaming ? STREAMING : 0) | (phase === READY ? SHOWN : 0);
    this.lastReady = value;
    this.failure = error;
    this.snapshot = null;
    this.revision++;
    // Inside the computation the state is its result; elsewhere a local write.
    if (!this.inside) assign(R, this.node, this.revision, Object.is(previous, value));
  }
  /** The teardown, when the task stops or its owner does. */
  end() {
    if (this.ended) return;
    this.ended = true;
    this.generation++;
    loading(this.root, this.flags & PHASE, -1);
    const cancel = this.cancel;
    if (cancel !== null) {
      this.cancel = null;
      this.cancelled(cancel);
    }
  }
}

/** The task's teardown, as the state memo's cleanup (`this` is the memo). @this {import('./core.js').Node<any>} */
function ended() {
  /** @type {Job<any>} */ (/** @type {unknown} */ (this.apply)).end();
}
/** The state's computation (`this` is the state memo, whose `apply` is its job). @this {import('./core.js').Node<any>} */
function stateOf() {
  const job = /** @type {Job<any>} */ (/** @type {unknown} */ (this.apply));
  const R = job.root;
  const owner = R.owner;
  const previous = job.lastReady;
  let revision;
  try {
    revision = job.compute();
  } catch (error) {
    changed(R, this, false);
    throw error;
  } finally {
    // Core restores current, purity and context; only task work changes owner.
    R.owner = owner;
  }
  if (revision !== this.value) changed(R, this, this.value !== undefined && Object.is(previous, job.lastReady));
  return revision;
}

/**
 * @template T
 * @typedef {Job<T>} Task
 */

/** @param {unknown} source @returns {source is Job<any>} */
const tasked = source => source instanceof Job;

/**
 * @template T
 * @param {Source<T>} source
 * @returns {() => T}
 */
const reader = source => (typeof source === 'function' ? source : () => $read(source));

/**
 * @template T
 * @typedef {{ status: 'pending' | 'ready' | 'failed', value: T | null, error: any }} Outcomes
 */

/**
 * Runs `read` and reports pending, ready or failed as data.
 * @template T
 * @param {Source<T>} read
 * @returns {Outcomes<T>}
 */
export function $outcome(read) {
  // Status as data: an indicator, never held by a transition.
  peeked();
  const run = reader(read);
  try {
    return { status: 'ready', value: run(), error: null };
  } catch (error) {
    if (error === WAITING) {
      const state = waitedOn();
      return { status: 'pending', value: state == null ? null : state.value, error: null };
    }
    return { status: 'failed', value: null, error };
  }
}

/**
 * The last ready value, kept while newer work is pending or failed.
 * @template T
 * @param {Source<T>} source
 * @returns {T | null}
 */
export function $lastReady(source) {
  // The value alone: not a status read (a launch that keeps it reruns nothing),
  // but an indicator: a transition never holds what shows it.
  peeked();
  if (source instanceof Job) {
    $read(source.node);
    return source.lastReady;
  }
  return $outcome(source).value;
}

/**
 * True while a task, or anything a computation reads, is still pending.
 * @param {Source<any>} source
 */
export function $pending(source) {
  peeked();
  return tasked(source) ? source.current().status === 'pending' : $outcome(source).status === 'pending';
}

/** Counts pending tasks; -1 denotes creation or disposal. @param {import('./core.js').Graph} R @param {number} was @param {number} now */
function loading(R, was, now) {
  if (was !== PENDING && now === PENDING) R.loading++;
  if (was === PENDING && now !== PENDING) R.loading--;
}

/**
 * @template I, T
 * @param {import('./core.js').Graph} root
 * @param {Source<I>} source
 * @param {(input: I, next: any, complete: any) => unknown} start
 * @param {boolean} stream
 * @returns {Task<T>}
 */
function launcher(root, source, start, stream) {
  const R = root;
  live(R);
  if (!$alive(R.owner)) throw new Error('RRX: cannot attach to a disposed scope');
  /** @type {Job<T>} */
  const job = new Job(R, reader(source), start, stream);
  loading(R, -1, PENDING);
  // A memo's `apply` (unused by memos) holding the job makes the state memo
  // eager; its own watch makes its sources count it as a reader.
  const node = $memo(R, stateOf);
  node.apply = /** @type {any} */ (job);
  node.watchers = 1;
  job.node = /** @type {any} */ (node);
  // The teardown runs when the memo is freed, with its owner or by stop.
  node.cleanup = ended;
  // The first computation launches the work.
  update(R, node);
  return job;
}

/**
 * An asynchronous value computed from `source`. `start(input, complete)`
 * begins the work and may return a cancel function; completion runs on the
 * graph owner, followed by a `$flush`.
 * @template I, T
 * @param {import('./core.js').Graph} root
 * @param {Source<I>} source
 * @param {(input: I, complete: (result: Result<T>) => void) => unknown} start
 * @returns {Task<T>}
 */
export function $task(root, source, start) {
  return launcher(root, source, start, false);
}

/**
 * A task whose work reports values as they arrive: `start(input, next,
 * complete)` calls `next(value)` for each value, then `complete(result)` once.
 * @template I, T
 * @param {import('./core.js').Graph} root
 * @param {Source<I>} source
 * @param {(input: I, next: (value: T) => void, complete: (result: Result<T>) => void) => unknown} start
 * @returns {Task<T>}
 */
export function $stream(root, source, start) {
  return launcher(root, source, start, true);
}

/**
 * Runs a task's work again for its current input, or an async memo's at its
 * next read. Quiet: the value shown stays, not pending, until the new one
 * replaces it; `$changing` first makes the reload read as pending.
 * @param {Task<any> | import('./core.js').Readable<any>} source
 */
export function $rerun(source) {
  if (tasked(source)) source.retry();
  else requery(/** @type {any} */ (source));
}

/**
 * A memo of `read`'s outcome that keeps the previous ready value while newer
 * work is pending.
 * @template T
 * @param {import('./core.js').Graph} root
 * @param {() => T} read
 * @returns {import('./core.js').Readable<Outcomes<T>>}
 */
export function $load(root, read) {
  /** @type {T | null} */
  let previous = null;
  return $memo(root, () => {
    const result = $outcome(read);
    if (result.status === 'ready') {
      previous = result.value;
      return result;
    }
    return { status: result.status, value: previous, error: result.error };
  });
}

/**
 * @template T
 * @typedef {{ ticket: number, update: (current: T) => T }} Layer
 */
/**
 * @template T
 * @typedef {{
 *   value: import('./core.js').Readable<T>,
 *   apply: (update: (current: T) => T) => number,
 *   settle: (ticket: number) => void,
 *   commit: (ticket: number) => void,
 *   pending: import('./core.js').Readable<boolean>,
 * }} Optimism
 */

/**
 * Pending updates layered over a base signal, in the order they began; value
 * applies every unsettled update to the current base, so a base that changes
 * meanwhile is rebased rather than overwritten.
 * @template T
 * @param {import('./core.js').Graph} root
 * @param {import('./core.js').Writable<T>} base
 * @returns {Optimism<T>}
 */
export function $optimistic(root, base) {
  /** @type {import('./core.js').Writable<Layer<T>[]>} */
  const layers = $equals($signal(root, /** @type {Layer<T>[]} */ ([])), false);
  let serial = 0;
  const current = () => $untrack(root, () => $read(layers));
  const value = $memo(root, () => {
    let result = $read(base);
    for (const layer of $read(layers)) result = layer.update(result);
    return result;
  });
  /** @param {number} ticket */
  const remove = ticket => {
    const found = current().find(layer => layer.ticket === ticket);
    if (found !== undefined) $write(layers, current().filter(layer => layer.ticket !== ticket));
    return found;
  };
  return {
    value,
    apply: update => {
      serial++;
      $write(layers, [...current(), { ticket: serial, update }]);
      return serial;
    },
    settle: ticket => {
      remove(ticket);
    },
    commit: ticket => {
      const found = remove(ticket);
      if (found !== undefined) $write(base, found.update($untrack(root, () => $read(base))));
    },
    pending: $memo(root, () => $read(layers).length > 0),
  };
}

/**
 * An action shown optimistically while its work runs: `update(current,
 * input)` computes the optimistic value; success commits it over the base,
 * failure drops it, and `done(result)` observes the outcome. When the commit
 * reruns `update` over the base and it throws, the update is dropped, `done`
 * sees `{ ok: false, error }` and the error is raised from `complete`; an error
 * `done` throws then goes to the root's error boundary (left unhandled, it is
 * raised in a microtask). Each call returns a function that stops the work and
 * drops its update.
 * @template T, I, R
 * @param {import('./core.js').Graph} root
 * @param {{ apply: (update: (current: T) => T) => number, settle: (ticket: number) => void, commit: (ticket: number) => void }} optimistic
 * @param {(current: T, input: I) => T} update
 * @param {(input: I, complete: (result: Result<R>) => void) => unknown} start
 * @param {((result: Result<R>) => void) | null} [done]
 * @returns {(input: I) => () => void}
 */
export function $act(root, optimistic, update, start, done) {
  return input => {
    const ticket = optimistic.apply(current => update(current, input));
    let finished = false;
    /** @type {any} */
    let cancel = null;
    /** The change marks made while `start` ran: they end with the action. @type {(() => void)[]} */
    const marks = [];
    const release = () => {
      for (let i = 0; i < marks.length; i++) marks[i]();
      marks.length = 0;
    };
    /** @param {Result<R>} result */
    const complete = result => {
      if (finished || root.closed) return;
      finished = true;
      // The commit's write is held until done has run: under $auto it would
      // flush at once, and another computation's failure in that flush is not
      // the action's. Only the user's update, rerun by commit over the base it
      // has now, can fail the action: the layer is gone and the marks end all
      // the same, done sees the failure, and the failure is raised. A failure
      // of the held flush surfaces after, as any auto flush's.
      hold(root);
      /** @type {{ error: any } | null} */
      let raised = null;
      try {
        /** @type {Result<R>} */
        let outcome = result;
        /** @type {{ error: any } | null} */
        let failed = null;
        try {
          if (result.ok) optimistic.commit(ticket);
          else optimistic.settle(ticket);
        } catch (error) {
          failed = { error };
          outcome = { ok: false, error };
        } finally {
          release();
        }
        if (typeof done === 'function')
          try {
            done(outcome);
          } catch (also) {
            if (failed === null) throw also;
            // The commit failure is the one raised; done's own goes to the root's boundary.
            const left = $deliver(root, null, also);
            if (left !== null)
              queueMicrotask(() => {
                throw left;
              });
          }
        if (failed !== null) throw failed.error;
      } catch (error) {
        raised = { error };
      }
      try {
        unhold(root);
      } catch (flush) {
        if (raised === null) throw flush;
        // Both failed: the action's failure is raised; the flush's goes the way an unhandled one does.
        queueMicrotask(() => {
          throw flush;
        });
      }
      if (raised !== null) throw raised.error;
    };
    const outer = acting;
    acting = marks;
    try {
      cancel = start(input, complete);
    } catch (error) {
      // Thrown by a complete start called itself (its commit): raised, not lost; the marks still end.
      if (finished) throw error;
      complete({ ok: false, error });
    } finally {
      acting = outer;
      // A mark made after start completed the action has nothing left to wait for.
      if (finished) release();
    }
    return () => {
      if (finished) return;
      finished = true;
      if (root.closed) {
        if (typeof cancel === 'function') cancel();
        return;
      }
      // As complete: the settle's flush is held, so the work is cancelled
      // whatever settle or that flush throws; the first failure is raised after.
      hold(root);
      /** @type {{ error: any } | null} */
      let raised = null;
      try {
        optimistic.settle(ticket);
      } catch (error) {
        raised = { error };
      }
      try {
        release();
      } catch (error) {
        raised ??= { error };
      }
      if (typeof cancel === 'function')
        try {
          cancel();
        } catch (error) {
          raised ??= { error };
        }
      try {
        unhold(root);
      } catch (flush) {
        if (raised === null) throw flush;
        queueMicrotask(() => {
          throw flush;
        });
      }
      if (raised !== null) throw raised.error;
    };
  };
}

/**
 * The change marks of the action whose `start` runs (a register, set only while it
 * runs). @type {(() => void)[] | null}
 */
let acting = null;

/**
 * Declares that `source`, a task or memo, is changing: it reads pending,
 * keeping its value for `$lastReady`, so `$pending` shows it and a transition
 * holds what shows its value. Made while an action
 * starts (`$act`), it ends when the action does; otherwise at the source's
 * next value (`$changing(user); $rerun(user)` reads as pending until the
 * reload lands). The returned function ends it earlier.
 * @param {Task<any> | import('./core.js').Readable<any>} source
 * @returns {() => void}
 */
export function $changing(source) {
  const next = acting === null;
  let done = false;
  if (tasked(source)) {
    const job = source;
    job.beginChange(next);
    const end = () => {
      if (done) return;
      done = true;
      job.endChange(next);
    };
    if (!next) /** @type {(() => void)[]} */ (acting).push(end);
    return end;
  }
  const x = /** @type {any} */ (source);
  beginChange(x, next);
  const end = () => {
    if (done) return;
    done = true;
    endChange(x, next);
  };
  if (!next) /** @type {(() => void)[]} */ (acting).push(end);
  return end;
}

/**
 * True while a shown value waits on newer work for a changed input or a
 * `$changing` mark; a quiet `$rerun` does not count. A value counts once it
 * has been ready, whatever it is (null and undefined too).
 * @param {Source<any>} source
 */
export function $behind(source) {
  // Status as data: an indicator, never held by a transition.
  peeked();
  if (tasked(source)) {
    const state = source.current();
    return state.status === 'pending' && state.behind === true;
  }
  const run = reader(source);
  try {
    run();
    return false;
  } catch (error) {
    if (error !== WAITING) return false;
    const state = waitedOn();
    return state != null && state.behind === true;
  }
}

/**
 * Calls `done(outcome)` once `read` is ready or failed, then stops watching.
 * Returns a stop function for giving up earlier.
 * @template T
 * @param {import('./core.js').Graph} root
 * @param {Source<T>} read
 * @param {(outcome: Outcomes<T>) => void} done
 * @returns {() => void}
 */
export function $whenDone(root, read, done) {
  live(root);
  /** @type {(() => void) | null} */
  let stopper = null;
  let finished = false;
  const quit = () => {
    if (stopper !== null) stopper();
  };
  const watching = $watch(root, () => $outcome(read), outcome => {
    if (!finished && outcome.status !== 'pending') {
      finished = true;
      try {
        done(outcome);
      } finally {
        // Stop watching even when done throws; done may have disposed the root: then nothing is left to stop.
        if (!root.closed) $after(root, quit);
      }
    }
  });
  stopper = watching.stop;
  if (finished) quit();
  return watching.stop;
}

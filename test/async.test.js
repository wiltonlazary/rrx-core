// Async memos: a memo whose computation returns a promise or an async
// iterator: async lives in the graph.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  $root, $signal, $read, $write, $memo, $flush, $dispose, $scope, $enter, $stop, $watch, $settled, $auto,
  $lastReady, $pending, $rerun, $behind, $outcome, $discard, $task, $changing, $optimistic, $act,
} from '../src/index.js';

/** A promise and its settle functions. */
function deferred() {
  /** @type {(value: any) => void} */ let resolve = () => {};
  /** @type {(error: any) => void} */ let reject = () => {};
  const promise = new Promise((ok, fail) => {
    resolve = ok;
    reject = fail;
  });
  return { promise, resolve, reject };
}

// A memo follows its promise with a handler it attached first, so a test that
// awaits the same promise resumes after the memo has taken the completion.

describe('rrx-core async memos', () => {
  it('reads pending until the promise resolves, then its value; readers follow', async () => {
    const root = $root('app');
    $auto(root, true);
    const id = $signal(root, 1);
    /** @type {ReturnType<typeof deferred>[]} */
    const calls = [];
    const user = $memo(root, () => {
      const d = deferred();
      calls.push(d);
      $read(id);
      return d.promise;
    });
    const seen = [];
    $watch(root, () => $outcome(user), outcome => seen.push(outcome.status + ':' + outcome.value));
    assert.equal($pending(user), true);
    assert.equal($lastReady(user), null);
    calls[0].resolve('ada');
    await calls[0].promise;
    assert.equal($read(user), 'ada');
    assert.equal($pending(user), false);
    $write(id, 2);
    assert.equal($pending(user), true, 'a new input reads pending');
    assert.equal($lastReady(user), 'ada', 'keeping the last value for $lastReady');
    assert.equal($behind(user), true);
    calls[1].resolve('grace');
    await calls[1].promise;
    assert.equal($read(user), 'grace');
    assert.deepEqual(seen, ['pending:null', 'ready:ada', 'pending:ada', 'ready:grace']);
    $dispose(root);
  });

  it('ignores a superseded completion', async () => {
    const root = $root('app');
    $auto(root, true);
    const id = $signal(root, 1);
    /** @type {ReturnType<typeof deferred>[]} */
    const calls = [];
    const user = $memo(root, () => {
      const d = deferred();
      calls.push(d);
      $read(id);
      return d.promise;
    });
    $watch(root, () => $outcome(user), () => {});
    $write(id, 2);
    assert.equal(calls.length, 2);
    calls[1].resolve('new');
    calls[0].resolve('old');
    await calls[0].promise;
    assert.equal($read(user), 'new', 'the late completion of the first input is ignored');
    $dispose(root);
  });

  it('brings itself up to date when a completion finds its input changed', async () => {
    const root = $root('app');
    const id = $signal(root, 1);
    /** @type {ReturnType<typeof deferred>[]} */
    const calls = [];
    const user = $memo(root, () => {
      const d = deferred();
      calls.push({ ...d, id: $read(id) });
      return d.promise;
    });
    assert.equal($pending(user), true);
    $write(id, 2); // not flushed, not read: the memo is dirty
    calls[0].resolve('stale');
    await calls[0].promise;
    assert.equal(calls.length, 2, 'the completion recomputed the memo for the new input first');
    assert.equal($pending(user), true, 'and was ignored as superseded');
    calls[1].resolve('fresh');
    await calls[1].promise;
    assert.equal($read(user), 'fresh');
    $dispose(root);
  });

  it('raises a rejection as the memo failure', async () => {
    const root = $root('app');
    const d = deferred();
    const user = $memo(root, () => d.promise);
    assert.equal($outcome(user).status, 'pending');
    d.reject(new Error('down'));
    await d.promise.catch(() => {});
    assert.throws(() => $read(user), /down/);
    assert.equal($outcome(user).status, 'failed');
    $dispose(root);
  });

  it('streams an async iterator: each value in turn, the old one stopped when superseded', async () => {
    const root = $root('app');
    $auto(root, true);
    const id = $signal(root, 1);
    let stopped = 0;
    /** Each iterator's push; it returns the promise its value settles, which the memo follows. @type {((value: number) => Promise<unknown>)[]} */
    const pushes = [];
    const feed = $memo(root, () => {
      const base = $read(id) * 100;
      /** @type {((r: IteratorResult<number>) => void) | null} */
      let waiting = null;
      /** @type {Promise<IteratorResult<number>> | null} */
      let handed = null;
      /** @type {IteratorResult<number>[]} */
      const queue = [];
      pushes.push(v => {
        const r = { done: false, value: base + v };
        if (waiting !== null) {
          const w = waiting;
          waiting = null;
          w(r);
          return /** @type {Promise<IteratorResult<number>>} */ (handed);
        }
        queue.push(r);
        return Promise.resolve(r);
      });
      return {
        [Symbol.asyncIterator]() {
          return this;
        },
        next() {
          const r = queue.shift();
          return r !== undefined ? Promise.resolve(r) : (handed = new Promise(ok => (waiting = ok)));
        },
        return() {
          stopped++;
          return Promise.resolve({ done: true, value: undefined });
        },
      };
    });
    const seen = [];
    $watch(root, () => $outcome(feed), o => seen.push(o.status + ':' + o.value));
    await pushes[0](1);
    await pushes[0](2);
    assert.equal($read(feed), 102);
    assert.equal($pending(feed), false, 'ready from the first value on');
    $write(id, 2);
    assert.equal(stopped, 1, 'the superseded iterator is stopped');
    pushes[0](3);
    await pushes[1](1);
    assert.equal($read(feed), 201);
    assert.deepEqual(seen, ['pending:null', 'ready:101', 'ready:102', 'pending:102', 'ready:201']);
    $dispose(root);
  });

  it('reruns quietly: the value stays shown, not pending, until the new one', async () => {
    const root = $root('app');
    $auto(root, true);
    /** @type {ReturnType<typeof deferred>[]} */
    const calls = [];
    const list = $memo(root, () => {
      const d = deferred();
      calls.push(d);
      return d.promise;
    });
    const seen = [];
    $watch(root, () => $outcome(list), o => seen.push(o.status + ':' + o.value));
    calls[0].resolve('a');
    await calls[0].promise;
    $rerun(list);
    assert.equal(calls.length, 2, 'the work runs again');
    assert.equal($pending(list), false, 'a rerun is quiet');
    assert.equal($read(list), 'a');
    calls[1].resolve('b');
    await calls[1].promise;
    assert.deepEqual(seen, ['pending:null', 'ready:a', 'ready:b']);
    $dispose(root);
  });

  it('reruns a task quietly too; a changed input still reads pending', () => {
    const root = $root('app');
    const id = $signal(root, 1);
    /** @type {((r: any) => void)[]} */
    const done = [];
    const task = $task(root, () => $read(id), (_, complete) => void done.push(complete));
    done[0]({ ok: true, value: 'a' });
    $flush(root);
    $rerun(task);
    assert.equal(done.length, 2, 'the work runs again');
    assert.equal($pending(task), false, 'quiet: the ready value stays');
    assert.equal($read(task), 'a');
    done[1]({ ok: true, value: 'b' });
    $flush(root);
    assert.equal($read(task), 'b');
    $write(id, 2);
    assert.equal($pending(task), true, 'a new input reads pending');
    $dispose(root);
  });

  it('reads $behind once a value has been ready, null and undefined too', async () => {
    const root = $root('app');
    $auto(root, true);
    const id = $signal(root, 1);
    /** @type {ReturnType<typeof deferred>[]} */
    const calls = [];
    const user = $memo(root, () => {
      const d = deferred();
      calls.push(d);
      $read(id);
      return d.promise;
    });
    const derived = () => $read(user);
    assert.equal($behind(user), false, 'the first load shows nothing yet');
    calls[0].resolve(null);
    await calls[0].promise;
    assert.equal($behind(user), false, 'ready');
    $rerun(user);
    $read(user);
    assert.equal($behind(user), false, 'a quiet rerun does not count');
    calls[1].resolve(undefined);
    await calls[1].promise;
    $write(id, 2);
    assert.equal($pending(user), true);
    assert.equal($behind(user), true, 'a shown undefined waits on newer work');
    assert.equal($behind(derived), true, 'through a computation too');
    calls[2].resolve(null);
    await calls[2].promise;
    const end = $changing(user);
    assert.equal($behind(user), true, 'a $changing mark keeps a shown null behind');
    end();
    assert.equal($behind(user), false);
    $dispose(root);
  });

  it('reads $behind on a task once a value has been ready, null too', () => {
    const root = $root('app');
    const id = $signal(root, 1);
    /** @type {((r: any) => void)[]} */
    const done = [];
    const task = $task(root, () => $read(id), (_, complete) => void done.push(complete));
    assert.equal($behind(task), false, 'the first load shows nothing yet');
    $write(id, 2);
    $flush(root);
    assert.equal($behind(task), false, 'a new input before any value is still a first load');
    done[1]({ ok: true, value: null });
    $flush(root);
    $rerun(task);
    assert.equal($behind(task), false, 'a quiet rerun does not count');
    done[2]({ ok: true, value: null });
    $flush(root);
    $write(id, 3);
    $flush(root);
    assert.equal($behind(task), true, 'a shown null waits on newer work');
    assert.equal($behind(() => $read(task)), true, 'through a computation too');
    done[3]({ ok: true, value: undefined });
    $flush(root);
    assert.equal($behind(task), false);
    const end = $changing(task);
    assert.equal($behind(task), true, 'a $changing mark keeps a shown undefined behind');
    end();
    assert.equal($behind(task), false);
    $dispose(root);
  });

  it('counts pending work for $settled, and ends it when the memo is freed', async () => {
    const root = $root('app');
    $auto(root, true);
    const d = deferred();
    const scope = $scope(root, null);
    const user = $enter(scope, () => $memo(root, () => d.promise));
    let settled = 0;
    $settled(root, () => settled++);
    $watch(root, () => $outcome(user), () => {});
    assert.equal(settled, 0, '$settled waits for the async memo');
    d.resolve('x');
    await d.promise;
    assert.equal(settled, 1);
    const e = deferred();
    const again = $enter(scope, () => $memo(root, () => e.promise));
    $watch(root, () => $outcome(again), () => {});
    $settled(root, () => settled++);
    $stop(scope);
    $flush(root);
    assert.equal(settled, 2, 'a freed memo no longer counts as pending');
    $dispose(root);
  });

  it('takes a thenable that completes during the computation as its result', () => {
    const root = $root('app');
    const now = $memo(root, () => ({ then: (/** @type {(v: number) => void} */ ok) => ok(7) }));
    assert.equal($read(now), 7);
    assert.equal($pending(now), false);
    $discard(now);
    $dispose(root);
  });
});

describe('rrx-core changing', () => {
  for (const memo of [false, true]) for (const synchronous of [false, true]) {
    it(`refreshes ${memo ? 'memo' : 'direct'} input before releasing a change mark with ${synchronous ? 'synchronous' : 'pending'} replacement work`, () => {
      const root = $root('app');
      const input = $signal(root, 1);
      const source = memo ? $memo(root, () => $read(input)) : input;
      /** @type {((result: any) => void)[]} */
      const done = [];
      const task = $task(root, source, (value, complete) => {
        done.push(complete);
        if (synchronous) complete({ ok: true, value: value * 10 });
      });
      if (!synchronous) done[0]({ ok: true, value: 10 });
      const release = $changing(task);
      const releaseAnother = $changing(task);
      /** @type {any[]} */
      const seen = [];
      $watch(root, () => $outcome(task), state => void seen.push(state));
      $flush(root);
      $write(input, 2);
      release();
      releaseAnother();
      assert.equal(done.length, 2, 'release brings the changed input up to date');
      const expected = synchronous
        ? { status: 'ready', value: 20, error: null }
        : { status: 'pending', value: 10, error: null };
      assert.deepEqual($outcome(task), expected);
      $flush(root);
      assert.deepEqual(seen.at(-1), expected, 'observers agree with a direct read');
      if (!synchronous) {
        done[1]({ ok: true, value: 20 });
        $flush(root);
      }
      assert.equal($read(task), 20);
      const releaseFresh = $changing(task);
      assert.equal($pending(task), true, 'completion and release did not consume a future change mark');
      releaseFresh();
      assert.equal($pending(task), false);
      $dispose(root);
    });
  }

  it('does not relaunch a stopped task when its change mark is released', () => {
    const root = $root('app');
    const input = $signal(root, 1);
    const scope = $scope(root, null);
    let launches = 0;
    const task = $enter(scope, () => $task(root, input, (value, complete) => {
      launches++;
      complete({ ok: true, value });
    }));
    const release = $changing(task);
    $write(input, 2);
    $stop(scope);
    assert.doesNotThrow(release);
    $flush(root);
    assert.equal(launches, 1);
    $dispose(root);
  });

  it('pends a task until its next value: changing then rerun reads as a reload', () => {
    const root = $root('app');
    /** @type {((r: any) => void)[]} */
    const done = [];
    const task = $task(root, () => 1, (_, complete) => void done.push(complete));
    done[0]({ ok: true, value: 'a' });
    $flush(root);
    $changing(task);
    $rerun(task);
    assert.equal($pending(task), true, 'changing: the reload reads pending');
    assert.equal($lastReady(task), 'a');
    done[1]({ ok: true, value: 'b' });
    $flush(root);
    assert.equal($pending(task), false);
    assert.equal($read(task), 'b');
    $dispose(root);
  });

  it('pends what an action marks changing until the action completes', () => {
    const root = $root('app');
    const base = $signal(root, ['milk']);
    const list = $optimistic(root, base);
    /** @type {((r: any) => void)[]} */
    const fetched = [];
    const server = $task(root, () => 1, (_, complete) => void fetched.push(complete));
    fetched[0]({ ok: true, value: 3 });
    $flush(root);
    /** @type {((r: any) => void)[]} */
    const saving = [];
    const add = $act(root, list, (items, item) => [...items, item], (item, complete) => {
      $changing(server);
      saving.push(complete);
    });
    add('eggs');
    assert.deepEqual($read(list.value), ['milk', 'eggs'], 'the optimistic value shows at once');
    assert.equal($pending(server), true, 'the count marked changing pends while the action runs');
    assert.equal($lastReady(server), 3);
    saving[0]({ ok: true, value: null });
    assert.equal($pending(server), false, 'and reads ready when it completes');
    assert.equal($read(server), 3);
    $dispose(root);
  });

  it('pends a memo, ended by its release function', () => {
    const root = $root('app');
    const n = $signal(root, 2);
    const twice = $memo(root, () => $read(n) * 2);
    assert.equal($read(twice), 4);
    const release = $changing(twice);
    assert.equal($pending(twice), true);
    assert.equal($lastReady(twice), 4);
    release();
    assert.equal($pending(twice), false);
    assert.equal($read(twice), 4);
    $changing(twice);
    $write(n, 3);
    assert.equal($read(twice), 6, 'the next value ends a change mark made outside an action');
    assert.throws(() => $changing($signal(root, 1)), /task or memo/);
    $dispose(root);
  });
});

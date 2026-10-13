// Background disposal: `$retire` makes a scope inert at once, `$sweep` frees it in
// time-bounded slices. The scope tests run against the records core and the
// column arena (the same contract); lists and tasks exist in the records
// build only (the arena is the graph algorithm, kept for measurement).

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import * as records from '../src/index.js';
import * as arena from '../src/arena.js';

for (const [name, g] of /** @type {[string, any][]} */ ([['records', records], ['arena', arena]])) {
  const {
    $root, $signal, $read, $write, $memo, $effect, $watch, $render, $flush, $dispose, $scope, $enter, $stop, $stats, $cleanup,
    $catch, $alive, $commit, $retire, $sweep, $sweeper,
  } = g;

  /** A root whose sweeps only run when a test calls `$sweep`. */
  const manual = () => {
    const root = $root('app');
    $sweeper(root, null);
    return root;
  };

  /** Sweeps in slices until nothing is left; returns the number of slices. @param {any} root @param {number} budget */
  const drain = (root, budget) => {
    let slices = 1;
    while ($sweep(root, budget) > 0) slices++;
    return slices;
  };

  describe(`rrx-core retire and sweep (${name})`, () => {
    it('makes 10k scopes inert at once and frees them in slices, in $stop order', () => {
      const root = manual();
      const source = $signal(root, 0);
      const baseline = $stats(root);
      /** @type {string[]} */
      const order = [];
      let runs = 0;
      const scopes = [];
      for (let i = 0; i < 10_000; i++) {
        const scope = $scope(root, null);
        $enter(scope, () => {
          const memo = $memo(root, () => $read(source) + i);
          $effect(root, () => {
            $read(memo);
            runs++;
          });
          $cleanup(scope, () => order.push(`first ${i}`));
          $cleanup(scope, () => order.push(`second ${i}`));
        });
        scopes.push(scope);
      }
      assert.equal(runs, 10_000);
      for (const scope of scopes) $retire(scope);
      assert.equal($alive(scopes[0]), false, '$alive is false right after $retire');
      $write(source, 1);
      $flush(root);
      assert.equal(runs, 10_000, 'no retired effect reruns');
      assert.deepEqual(order, [], 'nothing is freed before a sweep');
      const slices = drain(root, 1);
      assert.ok(slices > 1, `the sweep took slices (${slices})`);
      assert.equal(order.length, 20_000, 'every cleanup ran once');
      assert.deepEqual(order.slice(0, 4), ['second 0', 'first 0', 'second 1', 'first 1'], 'newest first, scope by scope');
      assert.deepEqual($stats(root), baseline, 'the graph is back to its baseline');
      assert.equal($sweep(root), 0);
      $dispose(root);
    });

    it('stops nested scopes inside a retired one, newest first, then its cleanups', () => {
      const root = manual();
      /** @type {string[]} */
      const order = [];
      const outer = $scope(root, null);
      $enter(outer, () => {
        $cleanup(outer, () => order.push('outer'));
        const inner = $scope(root, outer);
        $cleanup(inner, () => order.push('inner'));
        $enter(inner, () => {
          $effect(root, () => () => order.push('effect'));
        });
      });
      $retire(outer);
      drain(root, 4);
      assert.deepEqual(order, ['effect', 'inner', 'outer']);
      $dispose(root);
    });

    it('keeps retired memos, Watch, Render and `$commit` hooks from running', () => {
      const root = manual();
      const source = $signal(root, 1);
      const scope = $scope(root, null);
      let computed = 0;
      /** @type {number[]} */
      const applied = [];
      let commits = 0;
      const memo = $enter(scope, () => {
        const m = $memo(root, () => {
          computed++;
          return $read(source) * 10;
        });
        $watch(root, () => $read(m), value => void applied.push(value));
        $render(root, () => $read(source), value => void applied.push(-value));
        $commit(root, scope, () => void commits++);
        return m;
      });
      $flush(root);
      const seen = { computed, applied: applied.length, commits };
      $retire(scope);
      $write(source, 2);
      $flush(root);
      assert.deepEqual({ computed, applied: applied.length, commits }, seen, 'nothing of the scope ran');
      assert.equal($read(memo), 10, 'a retired memo read from outside keeps its last value');
      drain(root, 4);
      assert.equal($read(memo), 10, 'a freed memo reads its final value');
      $dispose(root);
    });

    it('delivers throwing cleanups to the nearest $catch and sweeps the rest', () => {
      const root = manual();
      /** @type {string[]} */
      const seen = [];
      /** @type {number[]} */
      const ran = [];
      const parent = $scope(root, null);
      $catch(parent, error => seen.push(error.message));
      const scopes = [];
      for (let i = 0; i < 5; i++) {
        const scope = $scope(root, parent);
        $cleanup(scope, () => ran.push(i));
        $cleanup(scope, () => {
          throw new Error(`cleanup ${i}`);
        });
        scopes.push(scope);
      }
      for (const scope of scopes) $retire(scope);
      drain(root, 4);
      assert.deepEqual(seen, ['cleanup 0', 'cleanup 1', 'cleanup 2', 'cleanup 3', 'cleanup 4']);
      assert.deepEqual(ran, [0, 1, 2, 3, 4], 'every other cleanup still ran');

      // Unhandled: the slice finishes its work, then raises the first failure.
      const bare = manual();
      const a = $scope(bare, null);
      const b = $scope(bare, null);
      let after = 0;
      $cleanup(a, () => {
        throw new Error('bare');
      });
      $cleanup(b, () => void after++);
      $retire(a);
      $retire(b);
      assert.throws(() => $sweep(bare, 100), /bare/);
      assert.equal(after, 1, 'the sweep went on past the failure');
      assert.equal($sweep(bare), 0);
      $dispose(root);
      $dispose(bare);
    });

    it('finishes the teardown at once on $stop or $dispose', () => {
      const root = manual();
      const baseline = $stats(root);
      let cleaned = 0;
      const scopes = [];
      for (let i = 0; i < 100; i++) {
        const scope = $scope(root, null);
        $enter(scope, () => {
          $effect(root, () => () => void cleaned++);
          $cleanup(scope, () => void cleaned++);
        });
        scopes.push(scope);
      }
      for (const scope of scopes) $retire(scope);
      // Part of the work swept, the rest stopped synchronously.
      $sweep(root, 0);
      const partly = cleaned;
      assert.ok(partly > 0 && partly < 200, `one chunk swept (${partly})`);
      for (const scope of scopes) $stop(scope);
      assert.equal(cleaned, 200, '$stop finished every retired scope');
      assert.deepEqual($stats(root), baseline);
      assert.equal($sweep(root), 0, 'nothing is left to sweep');

      const other = manual();
      let count = 0;
      for (let i = 0; i < 10; i++) {
        const scope = $scope(other, null);
        $cleanup(scope, () => void count++);
        $retire(scope);
      }
      $dispose(other);
      assert.equal(count, 10, '$dispose ran every retired cleanup');
      assert.equal($sweep(other), 0);
      $dispose(root);
    });

    it('never sweeps inside a flush and yields one chunk to queued work', () => {
      const root = manual();
      const source = $signal(root, 0);
      let inside = -1;
      $effect(root, () => {
        if ($read(source) > 0) inside = $sweep(root, 100);
      });
      const scope = $scope(root, null);
      let cleaned = 0;
      $cleanup(scope, () => void cleaned++);
      $retire(scope);
      $write(source, 1);
      $flush(root);
      assert.equal(inside, 1, 'a sweep inside a flush does nothing');
      assert.equal(cleaned, 0);
      assert.equal($sweep(root), 0);
      assert.equal(cleaned, 1);
      $dispose(root);
    });

    it('tells the host scheduler once when retired work appears', () => {
      const root = $root('app');
      let calls = 0;
      $sweeper(root, () => void calls++);
      const a = $scope(root, null);
      const b = $scope(root, null);
      $retire(a);
      $retire(b);
      assert.equal(calls, 1, 'one call for a batch of retirements');
      assert.equal($sweep(root), 0);
      $retire($scope(root, null));
      assert.equal(calls, 2, 'called again once the queue had drained');
      $retire(a);
      assert.equal(calls, 2, 'retiring twice does nothing');
      $dispose(root);
    });

    it('drains on its own with the default scheduler', async () => {
      const root = $root('app');
      let cleaned = 0;
      for (let i = 0; i < 1000; i++) {
        const scope = $scope(root, null);
        $cleanup(scope, () => void cleaned++);
        $retire(scope);
      }
      assert.equal(cleaned, 0);
      // timer: poll, the default drain runs on macrotasks and reports nothing when done.
      for (let i = 0; i < 100 && cleaned < 1000; i++) await new Promise(resolve => setTimeout(resolve, 1));
      assert.equal(cleaned, 1000);
      $dispose(root);
    });

    it('sweeps in idle periods where the host has idle callbacks', () => {
      const host = /** @type {any} */ (globalThis);
      /** @type {((deadline: any) => void)[]} */
      const queue = [];
      /** @type {any[]} */
      const options = [];
      host.requestIdleCallback = (/** @type {any} */ callback, /** @type {any} */ option) => {
        queue.push(callback);
        options.push(option);
      };
      try {
        const root = $root('app');
        let cleaned = 0;
        for (let i = 0; i < 2000; i++) {
          const scope = $scope(root, null);
          $cleanup(scope, () => void cleaned++);
          $retire(scope);
        }
        assert.equal(queue.length, 1, 'one idle callback for a batch of retirements');
        assert.ok(options[0].timeout > 0, 'with a timeout, so a page that is never idle still sweeps');
        // An idle period with no time left frees one chunk, and asks again.
        /** @type {any} */ (queue.shift())({ didTimeout: false, timeRemaining: () => 0 });
        assert.ok(cleaned > 0 && cleaned < 2000, `one chunk (${cleaned})`);
        assert.equal(queue.length, 1, 'the drain asks for the next idle period');
        for (let i = 0; i < 1000 && queue.length > 0; i++) /** @type {any} */ (queue.shift())({ didTimeout: i % 2 === 0, timeRemaining: () => 1 });
        assert.equal(cleaned, 2000);
        assert.equal(queue.length, 0, 'nothing is asked once the work is done');
        $dispose(root);
      } finally {
        delete host.requestIdleCallback;
      }
    });

    it('keeps draining after a slice raises an unhandled cleanup failure', () => {
      const host = /** @type {any} */ (globalThis);
      /** @type {((deadline: any) => void)[]} */
      const queue = [];
      host.requestIdleCallback = (/** @type {any} */ callback) => void queue.push(callback);
      try {
        const root = $root('app');
        const bad = $scope(root, null);
        $cleanup(bad, () => { throw new Error('boom'); });
        $retire(bad);
        let cleaned = 0;
        for (let i = 0; i < 2000; i++) {
          const scope = $scope(root, null);
          $cleanup(scope, () => void cleaned++);
          $retire(scope);
        }
        assert.throws(() => /** @type {any} */ (queue.shift())({ didTimeout: false, timeRemaining: () => 0 }), /boom/);
        assert.equal(queue.length, 1, 'the failing slice still asks for the next one');
        for (let i = 0; i < 1000 && queue.length > 0; i++) /** @type {any} */ (queue.shift())({ didTimeout: false, timeRemaining: () => 1 });
        assert.equal(cleaned, 2000);
        assert.equal(queue.length, 0);
        $dispose(root);
      } finally {
        delete host.requestIdleCallback;
      }
    });
  });
}

const {
  $root, $signal, $read, $write, $effect, $flush, $dispose, $scope, $enter, $stop, $cleanup, $outcome,
  $alive, $owner, $retire, $sweep, $sweeper, $keyed, $rows, $positions, $task, $auto, $settled,
} = records;

/** A root whose sweeps only run when a test calls `$sweep`. */
const manual = () => {
  const root = $root('app');
  $sweeper(root, null);
  return root;
};

/** Sweeps in slices until nothing is left; returns the number of slices. @param {any} root @param {number} budget */
const drain = (root, budget) => {
  let slices = 1;
  while ($sweep(root, budget) > 0) slices++;
  return slices;
};

describe('rrx-core retire and sweep: lists and tasks (records)', () => {
  it('retires removed Keyed, Rows and Positions rows; a returning key builds a fresh row', () => {
    for (const kind of ['Keyed', 'Rows', 'Positions']) {
      const root = manual();
      const items = $signal(root, ['a', 'b']);
      /** @type {string[]} */
      const built = [];
      /** @type {string[]} */
      const cleaned = [];
      let reads = 0;
      /** @param {() => string} item */
      const build = item => {
        const name = item();
        built.push(name);
        $cleanup(/** @type {any} */ ($owner(root)), () => void cleaned.push(name));
        $effect(root, () => {
          item();
          reads++;
        });
        return name;
      };
      const list =
        kind === 'Keyed' ? $keyed(root, () => $read(items), null, item => build(() => $read(item)), { retire: true })
          : kind === 'Rows' ? $rows(root, () => $read(items), null, build, { retire: true })
            : $positions(root, () => $read(items), item => build(() => $read(item)), { retire: true });
      $effect(root, () => void $read(list));
      $write(items, ['a']);
      $flush(root);
      assert.deepEqual($read(list), ['a'], kind);
      assert.deepEqual(cleaned, [], `${kind}: the removed row waits for a sweep`);
      $write(items, ['a', 'b']);
      $flush(root);
      assert.deepEqual(built, ['a', 'b', 'b'], `${kind}: the returning key built a fresh row`);
      assert.deepEqual($read(list), ['a', 'b'], kind);
      assert.equal($sweep(root), 0);
      assert.deepEqual(cleaned, ['b'], `${kind}: the retired row was swept`);
      $dispose(root);
    }
  });

  it('retires every row at once when a list update keeps none of them', () => {
    for (const kind of ['Keyed', 'Rows', 'Positions']) {
      for (const next of [[], ['x', 'y']]) {
        if (kind === 'Positions' && next.length > 0) continue;
        const root = manual();
        const items = $signal(root, ['a', 'b', 'c']);
        /** @type {any[]} */
        const scopes = [];
        /** @type {string[]} */
        const cleaned = [];
        let runs = 0;
        /** @param {() => string} item */
        const build = item => {
          const name = item();
          const scope = /** @type {any} */ ($owner(root));
          scopes.push(scope);
          $cleanup(scope, () => void cleaned.push(name));
          $effect(root, () => {
            item();
            runs++;
          });
          return name;
        };
        const owner = $scope(root, null);
        const list = $enter(owner, () =>
          kind === 'Keyed' ? $keyed(root, () => $read(items), null, item => build(() => $read(item)), { retire: true })
            : kind === 'Rows' ? $rows(root, () => $read(items), null, build, { retire: true })
              : $positions(root, () => $read(items), item => build(() => $read(item)), { retire: true }));
        $effect(root, () => void $read(list));
        const old = scopes.slice();
        const box = old[0].owner;
        assert.ok(old.every(s => s.owner === box) && box !== owner, `${kind}: the rows share one container`);
        $write(items, next);
        $flush(root);
        const label = `${kind} to [${next}]`;
        assert.deepEqual($read(list), next, label);
        assert.equal($alive(box), false, `${label}: the container was retired`);
        assert.ok(old.every(s => !$alive(s)), `${label}: every old row is inert at once`);
        assert.equal($alive(owner), true, label);
        assert.deepEqual(cleaned, [], `${label}: the rows wait for a sweep`);
        const fresh = scopes.slice(old.length);
        assert.ok(fresh.every(s => $alive(s) && s.owner !== box), `${label}: new rows live in a new container`);
        const before = runs;
        $write(items, ['a']);
        $flush(root);
        assert.deepEqual($read(list), ['a'], label);
        assert.ok(runs >= before, label);
        drain(root, 4);
        assert.deepEqual([...cleaned].sort(), [...['a', 'b', 'c'], ...next].sort(), `${label}: the sweep ran every old row's cleanup`);
        $stop(owner);
        assert.equal(cleaned.length, 3 + next.length + 1, `${label}: stopping the owner stops the live rows`);
        $dispose(root);
      }
    }
  });

  it('retires only updates that remove at least the threshold (retire: n)', () => {
    for (const kind of ['Keyed', 'Rows', 'Positions']) {
      const root = manual();
      const items = $signal(root, ['a', 'b', 'c', 'd', 'e']);
      /** @type {string[]} */
      const cleaned = [];
      /** @param {() => string} item */
      const build = item => {
        const name = item();
        $cleanup(/** @type {any} */ ($owner(root)), () => void cleaned.push(name));
        return name;
      };
      const removal = { retire: 3 };
      const list =
        kind === 'Keyed' ? $keyed(root, () => $read(items), null, item => build(() => $read(item)), removal)
          : kind === 'Rows' ? $rows(root, () => $read(items), null, build, removal)
            : $positions(root, () => $read(items), item => build(() => $read(item)), removal);
      $effect(root, () => void $read(list));
      $write(items, ['a', 'b', 'c', 'd']);
      $flush(root);
      assert.deepEqual(cleaned, ['e'], `${kind}: one removed row, below the threshold, stops at once`);
      $write(items, ['a']);
      $flush(root);
      assert.deepEqual($read(list), ['a'], kind);
      assert.deepEqual(cleaned, ['e'], `${kind}: three removed rows retire and wait for a sweep`);
      drain(root, 4);
      assert.deepEqual([...cleaned].sort(), ['b', 'c', 'd', 'e'], `${kind}: the sweep ran their cleanups`);
      $write(items, []);
      $flush(root);
      assert.equal(cleaned.at(-1), 'a', `${kind}: clearing one row, below the threshold, stops it at once`);
      $dispose(root);
    }
  });

  it('ignores task completions that land in a retired scope', () => {
    const root = manual();
    const input = $signal(root, 1);
    /** @type {((result: any) => void)[]} */
    const pending = [];
    const scope = $scope(root, null);
    const task = $enter(scope, () => $task(root, () => $read(input), (_value, complete) => void pending.push(complete)));
    $flush(root);
    $retire(scope);
    pending[0]({ ok: true, value: 42 });
    $flush(root);
    assert.equal($outcome(task).status, 'pending', 'the late completion was ignored');
    $dispose(root);
  });

  it('runs $settled under $auto once a stop or a sweep ends the last pending task', async () => {
    for (const how of ['stop', 'sweep', 'default drain']) {
      const root = how === 'sweep' ? manual() : $root('app');
      $auto(root, true);
      const scope = $scope(root, null);
      $enter(scope, () => $task(root, () => 1, () => () => {}));
      let settled = 0;
      $settled(root, () => settled++);
      $flush(root);
      assert.equal(settled, 0, `${how}: the task is pending`);
      if (how === 'stop') $stop(scope);
      else $retire(scope);
      if (how === 'sweep') drain(root, 4);
      if (how === 'default drain')
        // timer: poll, the default drain runs on macrotasks and reports nothing when done.
        for (let i = 0; i < 100 && root.loading > 0; i++) await new Promise(done => setTimeout(done, 1));
      assert.equal(root.loading, 0, `${how}: nothing is pending`);
      assert.equal(settled, 1, `${how}: $settled ran without a $flush`);
      $dispose(root);
    }
  });
});

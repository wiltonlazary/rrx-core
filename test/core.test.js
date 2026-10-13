// Core semantics of rrx-core, run against the production record core and the
// column arena (the same contract).

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import * as records from '../src/core.js';
import * as arena from '../src/arena.js';

for (const [name, g] of /** @type {[string, any][]} */ ([['records', records], ['arena', arena]])) {
  const {
    $root, $signal, $read, $write, $memo, $scan, $derive, $effect, $cancel, $watch, $render, $untrack, $observer, $batch, $batching,
    $flush, $auto, $touch, $kind, $whenUnwatched, $writable, $after, $settled, $defer, $deferred, $commit, $scope, $enter, $stop,
    $cleanup, $release, $cleanups, $alive, $mount, $mounts, $unowned, $context, $provide, $lookup, $update, $entries, $with, $catch, $throw, $deliver,
    $sources, $observers, $stats, $identity, $discard, $invalidate, $dispose, $closed, $owner, $equals,
  } = g;

  describe(`rrx-core (${name})`, () => {
    it('reads and writes signals, storing closures as values', () => {
      const root = $root('app');
      const f = () => 1;
      const s = $signal(root, f);
      assert.equal($read(s), f);
      const h = () => 2;
      assert.equal($write(s, h), h);
      assert.equal($read(s), h);
      $dispose(root);
    });

    it('computes memos lazily, fresh before a flush', () => {
      const root = $root('app');
      const s = $signal(root, 1);
      let runs = 0;
      const m = $memo(root, () => {
        runs++;
        return $read(s) * 2;
      });
      assert.equal(runs, 0);
      assert.equal($read(m), 2);
      assert.equal($read(m), 2);
      assert.equal(runs, 1);
      $write(s, 2);
      assert.equal(runs, 1, 'invalidation never evaluates');
      assert.equal($read(m), 4, 'fresh memo before flush');
      $dispose(root);
    });

    it('runs effects at creation and at each flush, not before it', () => {
      const root = $root('app');
      const s = $signal(root, 0);
      const seen = [];
      $effect(root, () => {
        seen.push($read(s));
      });
      $write(s, 1);
      $write(s, 2);
      assert.deepEqual(seen, [0]);
      $flush(root);
      assert.deepEqual(seen, [0, 2], 'writes coalesce until $flush');
      $flush(root);
      assert.deepEqual(seen, [0, 2], 'an empty flush does nothing');
      $dispose(root);
    });

    it('flushes each write under $auto, and once per $batch', () => {
      const root = $root('app');
      $auto(root, true);
      const s = $signal(root, 0);
      const seen = [];
      $effect(root, () => {
        seen.push($read(s));
      });
      $write(s, 1);
      $write(s, 2);
      assert.deepEqual(seen, [0, 1, 2]);
      $batch(root, () => {
        $write(s, 3);
        $write(s, 4);
        assert.equal($batching(root), true);
      });
      assert.equal($batching(root), false);
      assert.deepEqual(seen, [0, 1, 2, 4]);
      $dispose(root);
    });

    it('settles a diamond once and consistently', () => {
      const root = $root('app');
      const s = $signal(root, 1);
      const a = $memo(root, () => $read(s) + 1);
      const b = $memo(root, () => $read(s) * 10);
      const sum = $memo(root, () => $read(a) + $read(b));
      const seen = [];
      $effect(root, () => {
        seen.push($read(sum));
      });
      $write(s, 2);
      $flush(root);
      assert.deepEqual(seen, [12, 23]);
      $dispose(root);
    });

    it('stops at a memo that recomputes to an equal value', () => {
      const root = $root('app');
      const s = $signal(root, 1);
      const parity = $memo(root, () => $read(s) % 2);
      let computed = 0;
      const label = $memo(root, () => {
        computed++;
        return $read(parity) === 0 ? 'even' : 'odd';
      });
      let runs = 0;
      $effect(root, () => {
        $read(label);
        runs++;
      });
      $write(s, 3);
      $flush(root);
      assert.equal(runs, 1);
      assert.equal(computed, 1);
      $write(s, 4);
      $flush(root);
      assert.equal(runs, 2);
      $dispose(root);
    });

    it('follows unstable dependencies', () => {
      const root = $root('app');
      const flag = $signal(root, true);
      const a = $signal(root, 'a');
      const b = $signal(root, 'b');
      let runs = 0;
      const m = $memo(root, () => {
        runs++;
        return $read(flag) ? $read(a) : $read(b);
      });
      const seen = [];
      $effect(root, () => {
        seen.push($read(m));
      });
      $write(b, 'B');
      $flush(root);
      assert.equal(runs, 1, 'an unread branch does not recompute');
      $write(flag, false);
      $flush(root);
      $write(a, 'A');
      $flush(root);
      assert.deepEqual(seen, ['a', 'B']);
      assert.deepEqual($sources(m).map(x => $read(x)), [false, 'B']);
      $dispose(root);
    });

    it('propagates and checks a chain of 10 000 memos without recursion', () => {
      const root = $root('app');
      const s = $signal(root, 0);
      let last = s;
      for (let i = 0; i < 10000; i++) {
        const previous = last;
        last = $memo(root, () => $read(previous) + 1);
        $read(last);
      }
      const tail = last;
      let value = -1;
      $effect(root, () => {
        value = $read(tail);
      });
      assert.equal(value, 10000);
      $write(s, 5);
      $flush(root);
      assert.equal(value, 10005);
      $write(s, 6);
      assert.equal($untrack(root, () => $read(tail)), 10006, 'a pending chain revalidates on read');
      $dispose(root);
    });

    it('keeps unobserved memos detached and revalidates them by version', () => {
      const root = $root('app');
      const s = $signal(root, 1);
      const other = $signal(root, 0);
      let computed = 0;
      const double = $memo(root, () => {
        computed++;
        return $read(s) * 2;
      });
      assert.equal($read(double), 2);
      assert.equal($observers(s), 0, 'an unobserved memo does not watch');
      assert.equal($stats(root).effects, 0);
      assert.equal($read(double), 2);
      assert.equal(computed, 1, 'an unchanged memo is reused');
      $write(s, 2);
      assert.equal($read(double), 4);
      assert.equal(computed, 2);
      $write(other, 1);
      assert.equal($read(double), 4);
      assert.equal(computed, 2, 'an unrelated write keeps the memo');
      const seen = [];
      const e = $effect(root, () => {
        seen.push($read(double));
      });
      assert.equal($observers(s), 1);
      assert.equal(computed, 2, 'attaching does not recompute');
      $write(s, 3);
      $flush(root);
      assert.deepEqual(seen, [4, 6]);
      $cancel(e);
      assert.equal($observers(s), 0, 'the last observer leaving detaches');
      assert.equal($stats(root).effects, 0);
      assert.equal($read(double), 6);
      assert.equal(computed, 3);
      $dispose(root);
    });

    it('reports a self-triggering effect as a cycle', () => {
      const root = $root('app');
      const s = $signal(root, 0);
      $effect(root, () => {
        const v = $read(s);
        if (v > 0) $write(s, v + 1);
      });
      $write(s, 1);
      assert.throws(() => $flush(root), /reactive cycle/);
      $dispose(root);
    });

    it('grows a lone queued effect into an ordered reentrant batch', () => {
      for (const deferred of [false, true]) {
        const root = $root('app');
        const first = $signal(root, 0);
        const second = $signal(root, 0);
        const order = [];
        $effect(root, () => order.push(`first:${$read(first)}:${$deferred(root)}`));
        $effect(root, () => {
          const value = $read(second);
          order.push(`second:${value}:${$deferred(root)}`);
          if (value === 1) {
            $write(second, 2);
            $write(first, 1);
          }
        });
        order.length = 0;
        if (deferred) $defer(root, () => $write(second, 1));
        else $write(second, 1);
        $flush(root);
        assert.deepEqual(order, [`second:1:${deferred}`, `first:1:${deferred}`, `second:2:${deferred}`]);
        $flush(root);
        assert.equal(order.length, 3, 'the consumed single slot never runs again');
        $dispose(root);
      }
    });

    if (typeof g.pull === 'function') it('ignores a stale low slot after pulling and promoting its observer', () => {
      const root = $root('app');
      const input = $signal(root, 0);
      const seen = [];
      const watch = $watch(root, () => $read(input), value => seen.push([value, $deferred(root)]));
      $defer(root, () => $write(input, 1));
      g.pull(root, watch.observer);
      $write(input, 2);
      $flush(root);
      assert.deepEqual(seen, [[0, false], [1, false], [2, false]]);
      $defer(root, () => $write(input, 3));
      $flush(root);
      assert.deepEqual(seen, [[0, false], [1, false], [2, false], [3, true]], 'the queue remains usable after the stale slot');
      $dispose(root);
    });

    it('treats wide fan-out as ordinary work', () => {
      const root = $root('app');
      const s = $signal(root, 0);
      let total = 0;
      for (let i = 0; i < 1500; i++)
        $effect(root, () => {
          total += $read(s);
        });
      $write(s, 1);
      $flush(root);
      assert.equal(total, 1500);
      $dispose(root);
    });

    it('runs an owner before the children it may dispose', () => {
      const root = $root('app');
      const shown = $signal(root, true);
      const owner = $scope(root, null);
      let childRuns = 0;
      let child = null;
      $enter(owner, () =>
        $effect(root, () => {
          if (child !== null) $stop(child);
          child = $scope(root, $owner(root));
          $enter(child, () =>
            $effect(root, () => {
              $read(shown);
              childRuns++;
            }),
          );
          $read(shown);
        }),
      );
      assert.equal(childRuns, 1);
      $write(shown, false);
      $flush(root);
      assert.equal(childRuns, 2, 'a disposed child never runs; only its replacement');
      $dispose(root);
    });

    it('routes flush failures to $catch and raises the unhandled one after', () => {
      const root = $root('app');
      const scope = $scope(root, null);
      const s = $signal(root, 0);
      const caught = [];
      const applied = [];
      $catch(scope, error => caught.push(error.message));
      $enter(scope, () =>
        $effect(root, () => {
          if ($read(s) === 1) throw new Error('effect failed');
        }),
      );
      $watch(root, () => $read(s), v => {
        applied.push(v);
      });
      $write(s, 1);
      $flush(root);
      assert.deepEqual(caught, ['effect failed']);
      assert.deepEqual(applied, [0, 1], 'other work still runs');
      $write(s, 2);
      $flush(root);
      assert.deepEqual(caught, ['effect failed'], 'the failed effect recovers');
      const loose = $signal(root, 0);
      $effect(root, () => {
        if ($read(loose) === 1) throw new Error('unhandled');
      });
      $write(loose, 1);
      assert.throws(() => $flush(root), /unhandled/);
      assert.equal($deliver(root, scope, new Error('x')), null);
      $enter(scope, () => $throw(root, new Error('thrown')));
      assert.deepEqual(caught, ['effect failed', 'x', 'thrown'], '$deliver and $throw reach the boundary');
      assert.throws(() => $throw(root, new Error('loose')), /loose/);
      $dispose(root);
    });

    it('keeps memo failures as results and recovers', () => {
      const root = $root('app');
      const s = $signal(root, 1);
      const checked = $memo(root, () => {
        if ($read(s) < 0) throw new Error('negative');
        return $read(s);
      });
      const seen = [];
      $effect(root, () => {
        try {
          $read(checked);
          seen.push('ready');
        } catch {
          seen.push('failed');
        }
      });
      $write(s, -1);
      $flush(root);
      $write(s, 3);
      $flush(root);
      assert.deepEqual(seen, ['ready', 'failed', 'ready']);
      assert.equal($read(checked), 3);
      $dispose(root);
    });

    it('applies Watch after computations settle, only when the value changes', () => {
      const root = $root('app');
      const s = $signal(root, 1);
      const order = [];
      const w = $watch(root, () => $read(s) > 0, v => {
        order.push(`watch ${v}`);
        return () => order.push(`clean ${v}`);
      });
      $render(root, () => $read(s), v => {
        order.push(`render ${v}`);
      });
      $effect(root, () => {
        order.push(`effect ${$read(s)}`);
      });
      order.length = 0;
      $write(s, 2);
      $flush(root);
      assert.deepEqual(order, ['effect 2', 'render 2'], 'an equal Watch value does not apply');
      order.length = 0;
      $write(s, -1);
      $flush(root);
      assert.deepEqual(order, ['effect -1', 'render -1', 'clean true', 'watch false']);
      assert.throws(() => $watch(root, () => $write(s, 5), () => {}), /cannot write/);
      w.stop();
      assert.equal(order.at(-1), 'clean false', 'stopping runs the cleanup');
      $dispose(root);
    });

    it('queues applications in a batch and settles them once', () => {
      const root = $root('app');
      const s = $signal(root, 1);
      const applied = [];
      const result = $batch(root, () => {
        $watch(root, () => $read(s), v => {
          applied.push(v);
        });
        $write(s, 2);
        assert.deepEqual(applied, [], 'applications wait for the batch');
        assert.equal($batch(root, () => 'inner'), 'inner', 'nested batches join');
        return 'built';
      });
      assert.equal(result, 'built');
      assert.deepEqual(applied, [2]);
      assert.throws(() => $batch(root, () => {
        throw new Error('build failed');
      }), /build failed/);
      $write(s, 3);
      $flush(root);
      assert.deepEqual(applied, [2, 3], 'a failed batch leaves the graph usable');
      $dispose(root);
    });

    it('flushes what a failed batch wrote, then raises the build failure', () => {
      for (const auto of [false, true]) {
        const root = $root('app');
        $auto(root, auto);
        const a = $signal(root, 0);
        const seen = [];
        $effect(root, () => {
          seen.push($read(a));
        });
        assert.throws(() => $batch(root, () => {
          $write(a, 1);
          throw new Error('x');
        }), /^Error: x$/);
        assert.deepEqual(seen, [0, 1], `auto ${auto}: the batch still flushed`);
        $dispose(root);
      }
      // The build failure is raised first; the flush's own unhandled failure
      // is kept for the next `$flush`.
      const root = $root('app');
      const b = $signal(root, 0);
      $effect(root, () => {
        if ($read(b) === 1) throw new Error('effect');
      });
      assert.throws(() => $batch(root, () => {
        $write(b, 1);
        throw new Error('build');
      }), /build/);
      assert.throws(() => $flush(root), /effect/);
      $flush(root);
      $dispose(root);
    });

    it('returns from a batch whose build disposed its own root', () => {
      for (const auto of [false, true]) {
        const root = $root('app');
        $auto(root, auto);
        const a = $signal(root, 0);
        const seen = [];
        $effect(root, () => {
          seen.push($read(a));
        });
        assert.equal($batch(root, () => {
          $write(a, 1);
          $dispose(root);
          return 'done';
        }), 'done', `auto ${auto}`);
        assert.equal($closed(root), true);
        assert.deepEqual(seen, [0], `auto ${auto}: a disposed root runs nothing more`);
        assert.throws(() => $batch(root, () => 0), /disposed root/, 'a batch on a disposed root is still refused');
      }
    });

    it('settles commit-hook and $after writes in the same flush; $settled runs once idle', () => {
      const root = $root('app');
      const source = $signal(root, 0);
      const mirror = $signal(root, 0);
      const seen = [];
      $watch(root, () => $read(mirror), v => {
        seen.push(v);
      });
      const direct = [];
      $watch(root, () => $read(source), v => {
        direct.push(v);
      });
      $commit(root, null, () => {
        const wanted = $untrack(root, () => $read(source));
        if ($untrack(root, () => $read(mirror)) !== wanted) $write(mirror, wanted);
      });
      $write(source, 5);
      $flush(root);
      assert.deepEqual(seen, [0, 5]);
      $after(root, () => $write(source, 7));
      $flush(root);
      assert.deepEqual(direct, [0, 5, 7], 'an $after write settles in the same flush');
      assert.deepEqual(seen, [0, 5, 7], 'commit hooks see writes made after them');
      let later = 0;
      $after(root, () => later++);
      $flush(root);
      $flush(root);
      assert.equal(later, 1, '$after runs once');
      let settled = 0;
      $settled(root, () => settled++);
      $flush(root);
      assert.equal(settled, 1);
      $dispose(root);
    });

    it('runs a $settled callback that registers itself again once per flush', () => {
      const root = $root('app');
      let runs = 0;
      const every = () => {
        runs++;
        $settled(root, every);
      };
      $settled(root, every);
      $flush(root);
      assert.equal(runs, 1, 'the one it queued waits for the next flush');
      $flush(root);
      assert.equal(runs, 2);
      $dispose(root);
    });

    it('runs a $settled callback registered by one that wrote in a later round of the same flush', () => {
      const root = $root('app');
      const count = $signal(root, 0);
      const reads = [];
      $effect(root, () => void reads.push($read(count)));
      $flush(root);
      reads.length = 0;
      let later = 0;
      $settled(root, () => {
        $write(count, 1);
        $settled(root, () => later++);
      });
      $flush(root);
      assert.deepEqual(reads, [1], 'the write queued the effect');
      assert.equal(later, 1, 'the one it registered runs in the same flush');
      $dispose(root);
    });

    it('runs deferred work in the low lane after urgent work', () => {
      const root = $root('app');
      const urgent = $signal(root, 0);
      const low = $signal(root, 0);
      const order = [];
      $effect(root, () => {
        order.push(`low ${$read(low)} ${$deferred(root)}`);
      });
      $effect(root, () => {
        order.push(`urgent ${$read(urgent)}`);
      });
      order.length = 0;
      $defer(root, () => {
        assert.equal($deferred(root), true);
        $write(low, 1);
      });
      $write(urgent, 1);
      $flush(root);
      assert.deepEqual(order, ['urgent 1', 'low 1 true']);
      $dispose(root);
    });

    it('recomputes a memo in the contexts where it was made, as an effect runs', () => {
      const root = $root('app');
      const Ctx = $context('fallback');
      const v = $signal(root, 'one');
      /** @type {any} */
      let memo = null;
      const seen = [];
      $provide(root, Ctx, v, () => {
        memo = $memo(root, () => `${$read(v)} ${$lookup(Ctx)}`);
      });
      $effect(root, () => void seen.push($read(memo)));
      $write(v, 'two');
      $flush(root);
      assert.deepEqual(seen, ['one one', 'two two'], 'not the fallback');
      $dispose(root);
    });

    it('keeps what deferred work writes deferred: a low-lane effect\'s write queues its readers in the low lane', () => {
      const root = $root('app');
      const low = $signal(root, 0);
      const copy = $signal(root, 0);
      const urgent = $signal(root, 0);
      const order = [];
      // The low lane's effect hands its value on, as a region hands its update to its gate.
      $effect(root, () => {
        const v = $read(low);
        if (v > 0) $write(copy, v);
      });
      $effect(root, () => {
        order.push(`copy ${$read(copy)} ${$deferred(root)}`);
      });
      $effect(root, () => {
        order.push(`urgent ${$read(urgent)}`);
      });
      order.length = 0;
      $defer(root, () => $write(low, 1));
      $write(urgent, 1);
      $flush(root);
      assert.deepEqual(order, ['urgent 1', 'copy 1 true']);
      $dispose(root);
    });

    it('owns nodes, scopes and cleanups; $stop frees them newest first', () => {
      const root = $root('app');
      const s = $signal(root, 0);
      const scope = $scope(root, null);
      const order = [];
      $enter(scope, () => {
        const inner = $scope(root, $owner(root));
        $cleanup(inner, () => order.push('inner'));
        $effect(root, () => {
          $read(s);
          return () => order.push('effect');
        });
        $memo(root, () => $read(s));
      });
      const key = $cleanup(scope, () => order.push('released'));
      $cleanup(scope, () => order.push('scope'));
      $release(scope, key);
      assert.equal($cleanups(scope), 1);
      assert.equal($alive(scope), true);
      $stop(scope);
      assert.deepEqual(order, ['effect', 'inner', 'scope']);
      assert.equal($alive(scope), false);
      assert.equal($observers(s), 0);
      assert.deepEqual($stats(root), { scopes: 0, effects: 0, commits: 0, nodes: 1, links: 0 });
      // A scope counts as a scope, never as a node.
      const kept = $scope(root, null);
      assert.deepEqual($stats(root), { scopes: 1, effects: 0, commits: 0, nodes: 1, links: 0 });
      $stop(kept);
      assert.throws(() => $enter(scope, () => 0), /disposed scope/);
      assert.equal($cleanup(scope, () => {}), null);
      $dispose(root);
    });

    it('drains mounts at the host boundary', () => {
      const root = $root('app');
      const scope = $scope(root, null);
      const seen = [];
      $mount(scope, () => seen.push($owner(root) === scope));
      assert.deepEqual(seen, []);
      $mounts(scope);
      $mounts(scope);
      assert.deepEqual(seen, [true]);
      $dispose(root);
    });

    it('names roots with a checked id', () => {
      const root = $root('side-2');
      assert.equal(root.id, 'side-2');
      $dispose(root);
      for (const bad of ['', 'Side', '2side', 'a_b', 'a'.repeat(33), undefined]) assert.throws(() => $root(bad), /root id must match/);
      assert.equal($root('a'.repeat(32)).id.length, 32);
    });

    it('provides, looks up and shadows contexts, with a fallback', () => {
      const root = $root('app');
      const Theme = $context('light');
      const Size = $context(1);
      assert.equal($lookup(Theme), 'light');
      assert.equal($lookup(root, Theme), 'light');
      $provide(root, Theme, 'dark', () => {
        assert.equal($lookup(Theme), 'dark');
        assert.equal($lookup(Size), 1);
        $provide(root, Size, 2, () => {
          $provide(root, Theme, 'blue', () => {
            assert.equal($lookup(Theme), 'blue');
            assert.equal($lookup(Size), 2);
            assert.deepEqual($entries().map(([c, v]) => [c === Theme ? 'theme' : 'size', v]), [['theme', 'blue'], ['size', 2]]);
          });
          assert.equal($lookup(Theme), 'dark');
        });
      });
      assert.equal($lookup(root, Theme), 'light');
      assert.deepEqual($entries(root), []);
      $dispose(root);
    });

    it('carries contexts through scopes and effects; Update reaches only the provider below', () => {
      const root = $root('app');
      const Theme = $context('light');
      /** @type {string[]} */
      const seen = [];
      let inner = /** @type {any} */ (null);
      let outer = /** @type {any} */ (null);
      $provide(root, Theme, 'dark', () => {
        outer = $scope(root, $owner(root));
        $enter(outer, () => $effect(root, () => void seen.push(`outer:${$lookup(Theme)}`)));
        $provide(root, Theme, 'blue', () => {
          inner = $scope(root, $owner(root));
          $enter(inner, () => $effect(root, () => void seen.push(`inner:${$lookup(Theme)}`)));
        });
      });
      assert.deepEqual(seen, ['outer:dark', 'inner:blue']);
      $enter(inner, () => $update(Theme, value => `${value}!`));
      $flush(root);
      assert.deepEqual(seen, ['outer:dark', 'inner:blue', 'inner:blue!']);
      $enter(outer, () => $update(Theme, 'black'));
      $flush(root);
      assert.deepEqual(seen, ['outer:dark', 'inner:blue', 'inner:blue!', 'outer:black']);
      assert.throws(() => $update(root, Theme, 'x'), /nothing provides/);
      // A captured context re-enters later.
      const captured = $enter(inner, () => g.$current(root));
      assert.equal($with(root, captured, () => $lookup(Theme)), 'blue!');
      $dispose(root);
    });

    it('follows a reactive provided value until an Update overrides it', () => {
      const root = $root('app');
      const Theme = $context('light');
      const source = $signal(root, 'a');
      /** @type {string[]} */
      const seen = [];
      $provide(root, Theme, () => $read(source), () => $effect(root, () => void seen.push($lookup(Theme))));
      $write(source, 'b');
      $flush(root);
      assert.deepEqual(seen, ['a', 'b']);
      $dispose(root);
    });

    it('never lets one root see another root\'s contexts or pending reads', () => {
      const a = $root('a');
      const b = $root('b');
      const Theme = $context('none');
      const pending = $signal(a, 0);
      const waiting = $memo(a, () => {
        $read(pending);
        return g.wait({ value: 'held' });
      });
      $provide(a, Theme, 'from-a', () => {
        // b's code, running inside a's provider, sees b's (empty) chain.
        const scope = $scope(b, null);
        assert.equal($enter(scope, () => $lookup(Theme)), 'none');
        assert.equal($lookup(Theme), 'from-a');
      });
      assert.throws(() => $read(waiting), e => e === g.WAITING);
      // The register holds only the read that just threw, for its catcher.
      assert.deepEqual(g.waitedOn(), { value: 'held' });
      assert.equal(g.waitedOn(), null);
      // b's own pending memo records b's state, not a's.
      const own = $memo(b, () => g.wait({ value: 'mine' }));
      assert.throws(() => $read(own), e => e === g.WAITING);
      assert.deepEqual(g.waitedOn(), { value: 'mine' });
      const ready = $memo(b, () => 1);
      assert.equal($read(ready), 1);
      assert.equal(g.waitedOn(), null);
      assert.throws(() => $read(waiting), e => e === g.WAITING);
      assert.deepEqual(g.waitedOn(), { value: 'held' });
      $dispose(a);
      $dispose(b);
    });

    it('restores the running root after successful and failing nested flushes', () => {
      const a = $root('a');
      const b = $root('b');
      const context = $context('fallback');
      const trigger = $signal(a, 0);
      const nested = $signal(b, 0);
      const seen = [];
      const failure = new Error('nested effect failed');
      $provide(b, context, 'b', () => $effect(b, () => {
        const value = $read(nested);
        if (value > 0) seen.push(`inner:${$lookup(context)}`);
        if (value === 1) throw failure;
      }));
      $provide(a, context, 'a', () => $effect(a, () => {
        const value = $read(trigger);
        if (value === 0) return;
        seen.push(`before:${$lookup(context)}`);
        $write(nested, value);
        if (value === 1) assert.throws(() => $flush(b), error => error === failure);
        else $flush(b);
        seen.push(`after:${$lookup(context)}`);
      }));
      $write(trigger, 1);
      $flush(a);
      $write(trigger, 2);
      $flush(a);
      assert.deepEqual(seen, ['before:a', 'inner:b', 'after:a', 'before:a', 'inner:b', 'after:a']);
      $dispose(a);
      $dispose(b);
    });

    it('holds a $derive write until a source changes', () => {
      const root = $root('app');
      const s = $signal(root, 5);
      const local = $derive(root, () => $read(s) * 2);
      assert.equal($read(local), 10);
      $write(local, 42);
      assert.equal($read(local), 42);
      const seen = [];
      $effect(root, () => {
        seen.push($read(local));
      });
      $write(local, 43);
      $flush(root);
      $write(s, 10);
      $flush(root);
      assert.deepEqual(seen, [42, 43, 20]);
      assert.throws(() => $write($memo(root, () => 1), 2), /read-only/);
      $dispose(root);
    });

    it('uses custom equality on signals and memos', () => {
      const root = $root('app');
      const row = $equals($signal(root, { id: 1, name: 'a' }), (a, b) => a.id === b.id);
      let runs = 0;
      $effect(root, () => {
        $read(row);
        runs++;
      });
      $write(row, { id: 1, name: 'b' });
      $flush(root);
      assert.equal(runs, 1);
      assert.equal($read(row).name, 'a', 'an equal write keeps the stored value');
      $write(row, { id: 2, name: 'c' });
      $flush(root);
      assert.equal(runs, 2);
      const tick = $equals($signal(root, 0), false);
      let ticks = 0;
      $effect(root, () => {
        $read(tick);
        ticks++;
      });
      $write(tick, 0);
      $flush(root);
      assert.equal(ticks, 2, 'equality false notifies every write');
      $dispose(root);
    });

    it('touches readers, invalidates memos and scans', () => {
      const root = $root('app');
      const list = $signal(root, [1]);
      let runs = 0;
      $effect(root, () => {
        $read(list);
        runs++;
      });
      $read(list).push(2);
      $touch(root, () => $read(list));
      $flush(root);
      assert.equal(runs, 2);
      let external = 1;
      const m = $memo(root, () => external);
      const seen = [];
      $effect(root, () => {
        seen.push($read(m));
      });
      external = 2;
      $invalidate(m);
      $flush(root);
      assert.deepEqual(seen, [1, 2]);
      const s = $signal(root, 1);
      const total = $scan(root, 0, previous => previous + $read(s));
      assert.equal($read(total), 1);
      $write(s, 5);
      assert.equal($read(total), 6);
      $dispose(root);
    });

    it('runs whenUnwatched hooks and permits Writable signals in computations', () => {
      const root = $root('app');
      const s = $signal(root, 0);
      let hooks = 0;
      $whenUnwatched(s, () => hooks++);
      const e = $effect(root, () => {
        $read(s);
      });
      assert.equal($observers(s), 1);
      $cancel(e);
      assert.equal(hooks, 1);
      const shadow = $writable($signal(root, 0));
      const plain = $signal(root, 0);
      const m = $memo(root, () => {
        $write(shadow, 1);
        return true;
      });
      assert.equal($read(m), true);
      assert.equal($read(shadow), 1);
      assert.throws(() => $read($memo(root, () => $write(plain, 1))), /cannot write/);
      $dispose(root);
    });

    it('introspects kinds, identity, sources and the running observer', () => {
      const root = $root('app');
      const s = $signal(root, 1);
      const m = $memo(root, () => $read(s));
      let running = null;
      const e = $effect(root, () => {
        $read(m);
        running = $observer(root);
      });
      assert.equal(running, e);
      assert.equal($observer(root), null);
      assert.equal($kind(s), 'signal');
      assert.equal($kind(m), 'memo');
      assert.equal($kind(e), 'effect');
      assert.equal($kind($scope(root, null)), 'scope');
      assert.equal($kind({ value: 1 }), null);
      assert.equal($kind(3), null);
      assert.notDeepEqual($identity(s), $identity(m));
      assert.deepEqual($sources(e), [m]);
      assert.equal($observers(m), 1);
      $dispose(root);
    });

    it('frees a discarded node: readers recompute, the handle reads its final value', () => {
      const root = $root('app');
      const s = $signal(root, 7);
      let runs = 0;
      const seen = [];
      $effect(root, () => {
        runs++;
        seen.push($read(s));
      });
      $discard(s);
      assert.equal($read(s), 7, 'a freed node reads as its final value');
      $write(s, 8);
      assert.equal($read(s), 8, 'a stale write only updates the final value');
      $flush(root);
      assert.equal(runs, 2, 'the reader recomputes without the vanished source');
      assert.equal($stats(root).links, 0);
      $dispose(root);
    });

    it('disposes a root: cleanups run and handles refuse', () => {
      const root = $root('app');
      const s = $signal(root, 1);
      let cleaned = 0;
      $effect(root, () => {
        $read(s);
        return () => cleaned++;
      });
      $unowned(root, () => $signal(root, 2));
      $dispose(root);
      assert.equal(cleaned, 1);
      assert.equal($closed(root), true);
      assert.throws(() => $read(s), /disposed root/);
      assert.throws(() => $signal(root, 1), /disposed root/);
      $dispose(root);
    });

    it('disposes a root from one of its own running effects, at the root or in a scope', () => {
      for (const scoped of [false, true]) {
        const root = $root('app');
        const s = $signal(root, 0);
        let cleaned = 0;
        let after = 0;
        const run = () =>
          $effect(root, () => {
            if ($read(s) === 1) {
              $dispose(root);
              after++;
            }
            return () => cleaned++;
          });
        // A sibling effect that is not running when the root goes.
        $effect(root, () => () => cleaned++);
        if (scoped) $enter($scope(root, null), run);
        else run();
        $flush(root);
        $write(s, 1);
        $flush(root);
        assert.equal($closed(root), true, `${scoped ? 'scoped' : 'root'}: closed`);
        assert.equal(after, 1, `${scoped ? 'scoped' : 'root'}: the effect's run finishes`);
        // The first run's before the rerun, the disposing run's own, the sibling's.
        assert.equal(cleaned, 3, `${scoped ? 'scoped' : 'root'}: every cleanup ran`);
        assert.throws(() => $read(s), /disposed root/);
      }
    });

    it('runs a new effect in its own place, not in the queue place of a cancelled one', () => {
      const root = $root('app');
      /** @type {string[]} */
      const log = [];
      const s = $signal(root, 1);
      $effect(root, () => {
        const v = $read(s);
        log.push('A' + v);
        if (v === 2) $write(s, 3);
      });
      const X = $effect(root, () => void $read(s));
      $write(s, 2);
      // X leaves while queued; B, made after it, may take its slot.
      $cancel(X);
      $effect(root, () => void log.push('B' + $read(s)));
      $flush(root);
      assert.deepEqual(log, ['A1', 'B2', 'A2', 'A3', 'B3'], 'A, the older, runs first again');
      $dispose(root);
    });

    it('applies a new Render in its own place, not in the queue place of a stopped one', () => {
      /** @type {string[]} */
      const log = [];
      const root = $root('app');
      $auto(root, false);
      const s = $signal(root, 0);
      const S = $scope(root, null);
      let made = false;
      $render(root, () => $read(s), v => {
        log.push('X' + v);
        if (v === 1 && !made) {
          made = true;
          // W1, queued behind X, goes; W2, made now, may take its id.
          $stop(S);
          const T = $scope(root, null);
          $enter(T, () => $render(root, () => $read(s), w => void log.push('W2:' + w)));
        }
      });
      $enter(S, () => $render(root, () => $read(s), v => void log.push('W1:' + v)));
      $render(root, () => $read(s), v => void log.push('Y' + v));
      $flush(root);
      log.length = 0;
      $write(s, 1);
      $flush(root);
      assert.deepEqual(log, ['X1', 'Y1', 'W2:1'], 'Y, queued before W2 was made, applies first');
      $dispose(root);
    });

    it('runs the cleanup of a Render that stopped itself, even when a node it made took its id', () => {
      /** @type {string[]} */
      const log = [];
      const root = $root('app');
      const s = $signal(root, 1);
      const S = $scope(root, null);
      $enter(S, () =>
        $render(
          root,
          () => $read(s),
          v => {
            log.push('act' + v);
            $stop(S);
            const T = $scope(root, null);
            $enter(T, () => $watch(root, () => $read(s), w => void log.push('made' + w)));
            return () => void log.push('cleanup');
          },
        ),
      );
      log.push('|');
      $write(s, 2);
      $flush(root);
      assert.deepEqual(log, ['act1', 'made1', 'cleanup', '|', 'made2']);
      $dispose(root);
    });

    it('moves an effect queued deferred to the urgent lane when an urgent change reaches it', () => {
      for (const through of ['direct', 'memo']) {
        const root = $root('app');
        const a = $signal(root, 0);
        const b = $signal(root, 0);
        const u = $signal(root, 0);
        const m = $memo(root, () => $read(a) * 2);
        /** @type {string[]} */
        const order = [];
        $effect(root, () => {
          const x = through === 'memo' ? $read(m) : $read(a);
          if (x + $read(b)) order.push(`both deferred=${$deferred(root)}`);
        });
        $effect(root, () => {
          if ($read(u)) order.push(`urgent deferred=${$deferred(root)}`);
        });
        // Queued deferred by the first write, then reached by an urgent one.
        $defer(root, () => $write(a, 1));
        $write(b, 1);
        $write(u, 1);
        $flush(root);
        assert.deepEqual(order, ['both deferred=false', 'urgent deferred=false'], `${through}: it runs urgent, in creation order`);
        $dispose(root);
      }
    });

    it('moves it up when the urgent change reaches it only through a memo already dirty', () => {
      const root = $root('app');
      const a = $signal(root, 0);
      const c = $signal(root, 0);
      const u = $signal(root, 0);
      const m = $memo(root, () => $read(a) + $read(c));
      /** @type {string[]} */
      const order = [];
      $effect(root, () => {
        if ($read(m)) order.push(`m-reader deferred=${$deferred(root)}`);
      });
      $effect(root, () => {
        if ($read(u)) order.push(`urgent deferred=${$deferred(root)}`);
      });
      // The deferred write makes m dirty and queues its reader low; the urgent one meets m dirty.
      $defer(root, () => $write(a, 1));
      $write(c, 1);
      $write(u, 1);
      $flush(root);
      assert.deepEqual(order, ['m-reader deferred=false', 'urgent deferred=false']);
      $dispose(root);
    });

    it('reclaims the $commit hooks of stopped owners with no flush round, idle or none', () => {
      for (const flush of ['idle', 'none']) {
        const root = $root('app');
        for (let i = 0; i < 1000; i++) {
          const scope = $scope(root, null);
          $commit(root, scope, () => {});
          $stop(scope);
          if (flush === 'idle') $flush(root);
        }
        assert.equal($stats(root).commits, 0);
        assert.ok(root.commits.length <= 32, `${flush}: the hook list stays bounded (${root.commits.length})`);
        $dispose(root);
      }
    });

    it('reclaims the $commit hooks of stopped owners', () => {
      const root = $root('app');
      const s = $signal(root, 0);
      let runs = 0;
      $effect(root, () => void $read(s));
      $commit(root, null, () => void runs++);
      for (let i = 0; i < 1000; i++) {
        const scope = $scope(root, null);
        $commit(root, scope, () => {});
        $stop(scope);
      }
      $write(s, 1);
      $flush(root);
      assert.equal($stats(root).commits, 1);
      assert.ok(root.commits.length <= 32, `the hook list stays bounded (${root.commits.length})`);
      const before = runs;
      $write(s, 2);
      $flush(root);
      assert.equal(runs, before + 1, 'the live hook still runs');
      $dispose(root);
    });

    it('keeps its duplicate-link check and cycle guard past 2**31 computations and flushes', () => {
      // A root that has run 2**31 computations: a read repeated in one run links once.
      const a = $root('app');
      a.cycles = 2 ** 31;
      const sel = $signal(a, 0);
      const items = Array.from({ length: 50 }, (_, i) => $signal(a, i));
      $effect(a, () => {
        for (const it of items) {
          $read(sel);
          $read(it);
        }
      });
      $flush(a);
      $write(sel, 1);
      $flush(a);
      assert.equal($stats(a).links, 51, 'one link per source');
      $dispose(a);
      // A root that has flushed 2**31 times: a self-writing effect still trips the guard.
      const b = $root('app');
      b.serialAll = 2 ** 31;
      const s = $signal(b, 0);
      let runs = 0;
      $effect(b, () => {
        const v = $read(s);
        if (++runs < 10000) $write(s, v + 1);
      });
      assert.throws(() => $flush(b), /reactive cycle/);
      assert.ok(runs <= 102, `stopped after about 100 runs (${runs})`);
      $dispose(b);
    });

    it('keeps an owner cleanup list bounded under $cleanup and $release, and runs the live ones in order', () => {
      const root = $root('app');
      const scope = $scope(root, null);
      /** The owner's cleanup list (an internal of either core). @returns {any[]} */
      const list = () => (name === 'arena' ? root.cleanup[/** @type {any} */ (scope).id] : /** @type {any} */ (scope).cleanup);
      /** @type {string[]} */
      const ran = [];
      /** @type {(number | null)[]} */
      const kept = [];
      for (let i = 0; i < 10000; i++) {
        const key = $cleanup(scope, () => void ran.push('gone' + i));
        // Every hundredth stays; its key is released later, or never.
        if (i % 100 === 0) kept.push($cleanup(scope, () => void ran.push('kept' + i)));
        $release(scope, key);
      }
      assert.ok(list().length <= 2 * $cleanups(scope) + 32, `bounded: ${list().length} slots for ${$cleanups(scope)} live`);
      // A key given out before a compaction still names its cleanup.
      $release(scope, kept[1]);
      $release(scope, kept[1]);
      assert.equal($cleanups(scope), 99);
      $stop(scope);
      const expected = [];
      for (let i = 9900; i >= 0; i -= 100) if (i !== 100) expected.push('kept' + i);
      assert.deepEqual(ran, expected, 'the live cleanups ran, newest first');
      $dispose(root);
    });

    it('leaves a hold on a root disposed meanwhile without flushing it', () => {
      const root = $root('app');
      $auto(root, true);
      const s = $signal(root, 0);
      let runs = 0;
      $effect(root, () => {
        $read(s);
        runs++;
      });
      $flush(root);
      g.hold(root);
      $write(s, 1);
      $dispose(root);
      assert.doesNotThrow(() => g.unhold(root), 'nothing to flush on a closed root');
      assert.equal(runs, 1);
    });

    it('guards reads and recursive memos', () => {
      const root = $root('app');
      assert.throws(() => $read(/** @type {any} */ ({ value: 1 })));
      /** @type {any} */
      let self = null;
      self = $memo(root, () => $read(self));
      assert.throws(() => $read(self), /recursive memo/);
      const t = $signal(root, 0);
      $effect(root, () => {
        if ($read(t) === 1) $flush(root);
      });
      $write(t, 1);
      assert.throws(() => $flush(root), /recursive flush/);
      $dispose(root);
    });

    it('cancels an effect that stops its own owner while running', () => {
      const root = $root('app');
      const s = $signal(root, 0);
      const scope = $scope(root, null);
      let runs = 0;
      $enter(scope, () =>
        $effect(root, () => {
          runs++;
          if ($read(s) === 1) $stop(scope);
        }),
      );
      $write(s, 1);
      $flush(root);
      $write(s, 2);
      $flush(root);
      assert.equal(runs, 2);
      assert.equal($stats(root).effects, 0);
      $dispose(root);
    });
  });
}

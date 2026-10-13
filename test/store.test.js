// Stores, mirrors, overlays, lists and selection.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  $root, $signal, $read, $write, $memo, $effect, $watch, $flush, $dispose, $scope, $enter, $stop, $stats, $cleanup, $kind,
  $store, $at, $put, $raw, $reuse, $update, $mirror, $overlay, $keyed, $rows, $positions, $select, $merge, $split,
  $catch, $owner, $batch,
} from '../src/index.js';

describe('rrx-core stores', () => {
  it('notifies only the paths that changed', () => {
    const root = $root('app');
    const store = $store(root, { user: { name: 'ada', age: 36 }, rows: [1, 2] });
    assert.equal($kind(store), 'store');
    const runs = { name: 0, age: 0, user: 0, rows: 0 };
    $effect(root, () => {
      $at(store, ['user', 'name']);
      runs.name++;
    });
    $effect(root, () => {
      $at(store, ['user', 'age']);
      runs.age++;
    });
    $effect(root, () => {
      $at(store, ['user']);
      runs.user++;
    });
    $effect(root, () => {
      $at(store, ['rows', 1]);
      runs.rows++;
    });
    $put(store, ['user', 'age'], age => age + 1);
    $flush(root);
    assert.deepEqual(runs, { name: 1, age: 2, user: 2, rows: 1 });
    $put(store, ['user'], { name: 'grace', age: 37 });
    $flush(root);
    assert.deepEqual(runs, { name: 2, age: 2, user: 3, rows: 1 }, 'an equal descendant does not notify');
    $put(store, ['rows', 1], 5);
    $flush(root);
    assert.equal(runs.rows, 2);
    assert.deepEqual($raw(store).rows, [1, 5]);
    assert.throws(() => $put(store, ['rows', 7], 1), /out of bounds/);
    const f = () => 1;
    assert.equal($put(store, ['handler'], () => f), f, 'Put returns the closure it stored');
    assert.equal($at(store, ['handler']), f);
    $dispose(root);
  });

  it('frees cells when their last observer leaves', () => {
    const root = $root('app');
    const store = $store(root, { a: 1 });
    const before = $stats(root).nodes;
    const scope = $scope(root, null);
    $enter(scope, () =>
      $effect(root, () => {
        $at(store, ['a']);
      }),
    );
    assert.equal($stats(root).nodes, before + 2);
    $stop(scope);
    assert.equal($stats(root).nodes, before);
    $dispose(root);
  });

  it('merges fields, mirrors and keeps row identity', () => {
    const root = $root('app');
    const store = $store(root, { form: { a: 1, b: 2 } });
    const seen = [];
    $effect(root, () => {
      seen.push($at(store, ['form', 'b']));
    });
    $update(store, ['form'], { a: 5 });
    $flush(root);
    assert.deepEqual($raw(store).form, { a: 5, b: 2 });
    assert.deepEqual(seen, [2], 'an unchanged field does not notify');
    const source = $signal(root, [1, 2, 3]);
    const mirrored = $mirror(root, () => ({ total: $read(source).reduce((t, x) => t + x, 0), items: $read(source).map(x => ({ x })) }));
    const totals = [];
    const firsts = [];
    $effect(root, () => {
      totals.push($at(mirrored, ['total']));
    });
    $effect(root, () => {
      firsts.push($at(mirrored, ['items', 0, 'x']));
    });
    $write(source, [1, 2, 4]);
    $flush(root);
    assert.deepEqual(totals, [6, 7]);
    assert.deepEqual(firsts, [1], 'a mirrored path whose value is equal does not notify');
    const a = { id: 1, v: 'a' };
    const next = $reuse([a, { id: 2, v: 'b' }], [{ id: 2, v: 'c' }, { id: 1, v: 'a' }], row => row.id);
    assert.equal(next[1], a, 'an unchanged row keeps its previous value');
    assert.throws(() => $reuse([], [a, a], row => row.id), /duplicate/);
    $dispose(root);
  });

  it('mirrors on creation, inside a batch too', () => {
    for (const batched of [false, true]) {
      const root = $root('app');
      const s = $signal(root, [1, 2]);
      /** @type {any[]} */
      const seen = [];
      const build = () => {
        const p = $mirror(root, () => ({ max: Math.max(...$read(s)) }));
        assert.equal($at(p, ['max']), 2, `batched ${batched}: the first read is the mirror`);
        $watch(root, () => $at(p, ['max']), v => void seen.push(v));
        return p;
      };
      const p = batched ? $batch(root, build) : build();
      assert.deepEqual(seen, [2], `batched ${batched}: no observer sees the empty store`);
      $write(s, [5, 3]);
      $flush(root);
      assert.deepEqual(seen, [2, 5]);
      assert.equal($at(p, ['max']), 5);
      $dispose(root);
    }
  });

  it('layers optimistic updates over a store', () => {
    const root = $root('app');
    const store = $store(root, { count: 1, name: 'x' });
    const overlay = $overlay(root, store);
    const seen = [];
    $watch(root, () => overlay.read(['count']), value => {
      seen.push(value);
    });
    const ticket = overlay.apply(['count'], n => n + 10);
    $flush(root);
    assert.equal($read(overlay.pending), true);
    $put(store, ['count'], 2);
    $flush(root);
    overlay.commit(ticket);
    $flush(root);
    assert.equal($raw(store).count, 12);
    const dropped = overlay.apply(['count'], () => 0);
    overlay.settle(dropped);
    $flush(root);
    assert.deepEqual(seen, [1, 11, 12]);
    $dispose(root);
  });
});

describe('rrx-core lists and selection', () => {
  it('reuses Keyed rows by key and stops removed ones', () => {
    const root = $root('app');
    const items = $signal(root, [{ id: 'a', v: 1 }, { id: 'b', v: 2 }]);
    const built = [];
    const stopped = [];
    const owner = $scope(root, null);
    const rows = $enter(owner, () =>
      $keyed(root, () => $read(items), item => item.id, (item, index) => {
        const id = $read(item).id;
        built.push(id);
        $cleanup(/** @type {any} */ (root.owner), () => stopped.push(id));
        return $memo(root, () => `${$read(index)}:${$read(item).v}`);
      }),
    );
    const view = () => $read(rows).map(m => $read(m));
    assert.deepEqual(view(), ['0:1', '1:2']);
    $write(items, [{ id: 'b', v: 3 }, { id: 'c', v: 4 }, { id: 'a', v: 1 }]);
    assert.deepEqual(view(), ['0:3', '1:4', '2:1']);
    assert.deepEqual(built, ['a', 'b', 'c'], 'kept keys are not rebuilt');
    $write(items, [{ id: 'c', v: 4 }]);
    assert.deepEqual(view(), ['0:4']);
    assert.deepEqual(stopped.sort(), ['a', 'b']);
    $write(items, [{ id: 'c', v: 1 }, { id: 'c', v: 2 }]);
    assert.throws(() => $read(rows), /duplicate \$keyed key/);
    $stop(owner);
    assert.deepEqual(stopped.sort(), ['a', 'b', 'c']);
    $dispose(root);
  });

  it('leaves the kept rows as they were when an update fails', () => {
    for (const make of [$keyed, $rows]) {
      const root = $root('app');
      const items = $signal(root, [{ id: 'a', v: 1 }, { id: 'b', v: 2 }]);
      const owner = $scope(root, null);
      const rows = $enter(owner, () =>
        make(root, () => $read(items), item => item.id, (/** @type {any} */ item, /** @type {any} */ index) =>
          $memo(root, () => `${$read(index)}:${(typeof item === 'function' ? item() : $read(item)).v}`),
        ),
      );
      const view = () => $read(rows).map(m => $read(m));
      assert.deepEqual(view(), ['0:1', '1:2']);
      const shown = $read(rows);
      // Kept keys a and b move and change, then a duplicate fails the update.
      $write(items, [{ id: 'b', v: 20 }, { id: 'a', v: 10 }, { id: 'b', v: 30 }]);
      assert.throws(() => $read(rows), /duplicate \$keyed key/);
      assert.deepEqual(shown.map(m => $read(m)), ['0:1', '1:2'], `${make.name}: the rows shown keep their items and places`);
      $stop(owner);
      $dispose(root);
    }
    // By position: the kept rows change item, then a new row's build throws.
    const root = $root('app');
    const items = $signal(root, [1, 2]);
    const owner = $scope(root, null);
    const rows = $enter(owner, () =>
      $positions(root, () => $read(items), (item, index) => {
        if ($read(item) === 30) throw new Error('build failed');
        return $memo(root, () => `${$read(index)}:${$read(item)}`);
      }),
    );
    assert.deepEqual($read(rows).map(m => $read(m)), ['0:1', '1:2']);
    const shown = $read(rows);
    $write(items, [10, 20, 30]);
    assert.throws(() => $read(rows), /build failed/);
    assert.deepEqual(shown.map(m => $read(m)), ['0:1', '1:2'], '$positions: the rows shown keep their items');
    $stop(owner);
    $dispose(root);
  });

  it('leaves no row live from an update that fails on a duplicate key', () => {
    for (const make of [$keyed, $rows]) {
      const root = $root('app');
      const items = $signal(root, [{ id: 'a' }]);
      /** @type {string[]} */
      const live = [];
      const owner = $scope(root, null);
      const rows = $enter(owner, () =>
        make(root, () => $read(items), item => item.id, (/** @type {any} */ item) => {
          const id = typeof item === 'function' ? item().id : $read(item).id;
          live.push(id);
          $cleanup(/** @type {any} */ (root.owner), () => live.splice(live.indexOf(id), 1));
          return id;
        }),
      );
      assert.deepEqual($read(rows), ['a']);
      $write(items, [{ id: 'a' }, { id: 'x' }, { id: 'y' }, { id: 'x' }]);
      assert.throws(() => $read(rows), /duplicate \$keyed key/);
      assert.deepEqual(live, ['a'], `${make.name}: the rows built before the duplicate are stopped`);
      $write(items, [{ id: 'a' }, { id: 'x' }]);
      assert.deepEqual($read(rows), ['a', 'x'], `${make.name}: a later update builds again`);
      $stop(owner);
      assert.deepEqual(live, []);
      $dispose(root);
    }
  });

  it('leaves no row live from an update whose build or key throws', () => {
    /** @type {[string, (root: any, read: any, build: any) => any][]} */
    const lists = [
      ['$keyed', (root, read, build) => $keyed(root, read, item => item.id, build)],
      ['$rows', (root, read, build) => $rows(root, read, item => item.id, build)],
      ['$positions', (root, read, build) => $positions(root, read, build)],
      ['$keyed key', (root, read, build) => $keyed(root, read, item => {
        if (item.id === 'bad') throw new Error('bad key');
        return item.id;
      }, build)],
    ];
    for (const [name, make] of lists) {
      const root = $root('app');
      const items = $signal(root, [{ id: 'a' }]);
      /** @type {string[]} */
      const live = [];
      const owner = $scope(root, null);
      const rows = $enter(owner, () =>
        make(root, () => $read(items), (/** @type {any} */ item) => {
          const id = typeof item === 'function' ? item().id : $read(item).id;
          live.push(id);
          $cleanup(/** @type {any} */ (root.owner), () => live.splice(live.indexOf(id), 1));
          if (id === 'bad') throw new Error('bad build');
          return id;
        }),
      );
      assert.deepEqual($read(rows), ['a']);
      $write(items, [{ id: 'a' }, { id: 'x' }, { id: 'bad' }]);
      assert.throws(() => $read(rows), /bad (build|key)/);
      assert.deepEqual(live, ['a'], `${name}: the rows of the failed update are stopped`);
      $write(items, [{ id: 'a' }, { id: 'x' }]);
      assert.deepEqual($read(rows), ['a', 'x'], `${name}: a later update builds again`);
      assert.deepEqual(live, ['a', 'x'], `${name}: no duplicate row is live`);
      $stop(owner);
      assert.deepEqual(live, []);
      $dispose(root);
    }
  });

  it('maps by position with $positions', () => {
    const root = $root('app');
    const items = $signal(root, ['x', 'y']);
    let built = 0;
    const rows = $positions(root, () => $read(items), (item, index) => {
      built++;
      return $memo(root, () => `${$read(index)}=${$read(item)}`);
    });
    const view = () => $read(rows).map(m => $read(m));
    assert.deepEqual(view(), ['0=x', '1=y']);
    $write(items, ['z', 'y', 'w']);
    assert.deepEqual(view(), ['0=z', '1=y', '2=w']);
    assert.equal(built, 3);
    $write(items, ['q']);
    assert.deepEqual(view(), ['0=q']);
    $dispose(root);
  });

  it('selects in O(1): only the old and new keys notify', () => {
    const root = $root('app');
    const selected = $signal(root, 1);
    const is = $select(root, () => $read(selected));
    const runs = new Array(100).fill(0);
    for (let key = 0; key < 100; key++)
      $effect(root, () => {
        is(key);
        runs[key]++;
      });
    $write(selected, 50);
    $flush(root);
    assert.equal(runs.reduce((t, x) => t + x, 0), 102);
    assert.equal(runs[1], 2);
    assert.equal(runs[50], 2);
    assert.equal(is(50), true);
    $dispose(root);
  });

  it('gives Rows items a signal only once they are read tracked', () => {
    const root = $root('app');
    const items = $signal(root, [{ id: 1, v: 'a' }, { id: 2, v: 'b' }]);
    const rows = $rows(root, () => $read(items), x => x.id, item => ({ first: item().v, item }));
    const seen = [];
    $effect(root, () => {
      $read(rows);
    });
    const plain = $stats(root).nodes;
    $effect(root, () => {
      seen.push($read(rows)[0].item().v);
    });
    assert.equal($stats(root).nodes, plain + 2, 'the effect, and one item signal for the row it read');
    $write(items, [{ id: 1, v: 'z' }, { id: 2, v: 'y' }]);
    $flush(root);
    assert.deepEqual(seen, ['a', 'z'], 'a new item under the same key reaches the tracked reader');
    assert.equal($read(rows)[1].item().v, 'y', 'the unread row follows too');
    assert.equal($read(rows)[1].first, 'b', 'rows are kept by key');
    $dispose(root);
  });

  it('releases the keys of a $select with its owner', () => {
    const root = $root('app');
    const sel = $signal(root, 0);
    /** @type {number[]} */
    const counts = [];
    for (let c = 0; c < 200; c++) {
      const scope = $scope(root, null);
      $enter(scope, () => {
        const is = $select(root, () => $read(sel));
        for (let i = 0; i < 10; i++) $effect(root, () => is(i));
      });
      $flush(root);
      $stop(scope);
      $flush(root);
      counts.push($stats(root).nodes);
    }
    assert.equal(counts[199], counts[0], `the node count stays flat (${counts[0]} after one, ${counts[199]} after 200)`);
    $dispose(root);
  });

  it('keeps the keys a memo reads in its first run, past the sweep size', () => {
    const root = $root('app');
    const sel = $signal(root, 0);
    const is = $select(root, () => $read(sel));
    // One memo reads 100 keys: a sweep during its run must not free the ones it read.
    const m = $memo(root, () => {
      const on = [];
      for (let i = 0; i < 100; i++) if (is(i)) on.push(i);
      return on.join(',');
    });
    let seen = '';
    $effect(root, () => void (seen = $read(m)));
    $flush(root);
    assert.equal(seen, '0');
    $write(sel, 3);
    $flush(root);
    assert.equal(seen, '3', 'the memo follows the selection');
    $write(sel, 70);
    $flush(root);
    assert.equal(seen, '70');
    $dispose(root);
  });

  it('releases the keys nothing observes, in amortized sweeps', () => {
    const root = $root('app');
    const selected = $signal(root, 0);
    const is = $select(root, () => $read(selected));
    const scope = $scope(root, null);
    $enter(scope, () => {
      for (let key = 0; key < 100; key++) $effect(root, () => is(key));
    });
    const full = $stats(root).nodes;
    $stop(scope);
    // New keys trigger a sweep once the table has doubled: the 100 unobserved go.
    const later = $scope(root, null);
    $enter(later, () => {
      for (let key = 100; key < 230; key++) $effect(root, () => is(key));
    });
    $stop(later);
    // The sweep waits for the flush's end, so a computation making keys never loses the ones it read.
    $flush(root);
    assert.ok($stats(root).nodes < full + 30, `unobserved keys were released (${$stats(root).nodes} nodes vs ${full})`);
    $write(selected, 5);
    $flush(root);
    assert.equal(is(5), true);
    $dispose(root);
  });

  it('keeps the new rows when a removed row fails to stop', () => {
    for (const [name, map] of /** @type {const} */ ([
      ['Keyed', (/** @type {any} */ root, /** @type {any} */ read, /** @type {any} */ build) => $keyed(root, read, null, (item) => build(() => $read(item)))],
      ['Rows', (/** @type {any} */ root, /** @type {any} */ read, /** @type {any} */ build) => $rows(root, read, null, build)],
      ['Positions', (/** @type {any} */ root, /** @type {any} */ read, /** @type {any} */ build) => $positions(root, read, (item) => build(() => $read(item)))],
    ])) {
      // Handled: delivered once to the root boundary, the list still maps.
      const root = $root('app');
      /** @type {string[]} */
      const seen = [];
      /** @type {number[]} */
      const cleaned = [];
      $catch(root, error => seen.push(error.message));
      const items = $signal(root, [1, 2, 3]);
      const rows = map(root, () => $read(items), (/** @type {() => number} */ n) => {
        const value = n();
        $cleanup(/** @type {any} */ ($owner(root)), () => cleaned.push(value));
        $cleanup(/** @type {any} */ ($owner(root)), () => {
          throw new Error('row ' + value);
        });
        return value;
      });
      /** @type {number[][]} */
      const shown = [];
      $effect(root, () => void shown.push($read(rows)));
      $write(items, name === 'Positions' ? [1, 2] : [1, 3]);
      $flush(root);
      assert.deepEqual($read(rows), name === 'Positions' ? [1, 2] : [1, 3], name);
      assert.deepEqual(shown.at(-1), $read(rows), name);
      assert.deepEqual(seen, [name === 'Positions' ? 'row 3' : 'row 2'], name);
      // Every cleanup of the removed row ran (newest first: the throwing one, then this).
      assert.deepEqual(cleaned, [name === 'Positions' ? 3 : 2], name);
      // The rows left still fail as they stop with the root.
      assert.throws(() => $dispose(root), /row [1-3]/, name);

      // Unhandled: the list still commits its rows; the failure is raised when the flush ends.
      const bare = $root('bare');
      const list = $signal(bare, [1, 2]);
      const mapped = map(bare, () => $read(list), (/** @type {() => number} */ n) => {
        const value = n();
        $cleanup(/** @type {any} */ ($owner(bare)), () => {
          throw new Error('gone ' + value);
        });
        return value;
      });
      /** @type {number[][]} */
      const got = [];
      $effect(bare, () => void got.push($read(mapped)));
      $write(list, [1]);
      assert.throws(() => $flush(bare), /gone 2/, name);
      assert.deepEqual(got.at(-1), [1], name);
      assert.deepEqual($read(mapped), [1], name);
      assert.throws(() => $dispose(bare), /gone 1/, name);
    }
  });

  it('merges and splits properties', () => {
    const merged = $merge([{ a: 1, b: 2 }, () => ({ b: 3 }), null]);
    assert.equal(merged('a'), 1);
    assert.equal(merged('b'), 3);
    assert.equal(merged('c'), null);
    const [left, rest] = $split(merged, [['a']]);
    assert.equal(left('a'), 1);
    assert.equal(left('b'), null);
    assert.equal(rest('a'), null);
    assert.equal(rest('b'), 3);
  });

  it('reads and writes a path the same with a number or a string key', () => {
    const root = $root('app');
    const s = $store(root, { byId: { 5: 'a' }, items: ['x', 'y'] });
    /** @type {any[]} */
    const seen = [];
    $effect(root, () => void seen.push(['byId "5"', $at(s, ['byId', '5'])]));
    $effect(root, () => void seen.push(['byId 5', $at(s, ['byId', 5])]));
    $effect(root, () => void seen.push(['items "1"', $at(s, ['items', '1'])]));
    $effect(root, () => void seen.push(['items 1', $at(s, ['items', 1])]));
    $flush(root);
    seen.length = 0;
    $put(s, ['byId', 5], 'b');
    $put(s, ['items', '1'], 'z');
    $flush(root);
    assert.deepEqual(seen.sort(), [['byId "5"', 'b'], ['byId 5', 'b'], ['items "1"', 'z'], ['items 1', 'z']]);
    assert.deepEqual($raw(s), { byId: { 5: 'b' }, items: ['x', 'z'] });
    assert.equal(Array.isArray($raw(s).items), true, 'a string index keeps the array');
    assert.throws(() => $put(s, ['items', '01'], 'q'), /index out of bounds/, 'only a canonical numeric string is an index');
    $dispose(root);
  });

  it('merges index keys into a copy of an array with $update, keeping it an array', () => {
    const root = $root('app');
    const s = $store(root, { items: ['x', 'y'] });
    /** @type {any[]} */
    const seen = [];
    $effect(root, () => void seen.push($at(s, ['items', 0])));
    $flush(root);
    const before = $raw(s).items;
    $update(s, ['items'], { 0: 'q' });
    $flush(root);
    assert.deepEqual($raw(s).items, ['q', 'y']);
    assert.equal(Array.isArray($raw(s).items), true, 'still an array');
    assert.deepEqual(before, ['x', 'y'], 'a copy: the old array is left as it was');
    assert.deepEqual(seen, ['x', 'q']);
    assert.throws(() => $update(s, ['items'], { name: 'z' }), /index out of bounds/, 'a key that is not an index is refused');
    assert.throws(() => $update(s, ['items'], { 2: 'z' }), /index out of bounds/, 'as $put refuses one past the end');
    assert.deepEqual($raw(s).items, ['q', 'y'], 'a refused update changes nothing');
    $dispose(root);
  });

  it("reruns an overlay reader when a layer on an ancestor path reads a field that changed", () => {
    const root = $root('app');
    const store = $store(root, { user: { first: 'a', full: 'a' } });
    const o = $overlay(root, store);
    /** @type {any[]} */
    const seen = [];
    $watch(root, () => o.read(['user', 'full']), v => void seen.push(v));
    // The layer at ['user'] computes full from first, a sibling of the path read.
    o.apply(['user'], (/** @type {any} */ u) => ({ ...u, full: u.first + '!' }));
    $flush(root);
    $put(store, ['user', 'first'], 'b');
    $flush(root);
    assert.deepEqual(seen, ['a', 'a!', 'b!']);
    $dispose(root);
  });

  it('keeps a record\'s prototype and an own __proto__ key through $mirror, $put and $update', () => {
    const root = $root('app');
    const bare = Object.assign(Object.create(null), { a: 1 });
    const keyed = JSON.parse('{"__proto__": {"x": 1}, "b": 2}');
    const source = $signal(root, /** @type {any} */ ({ bare, keyed }));
    const mirror = $mirror(root, () => $read(source));
    $write(source, { bare: Object.assign(Object.create(null), { a: 2 }), keyed: JSON.parse('{"__proto__": {"x": 2}, "b": 2}') });
    $flush(root);
    const m = $raw(mirror);
    assert.equal(Object.getPrototypeOf(m.bare), null, '$mirror keeps a null prototype');
    assert.equal(Object.getPrototypeOf(m.keyed), Object.prototype, 'an own __proto__ key does not become the prototype');
    assert.deepEqual(Object.getOwnPropertyDescriptor(m.keyed, '__proto__')?.value, { x: 2 });
    const s = $store(root, { rec: Object.assign(Object.create(null), { a: 1 }) });
    $put(s, ['rec', '__proto__'], { y: 1 });
    assert.equal(Object.getPrototypeOf($raw(s).rec), null, '$put keeps a null prototype');
    assert.deepEqual(Object.getOwnPropertyDescriptor($raw(s).rec, '__proto__')?.value, { y: 1 }, '$put writes __proto__ as a key');
    $update(s, ['rec'], JSON.parse('{"__proto__": {"z": 1}}'));
    assert.equal(Object.getPrototypeOf($raw(s).rec), null, '$update keeps a null prototype');
    assert.deepEqual(Object.getOwnPropertyDescriptor($raw(s).rec, '__proto__')?.value, { z: 1 }, '$update writes __proto__ as a key');
    $dispose(root);
  });

  it('writes into a primitive, a Date, a Map or an inherited key as a plain record', () => {
    const root = $root('app');
    const s = $store(root, /** @type {any} */ ({ count: 5, when: new Date(0), m: new Map() }));
    $put(s, ['count', 'x'], 1);
    assert.equal(Object.getPrototypeOf($raw(s).count), Object.prototype);
    assert.equal(`${$raw(s).count}`, '[object Object]', 'a usable record, not a broken Number');
    $put(s, ['when', 'x'], 1);
    assert.equal(String($raw(s).when), '[object Object]');
    $update(s, ['m'], { a: 1 });
    assert.deepEqual($raw(s).m, { a: 1 });
    $put(s, ['constructor', 'x'], 1);
    assert.equal(Object.getPrototypeOf($raw(s).constructor), Object.prototype, 'an inherited key reads its value, a function, as nothing to copy');
    $dispose(root);
  });

  it('reads own keys only: an inherited member is missing, never Object or its prototype', () => {
    const root = $root('app');
    const s = $store(root, /** @type {any} */ ({ counts: { a: 1 }, list: [1, 2], rec: Object.assign(Object.create(null), { b: 2 }) }));
    assert.equal($at(s, ['counts', 'constructor']), null, 'untracked');
    assert.equal($at(s, ['__proto__']), null);
    assert.equal($at(s, ['counts', 'toString']), null);
    assert.equal($at(s, ['constructor', 'prototype']), null);
    assert.equal($at(s, ['list', 'length']), 2, "an array's length is its own");
    assert.equal($at(s, ['list', 1]), 2);
    const seen = [];
    $effect(root, () => {
      seen.push([$at(s, ['counts', 'constructor']), $at(s, ['counts', '__proto__']), $at(s, ['counts', 'a'])]);
    });
    $flush(root);
    assert.deepEqual(seen, [[null, null, 1]], 'tracked');
    /** @type {any[]} */
    const given = [];
    $put(s, ['counts', 'constructor'], (/** @type {any} */ old) => {
      given.push(old);
      return (old ?? 0) + 1;
    });
    $put(s, ['__proto__'], (/** @type {any} */ old) => {
      given.push(old);
      return { x: 1 };
    });
    assert.deepEqual(given, [null, null], 'an updater is given nothing for an inherited key');
    assert.equal($raw(s).counts.constructor, 1, 'stored as an own key');
    assert.equal(Object.prototype.x, undefined, 'Object.prototype is untouched');
    assert.equal(Object.getPrototypeOf($raw(s)), Object.prototype);
    assert.deepEqual(Object.getOwnPropertyDescriptor($raw(s), '__proto__')?.value, { x: 1 });
    $flush(root);
    assert.deepEqual(seen.at(-1), [1, null, 1], 'the own key notifies its reader');
    $dispose(root);
  });
});

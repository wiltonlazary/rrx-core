// The records core (core.js) and the arena (arena.js) keep one contract, so
// the same generated program must log the same on both: fixed seeds build
// programs of signals, memos (throwing, custom equality), effects, watches
// and renders (writing, deferring, stopping their scope, disposing the
// root), scopes (catching), stops, retires, sweeps, urgent, deferred and
// mixed writes, batches, flushes, reads, $after, $settled, discards, $auto
// and cleanups (registered and released in churns, earlier keys released);
// applications that stop other scopes and make new Renders and Watches
// (cascades), so an application run in a gone node's queue place shows;
// writes under a hold; and the program ends with the root disposed under a
// hold. Every step's log and $stats must agree. It has found the arena's
// divergences. On a difference it prints the seed and the ops up to the
// first differing step.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import * as records from '../src/core.js';
import * as arena from '../src/arena.js';

/**
 * The seeds: 1 to 1800, and the seeds that once found a divergence the
 * range misses (4280: a cancelled effect's queue entry run for the effect
 * that took its id). About 3 s in all.
 */
const SEEDS = [...Array.from({ length: 1800 }, (_, i) => i + 1), 4280];
/** Ops per program. */
const LENGTH = 60;

function rng(seed) {
  let s = seed >>> 0 || 1;
  return () => {
    s ^= s << 13; s >>>= 0; s ^= s >>> 17; s ^= s << 5; s >>>= 0;
    return s / 4294967296;
  };
}

function gen(seed, n) {
  const r = rng(seed);
  const pick = k => Math.floor(r() * k);
  // Ops added after the first programs draw from a second stream (cascades,
  // the cleanup churn), so a seed's first-stream program never changes; the
  // holds draw from a third, so the second stream's draws stay as they were.
  const r2 = rng(seed ^ 0x9e3779b9);
  const more = k => Math.floor(r2() * k);
  const r3 = rng(seed ^ 0x85ebca6b);
  const hold = k => Math.floor(r3() * k);
  const ops = [];
  let nodes = 0; // signals + memos (readables)
  let sigs = [];
  let scopes = 1; // scope 0 = root
  let cleanups = 0;
  let effs = 0;
  const readables = [];
  for (let i = 0; i < n; i++) {
    const t = r();
    if (nodes < 3 || t < 0.12) { ops.push({ op: 'sig', v: pick(10), id: nodes }); sigs.push(nodes); readables.push(nodes); nodes++; }
    else if (t < 0.24) {
      const deps = Array.from({ length: 1 + pick(3) }, () => readables[pick(readables.length)]);
      ops.push({ op: 'memo', deps, id: nodes, owner: pick(scopes), thr: pick(5) === 0, eq: pick(4) === 0 }); readables.push(nodes); nodes++;
    } else if (t < 0.36) {
      const deps = Array.from({ length: 1 + pick(3) }, () => readables[pick(readables.length)]);
      ops.push({ op: ['effect', 'watch', 'render'][pick(3)], deps, id: effs++, owner: pick(scopes), stopSelf: pick(12) === 0, dispose: pick(40) === 0, write: pick(6) === 0 ? sigs[pick(sigs.length)] : -1, dw: pick(3) === 0,
        // While it applies: stop another scope (its queued applications go) and make a Render or Watch (it may take a freed id).
        stopOther: more(4) === 0 ? 1 + more(Math.max(1, scopes - 1)) : 0, spawn: more(4) === 0 ? ['watch', 'render'][more(2)] : '' });
    } else if (t < 0.42) { ops.push({ op: 'scope', parent: pick(scopes), id: scopes++, catch: pick(2) === 0 }); }
    else if (t < 0.46) { ops.push({ op: 'stop', s: 1 + pick(Math.max(1, scopes - 1)) }); }
    else if (t < 0.48) { ops.push({ op: 'retire', s: 1 + pick(Math.max(1, scopes - 1)) }); }
    else if (t < 0.50) { ops.push({ op: 'sweep' }); }
    else if (t < 0.58) { const k = pick(4); ops.push({ op: ['defer', 'mixed', 'deferBatch', 'deferFlush'][k], s: sigs[pick(sigs.length)], s2: sigs[pick(sigs.length)], v: pick(10), v2: pick(10) }); }
    else if (t < 0.66) { ops.push({ op: 'write', s: sigs[pick(sigs.length)], v: pick(10) }); }
    else if (t < 0.72) { ops.push({ op: 'batch', w: Array.from({ length: 2 }, () => [sigs[pick(sigs.length)], pick(10)]) }); }
    else if (t < 0.84) { ops.push({ op: 'flush' }); }
    else if (t < 0.88) { ops.push({ op: 'read', n: readables[pick(readables.length)] }); }
    else if (t < 0.90) { const k = pick(4); ops.push({ op: ['defer', 'mixed', 'deferBatch', 'deferFlush'][k], s: sigs[pick(sigs.length)], s2: sigs[pick(sigs.length)], v: pick(10), v2: pick(10) }); }
    else if (t < 0.92) { ops.push({ op: 'after' }); }
    else if (t < 0.94) { ops.push({ op: 'settled' }); }
    else if (t < 0.96) { ops.push({ op: 'discard', n: readables[pick(readables.length)] }); }
    else if (t < 0.98) { ops.push({ op: 'auto', on: pick(2) === 0 }); }
    else {
      // From the second stream: a churn of cleanups registered and released (compaction), and the release of an earlier key.
      ops.push({ op: 'cleanup', s: pick(scopes), churn: more(3) === 0 ? more(60) : 0, drop: more(2) === 0 ? more(4 * cleanups + 1) - 1 : -1 });
      cleanups++;
    }
    // From the third stream: a write under a hold (the root stays open here).
    if (hold(20) === 0) ops.push({ op: 'held', s: sigs[hold(sigs.length)], v: hold(10), dispose: false });
    // From the second stream: a cascade, an application that stops a scope holding queued ones and makes a new one.
    if (more(12) === 0) ops.push({ op: 'cascade', id: i, deps: [readables[more(readables.length)]], kind: ['$render', '$watch'][more(2)] });
  }
  ops.push({ op: 'flush' });
  // Last, from the third stream: the root disposed under a hold, then left (the program's end).
  ops.push({ op: 'held', s: sigs[hold(sigs.length)], v: hold(10), dispose: true });
  return ops;
}

function run(g, ops) {
  const log = [];
  const L = x => log.push(String(x).replace(/Cannot read properties of (undefined|null) \(reading '[^']*'\)/g, 'TypeError'));
  const root = g.$root('app');
  g.$catch(root, e => L('root-catch:' + (e && e.message)));
  const nodes = [];
  const scopes = [null];
  const effects = [];
  /** @type {boolean[]} */
  const spawned = [];
  /** The scope and key of each cleanup the 'cleanup' ops keep. @type {[number, any][]} */
  const keys = [];
  const own = i => (i === 0 ? null : scopes[i]);
  const inScope = (i, f) => {
    const s = own(i);
    if (s == null) return f();
    return g.$enter(s, f);
  };
  const val = x => { try { return g.$read(nodes[x]); } catch (e) { return 'E:' + (e && e.message); } };
  const reads = deps => deps.map(d => { const v = g.$read(nodes[d]); return typeof v === 'number' ? v : 0; });
  const step = (i, o) => {
    switch (o.op) {
      case 'sig': nodes[o.id] = g.$signal(root, o.v); break;
      case 'memo': nodes[o.id] = inScope(o.owner, () => {
        const m = g.$memo(root, () => {
          const v = reads(o.deps[0] !== undefined && (g.$read(nodes[o.deps[0]]) & 1) ? o.deps : o.deps.slice(0, 1));
          const sum = v.reduce((a, b) => a + b, 0);
          if (o.thr && sum % 3 === 0) throw new Error('m' + o.id);
          return sum % 17;
        });
        if (o.eq) g.$equals(m, (a, b) => (a & 1) === (b & 1));
        return m;
      }); break;
      case 'effect': case 'watch': case 'render': inScope(o.owner, () => {
        const body = () => {
          const v = reads(o.deps);
          return v;
        };
        const act = v => {
          L(`${o.op}${o.id}:${v}:d=${g.$deferred(root)}`);
          if (o.write >= 0 && nodes[o.write]) { if (o.dw) g.$defer(root, () => g.$write(nodes[o.write], v[0] % 4)); else g.$write(nodes[o.write], v[0] % 4); }
          if (o.stopSelf && o.owner !== 0) g.$stop(scopes[o.owner]);
          if (o.stopOther > 0 && scopes[o.stopOther]) g.$stop(scopes[o.stopOther]);
          if (o.spawn && !g.$closed(root) && !spawned[o.id]) {
            spawned[o.id] = true;
            const T = g.$scope(root, null);
            g.$enter(T, () => g[o.spawn === 'watch' ? '$watch' : '$render'](root, body, (/** @type {any} */ w) => void L(`sp${o.id}:${w}`)));
          }
          if (o.dispose) g.$dispose(root);
          return () => L(`c${o.id}`);
        };
        if (o.op === 'effect') effects[o.id] = g.$effect(root, () => act(body()));
        else effects[o.id] = g[o.op === 'watch' ? '$watch' : '$render'](root, body, act);
      }); break;
      case 'scope': {
        const s = g.$scope(root, own(o.parent));
        scopes[o.id] = s;
        if (o.catch) g.$catch(s, e => L(`catch${o.id}:${e && e.message}`));
        g.$cleanup(s, () => L(`sc${o.id}`));
        break;
      }
      case 'stop': if (scopes[o.s]) g.$stop(scopes[o.s]); break;
      case 'retire': if (scopes[o.s]) g.$retire(scopes[o.s]); break;
      case 'sweep': L('sweep:' + g.$sweep(root, 1000)); break;
      case 'write': L('w:' + g.$write(nodes[o.s], o.v)); break;
      case 'batch': g.$batch(root, () => { for (const [s, v] of o.w) g.$write(nodes[s], v); }); break;
      case 'flush': g.$flush(root); break;
      case 'read': L('r' + o.n + ':' + val(o.n)); break;
      case 'defer': g.$defer(root, () => g.$write(nodes[o.s], o.v)); break;
      case 'mixed': g.$defer(root, () => g.$write(nodes[o.s], o.v)); g.$write(nodes[o.s2], o.v2); break;
      case 'deferBatch': g.$batch(root, () => { g.$defer(root, () => g.$write(nodes[o.s], o.v)); g.$write(nodes[o.s2], o.v2); }); break;
      case 'deferFlush': g.$defer(root, () => { g.$write(nodes[o.s], o.v); g.$write(nodes[o.s2], o.v2); }); g.$flush(root); break;
      case 'after': g.$after(root, () => L('after' + i)); break;
      case 'settled': g.$settled(root, () => L('settled' + i)); break;
      case 'discard': g.$discard(nodes[o.n]); break;
      case 'auto': g.$auto(root, o.on); break;
      case 'cleanup': {
        if (o.s === 0) break;
        keys.push([o.s, g.$cleanup(scopes[o.s], () => L('cu' + i))]);
        for (let j = 0; j < (o.churn ?? 0); j++) {
          const key = g.$cleanup(scopes[o.s], () => L(`ch${i}.${j}`));
          if (j % 7 !== 0) g.$release(scopes[o.s], key);
          else keys.push([o.s, key]);
        }
        const drop = keys[o.drop ?? -1];
        if (drop !== undefined) g.$release(scopes[drop[0]], drop[1]);
        break;
      }
      case 'held':
        g.hold(root);
        try {
          g.$write(nodes[o.s], o.v);
          if (o.dispose) g.$dispose(root);
        } finally {
          g.unhold(root);
        }
        break;
      case 'cascade': {
        // X applies first, W1 (in S) and Y are queued after it; X stops S and makes W2, which may take W1's id.
        const S = g.$scope(root, null);
        let armed = false;
        g.$render(root, () => reads(o.deps), v => {
          L(`cx${o.id}:${v}`);
          if (!armed || g.$closed(root)) return;
          armed = false;
          g.$stop(S);
          const T = g.$scope(root, null);
          g.$enter(T, () => g[o.kind](root, () => reads(o.deps), w => void L(`cw${o.id}:${w}`)));
        });
        g.$enter(S, () => g.$render(root, () => reads(o.deps), v => void L(`c1${o.id}:${v}`)));
        g.$render(root, () => reads(o.deps), v => void L(`cy${o.id}:${v}`));
        armed = true;
        break;
      }
    }
  };
  let guard = 0;
  for (let i = 0; i < ops.length; i++) {
    const o = ops[i];
    try { step(i, o); } catch (e) { L(`throw@${i}:${o.op}:${e && e.message}`); }
    if (!g.$closed(root)) { const st = g.$stats(root); delete st.links; L(`stats@${i}:${JSON.stringify(st)}`); }
    else L('closed@' + i);
    if (++guard > 100000) break;
  }
  return log;
}

describe('rrx-core records and arena, differential', () => {
  it(`log the same over ${SEEDS.length} generated programs`, { timeout: 60000 }, () => {
    /** @type {string[]} */
    const report = [];
    for (const seed of SEEDS) {
      const ops = gen(seed, LENGTH);
      let a, b;
      try {
        a = run(records, ops);
      } catch (e) {
        a = ['CRASH ' + /** @type {any} */ (e).stack];
      }
      try {
        b = run(arena, ops);
      } catch (e) {
        b = ['CRASH ' + /** @type {any} */ (e).stack];
      }
      const k = Math.max(a.length, b.length);
      for (let i = 0; i < k; i++) {
        if (a[i] === b[i]) continue;
        const at = +(String(a[i] ?? b[i]).match(/@(\d+)/)?.[1] ?? ops.length - 1);
        report.push(`seed ${seed} differs at log ${i}: records=${a[i]} arena=${b[i]}\n  ops: ${JSON.stringify(ops.slice(0, at + 1))}`);
        break;
      }
      if (report.length >= 5) break;
    }
    if (report.length > 0) console.log(report.join('\n'));
    assert.deepEqual(report.map(r => r.split('\n')[0]), [], 'the seeds where the arena and the records core differ (their ops are printed above)');
  });
});

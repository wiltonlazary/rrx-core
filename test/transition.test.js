// Transitions: an update that makes a shown value pending is held, whole,
// until the value is ready, so the screen never mixes new and old state
// (the graph holds a consistent state; `$pending` and `$lastReady` read it).

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  $root, $signal, $read, $write, $memo, $render, $watch, $effect, $dispose, $auto, $batch, $task, $pending, $lastReady, $cancel, $flush, $catch, $outcome, $scope, $enter,
} from '../src/index.js';

function deferred() {
  /** @type {(value: any) => void} */ let resolve = () => {};
  const promise = new Promise(ok => (resolve = ok));
  return { promise, resolve };
}

/** A root with an id signal and an async user memo keyed by it; `answer(id, value)` completes a fetch. */
function app() {
  const root = $root('app');
  $auto(root, true);
  const id = $signal(root, 1);
  /** Each fetch and the promise the memo follows. @type {Map<number, { d: ReturnType<typeof deferred>, named: Promise<string> }>} */
  const fetches = new Map();
  const user = $memo(root, () => {
    const key = $read(id);
    const d = deferred();
    const named = d.promise.then(name => `${name}#${key}`);
    fetches.set(key, { d, named });
    return named;
  });
  // Memos are lazy: a first read starts the fetch.
  $pending(user);
  const answer = async (/** @type {number} */ key, /** @type {string} */ name) => {
    const fetch = /** @type {{ d: ReturnType<typeof deferred>, named: Promise<string> }} */ (fetches.get(key));
    fetch.d.resolve(name);
    // The memo's handler on `named` runs before this await resumes.
    await fetch.named;
  };
  return { root, id, user, answer };
}

describe('rrx-core transitions', () => {
  it('holds every application of the update until the async value is ready: no tearing', async () => {
    const { root, id, user, answer } = app();
    /** @type {string[]} */
    const screen = [];
    await answer(1, 'ada');
    $render(root, () => $read(id), v => void screen.push(`id=${v}`));
    $render(root, () => $read(user), v => void screen.push(`user=${v}`));
    assert.deepEqual(screen, ['id=1', 'user=ada#1']);
    screen.length = 0;
    $write(id, 2);
    assert.deepEqual(screen, [], 'the id is not shown ahead of its user');
    assert.equal($read(id), 2, 'reads in code see the newest state at once');
    await answer(2, 'grace');
    assert.deepEqual(screen, ['id=2', 'user=grace#2'], 'both apply together, in creation order');
    $dispose(root);
  });

  it('applies indicators at once: $pending and $lastReady readers are never held', async () => {
    const { root, id, user, answer } = app();
    /** @type {string[]} */
    const screen = [];
    $render(root, () => $pending(user), v => void screen.push(`pending=${v}`));
    $render(root, () => $lastReady(id), v => void screen.push(`typed=${v}`));
    $render(root, () => $lastReady(user), v => void screen.push(`latest=${v}`));
    await answer(1, 'ada');
    $render(root, () => $read(user), v => void screen.push(`user=${v}`));
    screen.length = 0;
    $write(id, 2);
    assert.deepEqual(screen, ['pending=true', 'typed=2'], 'the indicators show the change in flight');
    await answer(2, 'grace');
    assert.deepEqual(screen, ['pending=true', 'typed=2', 'pending=false', 'latest=grace#2', 'user=grace#2']);
    $dispose(root);
  });

  it('lets unrelated updates through while a transition holds', async () => {
    const { root, id, user, answer } = app();
    const clock = $signal(root, 0);
    /** @type {string[]} */
    const screen = [];
    await answer(1, 'ada');
    $render(root, () => $read(user), v => void screen.push(`user=${v}`));
    $render(root, () => $read(clock), v => void screen.push(`clock=${v}`));
    screen.length = 0;
    $write(id, 2);
    $write(clock, 1);
    assert.deepEqual(screen, ['clock=1'], 'the clock does not wait for the user');
    await answer(2, 'grace');
    assert.deepEqual(screen, ['clock=1', 'user=grace#2']);
    $dispose(root);
  });

  it('joins later updates that touch held work, and commits the latest state', async () => {
    const { root, id, user, answer } = app();
    /** @type {string[]} */
    const screen = [];
    await answer(1, 'ada');
    $render(root, () => $read(id), v => void screen.push(`id=${v}`));
    $render(root, () => $read(user), v => void screen.push(`user=${v}`));
    screen.length = 0;
    $write(id, 2);
    $write(id, 3);
    await answer(2, 'late');
    assert.deepEqual(screen, [], 'a superseded answer commits nothing');
    await answer(3, 'lin');
    assert.deepEqual(screen, ['id=3', 'user=lin#3']);
    $dispose(root);
  });

  it('keeps independent transitions apart, and merges them when an update touches both', async () => {
    const root = $root('app');
    $auto(root, true);
    const a = $signal(root, 1);
    const b = $signal(root, 1);
    /** @type {Map<string, ReturnType<typeof deferred>>} */
    const fetches = new Map();
    const load = (/** @type {string} */ name, /** @type {any} */ source) =>
      $memo(root, () => {
        const key = `${name}${$read(source)}`;
        const d = deferred();
        fetches.set(key, d);
        return d.promise;
      });
    const ra = load('a', a);
    const rb = load('b', b);
    const answer = async (/** @type {string} */ key) => {
      const d = /** @type {ReturnType<typeof deferred>} */ (fetches.get(key));
      d.resolve(key);
      await d.promise;
    };
    $pending(ra);
    $pending(rb);
    await answer('a1');
    await answer('b1');
    /** @type {string[]} */
    const screen = [];
    $render(root, () => $read(ra), v => void screen.push(`ra=${v}`));
    $render(root, () => $read(rb), v => void screen.push(`rb=${v}`));
    screen.length = 0;
    $write(a, 2);
    $write(b, 2);
    await answer('b2');
    assert.deepEqual(screen, ['rb=b2'], 'b commits on its own');
    await answer('a2');
    assert.deepEqual(screen, ['rb=b2', 'ra=a2']);
    screen.length = 0;
    $batch(root, () => {
      $write(a, 3);
      $write(b, 3);
    });
    await answer('b3');
    assert.deepEqual(screen, [], 'one update made both pending: one transition');
    await answer('a3');
    assert.deepEqual(screen, ['ra=a3', 'rb=b3']);
    $dispose(root);
  });

  it('releases held work when the suspended observer stops', async () => {
    const { root, id, user, answer } = app();
    /** @type {string[]} */
    const screen = [];
    await answer(1, 'ada');
    $render(root, () => $read(id), v => void screen.push(`id=${v}`));
    const shownUser = $render(root, () => $read(user), v => void screen.push(`user=${v}`));
    screen.length = 0;
    $write(id, 2);
    assert.deepEqual(screen, []);
    $cancel(shownUser.observer);
    assert.deepEqual(screen, ['id=2'], 'nothing waits any more: the held id applies');
    $dispose(root);
  });

  it('holds a task revalidation too, and never holds plain effects', () => {
    const root = $root('app');
    $auto(root, true);
    const id = $signal(root, 1);
    /** @type {((r: any) => void)[]} */
    const done = [];
    const task = $task(root, () => $read(id), (_, complete) => void done.push(complete));
    done[0]({ ok: true, value: 'one' });
    /** @type {string[]} */
    const screen = [];
    $watch(root, () => $read(id), v => void screen.push(`id=${v}`));
    $watch(root, () => $read(task), v => void screen.push(`task=${v}`));
    $effect(root, () => void screen.push(`effect=${$read(id)}`));
    screen.length = 0;
    $write(id, 2);
    assert.deepEqual(screen, ['effect=2'], 'a plain effect runs; the watches wait');
    done[1]({ ok: true, value: 'two' });
    $flush(root);
    assert.deepEqual(screen, ['effect=2', 'id=2', 'task=two']);
    $dispose(root);
  });

  it('ends the hold when the pending value fails: the failure reaches its boundary, the rest applies', () => {
    const root = $root('app');
    $auto(root, true);
    const id = $signal(root, 1);
    /** @type {((r: any) => void)[]} */
    const done = [];
    const task = $task(root, () => $read(id), (_, complete) => void done.push(complete));
    done[0]({ ok: true, value: 'one' });
    /** @type {string[]} */
    const screen = [];
    const boundary = $scope(root, null);
    $catch(boundary, error => void screen.push(`caught ${error.message}`));
    $watch(root, () => $read(id), v => void screen.push(`id=${v}`));
    $enter(boundary, () => $watch(root, () => $read(task), v => void screen.push(`task=${v}`)));
    $watch(root, () => $outcome(task).status, v => void screen.push(`outcome=${v}`));
    screen.length = 0;
    $write(id, 2);
    assert.deepEqual(screen, ['outcome=pending'], 'Outcome is an indicator');
    done[1]({ ok: false, error: new Error('down') });
    $flush(root);
    assert.deepEqual(screen, ['outcome=pending', 'caught down', 'id=2', 'outcome=failed']);
    $dispose(root);
  });
});

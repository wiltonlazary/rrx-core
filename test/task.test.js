// Tasks, readiness and optimistic updates.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  $root, $signal, $read, $write, $catch, $memo, $effect, $flush, $auto, $dispose, $scope, $enter, $stop, $watch, $settled, $untrack,
  $task, $load, $lastReady, $pending, $rerun, $behind, $whenDone, $stream, $outcome, $optimistic, $act, $changing, $kind, $whenUnwatched, $defer, $deferred, $observers,
  $context, $provide, $lookup, $current, $with, $owner,
} from '../src/index.js';

describe('rrx-core tasks', () => {
  it('keeps task source purity and caller ownership separate from its launch context', () => {
    const root = $root('app');
    const context = $context('fallback');
    const launchContext = $provide(root, context, 'launch', () => $current(root));
    const input = $signal(root, 1);
    const written = $signal(root, 0);
    const sources = [];
    const launches = [];
    let taskOwner = null;
    const task = $provide(root, context, 'source', () => {
      taskOwner = $owner(root);
      return $with(root, launchContext, () => $task(root, () => {
        const value = $read(input);
        sources.push({ value, owner: $owner(root).id, context: $lookup(root, context) });
        assert.throws(() => $write(written, 99), /computation cannot write/);
        return value;
      }, (value, complete) => {
        launches.push({ value, owner: $owner(root).id, context: $lookup(root, context) });
        $write(written, value);
        if (value === 2) $write(input, 3);
        complete({ ok: true, value });
      }));
    });
    const caller = $provide(root, context, 'caller', () => $owner(root));
    $enter(caller, () => {
      $write(input, 2);
      const read = $memo(root, () => {
        const value = $lastReady(task);
        assert.equal($owner(root), caller, 'a pulled launch restores its caller');
        assert.equal($lookup(root, context), 'caller');
        assert.throws(() => $write(written, 99), /computation cannot write/, 'the surrounding memo stays pure');
        return value;
      });
      assert.equal($read(read), 3, 'a self-writing launch rereads its input');
      assert.equal($owner(root), caller);
      assert.equal($lookup(root, context), 'caller');
    });
    assert.deepEqual(sources[0], { value: 1, owner: taskOwner.id, context: 'source' });
    assert.equal(sources.some(source => source.value === 2), true);
    assert.equal(sources.some(source => source.value === 3), true);
    for (const source of sources.slice(1)) {
      assert.equal(source.owner, caller.id, 'every pulled or stabilizing read uses the caller owner');
      assert.equal(source.context, 'source', 'every source read uses the task owner context');
    }
    assert.deepEqual(launches, [1, 2, 3].map(value => ({ value, owner: taskOwner.id, context: 'launch' })));
    assert.equal($read(written), 3, 'launch work may write');
    assert.equal($lookup(root, context), 'fallback');
    $dispose(root);
  });

  it('keeps captured readiness immutable and observes several completions before a flush', () => {
    const root = $root('app');
    const input = $signal(root, 1);
    const replies = [];
    const task = $task(root, input, (_, complete) => void replies.push(complete));
    const pending = task.current();
    replies[0]({ ok: true, value: 1 });
    const first = task.current();
    const memo = $memo(root, () => $read(task));
    assert.equal($read(memo), 1);
    $write(input, 2);
    $flush(root);
    const waiting = task.current();
    replies[1]({ ok: true, value: 2 });
    $rerun(task);
    replies[2]({ ok: true, value: 3 });
    assert.equal($read(memo), 3, 'two changes cannot cancel their notification');
    assert.deepEqual(pending, { status: 'pending', value: null, error: null, behind: false });
    assert.deepEqual(first, { status: 'ready', value: 1, error: null });
    assert.deepEqual(waiting, { status: 'pending', value: 1, error: null, behind: true });
    $dispose(root);
  });

  it('distinguishes a stream ending at the same value without rerunning value readers', () => {
    const root = $root('app');
    let emit = null;
    let end = null;
    const stream = $stream(root, () => 1, (_, next, complete) => {
      emit = next;
      end = complete;
    });
    const states = [];
    const values = [];
    $effect(root, () => void states.push(stream.current()));
    $effect(root, () => void values.push($lastReady(stream)));
    emit(2);
    $flush(root);
    const streaming = states.at(-1);
    emit(2);
    end({ ok: true, value: 2 });
    $flush(root);
    assert.deepEqual(values, [null, 2]);
    assert.equal(states.length, 3, 'pending, streaming, then terminal readiness');
    assert.equal(streaming.streaming, true, 'the earlier snapshot is immutable');
    assert.deepEqual(states.at(-1), { status: 'ready', value: 2, error: null });
    $dispose(root);
  });

  it('reads a task in ordinary computations and ignores late completions', () => {
    const root = $root('app');
    const source = $signal(root, 1);
    const derived = $memo(root, () => $read(source) * 2);
    const replies = [];
    let cancelled = 0;
    const scope = $scope(root, null);
    const task = $enter(scope, () =>
      $task(root, () => $read(source), (input, complete) => {
        replies.push({ input, complete });
        return () => cancelled++;
      }),
    );
    assert.equal($kind(task), 'task');
    const output = $load(root, () => $read(task) + $read(derived));
    assert.equal($read(output).status, 'pending');
    assert.equal($pending(task), true);
    replies[0].complete({ ok: true, value: 10 });
    $flush(root);
    assert.equal($read(output).value, 12);
    assert.equal($lastReady(task), 10);
    $write(source, 3);
    $flush(root);
    assert.equal(replies.length, 2);
    assert.equal(cancelled, 1, 'a new input cancels the running work');
    assert.equal($read(output).status, 'pending');
    assert.equal($read(output).value, 12, 'Load keeps the previous ready value');
    assert.equal($behind(task), true);
    replies[0].complete({ ok: true, value: 99 });
    assert.equal($pending(task), true, 'a late completion cannot replace the new generation');
    replies[1].complete({ ok: true, value: 20 });
    $flush(root);
    assert.equal($read(output).value, 26);
    assert.equal($behind(task), false);
    $rerun(task);
    $flush(root);
    assert.equal(replies.length, 3, '$rerun runs the work again for the current input');
    replies[2].complete({ ok: false, error: new Error('down') });
    $flush(root);
    assert.equal($outcome(task).status, 'failed');
    assert.equal($lastReady(task), 20);
    $stop(scope);
    assert.equal(cancelled, 3, 'stopping cancels the last work');
    $dispose(root);
  });

  it('keeps a memo over a pending task pending, not failed, through long chains', () => {
    const root = $root('app');
    let complete = null;
    const task = $task(root, () => 1, (input, done) => {
      complete = done;
    });
    let last = $memo(root, () => $read(task));
    for (let i = 0; i < 50; i++) {
      const previous = last;
      last = $memo(root, () => $read(previous) + 1);
    }
    const output = $load(root, () => $read(last));
    assert.equal($read(output).status, 'pending');
    assert.equal($read(output).status, 'pending', 'a cached pending memo still pends');
    complete({ ok: true, value: 1 });
    $flush(root);
    assert.equal($read(output).value, 51);
    $dispose(root);
  });

  it('streams values and resolves once', () => {
    const root = $root('app');
    let push = null;
    let end = null;
    const stream = $stream(root, () => 'q', (input, next, complete) => {
      push = next;
      end = complete;
    });
    const seen = [];
    $watch(root, () => $outcome(stream), outcome => {
      seen.push(`${outcome.status}:${outcome.value}`);
    });
    push(1);
    $flush(root);
    push(2);
    $flush(root);
    const resolved = [];
    $whenDone(root, stream, outcome => resolved.push(outcome.value));
    $flush(root);
    end({ ok: true, value: 3 });
    $flush(root);
    assert.deepEqual(seen, ['pending:null', 'ready:1', 'ready:2', 'ready:3']);
    assert.deepEqual(resolved, [2]);
    $dispose(root);
  });

  it('keeps the latest streamed value when the stream fails', () => {
    const root = $root('app');
    let emit = null;
    let end = null;
    const stream = $stream(root, () => 'q', (_, next, complete) => {
      emit = next;
      end = complete;
    });
    const seen = [];
    $watch(root, () => $lastReady(stream), value => void seen.push(value));
    emit(1);
    $flush(root);
    emit(2);
    $flush(root);
    const failure = new Error('stream failed');
    end({ ok: false, error: failure });
    $flush(root);
    assert.equal($lastReady(stream), 2);
    assert.equal($outcome(stream).error, failure);
    assert.throws(() => $read(stream), error => error === failure);
    assert.deepEqual(seen, [null, 1, 2]);
    $dispose(root);
  });

  it('runs $settled once no task is pending', () => {
    const root = $root('app');
    let complete = null;
    $task(root, () => 1, (input, done) => {
      complete = done;
    });
    let settled = 0;
    $settled(root, () => settled++);
    $flush(root);
    assert.equal(settled, 0);
    complete({ ok: true, value: 1 });
    $flush(root);
    assert.equal(settled, 1);
    $dispose(root);
  });

  it('rebases optimistic layers over a base that changes meanwhile', () => {
    const root = $root('app');
    const base = $signal(root, [1, 2]);
    const optimistic = $optimistic(root, base);
    const ticket = optimistic.apply(list => [...list, 3]);
    assert.deepEqual($read(optimistic.value), [1, 2, 3]);
    assert.equal($read(optimistic.pending), true);
    $write(base, [0]);
    assert.deepEqual($read(optimistic.value), [0, 3], 'an external write is rebased');
    optimistic.commit(ticket);
    assert.deepEqual($read(base), [0, 3]);
    assert.equal($read(optimistic.pending), false);
    const dropped = optimistic.apply(list => [...list, 9]);
    optimistic.settle(dropped);
    assert.deepEqual($read(optimistic.value), [0, 3]);
    $dispose(root);
  });

  it('acts optimistically: commit on success, drop on failure, stop on demand', () => {
    const root = $root('app');
    const base = $signal(root, 0);
    const optimistic = $optimistic(root, base);
    const pending = [];
    const results = [];
    const add = $act(root, optimistic, (current, n) => current + n, (n, complete) => {
      pending.push(complete);
      return () => results.push('cancelled');
    }, result => results.push(result.ok));
    add(5);
    assert.equal($read(optimistic.value), 5);
    pending[0]({ ok: true, value: null });
    assert.equal($read(base), 5);
    add(2);
    pending[1]({ ok: false, error: new Error('no') });
    assert.equal($read(optimistic.value), 5);
    const stop = add(7);
    assert.equal($read(optimistic.value), 12);
    stop();
    assert.equal($read(optimistic.value), 5);
    assert.deepEqual(results, [true, false, 'cancelled']);
    $dispose(root);
  });

  it('reruns a memo like $invalidate', () => {
    const root = $root('app');
    let external = 1;
    const m = $memo(root, () => external);
    assert.equal($read(m), 1);
    external = 2;
    $rerun(m);
    assert.equal($read(m), 2);
    $dispose(root);
  });

  it('pulls a task whose source changed up to date when it is read before a flush', () => {
    for (const kind of ['Task', 'Stream']) {
      const root = $root('app');
      const id = $signal(root, 13);
      /** @type {[number, (result: any) => void][]} */
      const launches = [];
      const task = kind === 'Task'
        ? $task(root, () => $read(id), (n, complete) => void launches.push([n, complete]))
        : $stream(root, () => $read(id), (n, _next, complete) => void launches.push([n, complete]));
      launches[0][1]({ ok: false, error: new Error('13 fails') });
      $flush(root);
      assert.equal($outcome(task).status, 'failed', kind);
      $write(id, 1);
      assert.equal($outcome(task).status, 'pending', `${kind}: a read pulls the new input`);
      assert.deepEqual(launches.map(l => l[0]), [13, 1], `${kind}: the new work launched at the read`);
      assert.equal($pending(task), true, kind);
      const memo = $memo(root, () => $outcome(() => $read(task)).status);
      assert.equal($read(memo), 'pending', `${kind}: a memo sees pending, not the old failure`);
      $flush(root);
      assert.equal(launches.length, 2, `${kind}: the flush does not launch the same input again`);
      // A read inside a pure computation pulls too, and a late completion stays ignored.
      $write(id, 2);
      assert.equal($read(memo), 'pending', kind);
      assert.deepEqual(launches.map(l => l[0]), [13, 1, 2], kind);
      launches[1][1]({ ok: true, value: 'late' });
      $flush(root);
      assert.equal($lastReady(task), null, `${kind}: the superseded completion was ignored`);
      launches[2][1]({ ok: true, value: 'two' });
      $flush(root);
      assert.equal($read(task), 'two', kind);
      assert.equal(launches.length, 3, kind);
      $dispose(root);
    }
  });

  it('launches once per input whether or not something reads it, and after its readers leave', () => {
    const root = $root('app');
    const id = $signal(root, 1);
    const shown = $signal(root, true);
    /** @type {number[]} */
    const launched = [];
    /** @type {((result: any) => void)[]} */
    const pending = [];
    const task = $task(root, () => $read(id), (n, complete) => {
      launched.push(n);
      pending.push(complete);
    });
    const seen = [];
    const reader = $scope(root, null);
    $enter(reader, () => $effect(root, () => {
      if ($read(shown)) seen.push($lastReady(task));
    }));
    pending[0]({ ok: true, value: 'one' });
    $flush(root);
    assert.deepEqual(seen, [null, 'one']);
    $write(id, 2);
    $flush(root);
    assert.deepEqual(launched, [1, 2], 'a read input launches once');
    // The reader stops reading: the task still launches for a new input.
    $write(shown, false);
    $flush(root);
    $write(id, 3);
    $flush(root);
    assert.deepEqual(launched, [1, 2, 3], 'with no reader left, the flush launches');
    $write(shown, true);
    $flush(root);
    $stop(reader);
    $write(id, 4);
    $flush(root);
    assert.deepEqual(launched, [1, 2, 3, 4], 'after the reader stopped, the flush launches');
    pending[3]({ ok: true, value: 'four' });
    $flush(root);
    assert.equal($read(task), 'four');
    $dispose(root);
  });

  it('propagates through memo input as task readers join and leave', () => {
    const root = $root('app');
    const input = $signal(root, 1);
    const doubled = $memo(root, () => $read(input) * 2);
    const launches = [];
    const replies = [];
    const task = $task(root, doubled, (value, complete) => {
      launches.push(value);
      replies.push(complete);
    });
    const latest = [];
    $effect(root, () => void latest.push($lastReady(task)));
    replies[0]({ ok: true, value: 2 });
    $flush(root);
    $write(input, 2);
    $flush(root);
    assert.deepEqual(launches, [2, 4], 'a sole effect still leaves the task eager');
    assert.deepEqual(latest, [null, 2], 'pending does not rerun the value reader');
    replies[1]({ ok: true, value: 4 });
    $flush(root);

    const extra = $scope(root, null);
    const states = [];
    $enter(extra, () => {
      const state = $memo(root, () => $outcome(task));
      $effect(root, () => {
        const current = $read(state);
        states.push(`${current.status}:${current.value}`);
      });
    });
    $write(input, 3);
    $flush(root);
    assert.deepEqual(states, ['ready:4', 'pending:4'], 'the added memo receives invalidation');
    replies[2]({ ok: true, value: 6 });
    $flush(root);
    assert.deepEqual(states, ['ready:4', 'pending:4', 'ready:6']);
    $stop(extra);
    $write(input, 4);
    $flush(root);
    replies[3]({ ok: true, value: 8 });
    $flush(root);
    assert.deepEqual(launches, [2, 4, 6, 8]);
    assert.deepEqual(latest, [null, 2, 4, 6, 8]);
    assert.deepEqual(states, ['ready:4', 'pending:4', 'ready:6'], 'the removed reader stays stopped');
    $dispose(root);
  });

  it('propagates task completions and blocked inputs to a task with one effect', () => {
    const root = $root('app');
    const input = $signal(root, 1);
    const upstreamReplies = [];
    const upstream = $task(root, input, (_, complete) => void upstreamReplies.push(complete));
    const launches = [];
    const replies = [];
    const downstream = $task(root, upstream, (value, complete) => {
      launches.push(value);
      replies.push(complete);
    });
    const states = [];
    $effect(root, () => void states.push($outcome(downstream).status));
    assert.deepEqual(launches, [], 'pending input does not start downstream work');
    upstreamReplies[0]({ ok: true, value: 10 });
    $flush(root);
    assert.deepEqual(launches, [10], 'publication launches the downstream task');
    assert.deepEqual(states, ['pending'], 'an unchanged pending result does not rerun its reader');
    replies[0]({ ok: true, value: 20 });
    $flush(root);
    $write(input, 2);
    $flush(root);
    const failure = new Error('upstream failed');
    upstreamReplies[1]({ ok: false, error: failure });
    $flush(root);
    assert.equal($outcome(downstream).error, failure);
    $write(input, 3);
    $flush(root);
    upstreamReplies[2]({ ok: true, value: 30 });
    $flush(root);
    assert.deepEqual(launches, [10, 30], 'recovery launches only ready inputs');
    replies[1]({ ok: true, value: 60 });
    $flush(root);
    assert.equal($read(downstream), 60);
    assert.deepEqual(states, ['pending', 'ready', 'pending', 'failed', 'pending', 'ready']);
    $dispose(root);
  });

  it('does not launch while the task or its owner stops', () => {
    const root = $root('app');
    const id = $signal(root, 1);
    /** @type {number[]} */
    const launched = [];
    let cancelled = 0;
    const scope = $scope(root, null);
    const task = $enter(scope, () => $task(root, () => $read(id), n => {
      launched.push(n);
      return () => cancelled++;
    }));
    $enter(scope, () => $effect(root, () => void $lastReady(task)));
    // A flush with a reader rests the eager observer; the reader launches.
    $write(id, 2);
    $flush(root);
    assert.deepEqual(launched, [1, 2]);
    $write(id, 3);
    $stop(scope);
    assert.deepEqual(launched, [1, 2], 'a changed input does not launch while its owner stops');
    assert.equal(cancelled, 2);
    const other = $task(root, () => $read(id), n => void launched.push(n));
    const outside = $scope(root, null);
    $enter(outside, () => $effect(root, () => void $lastReady(other)));
    $write(id, 4);
    $flush(root);
    assert.deepEqual(launched, [1, 2, 3, 4]);
    other.stop();
    $write(id, 5);
    $stop(outside);
    $flush(root);
    assert.deepEqual(launched, [1, 2, 3, 4], 'a stopped task does not launch when its reader leaves');
    $dispose(root);
  });

  it('reports outcomes of plain functions', () => {
    const root = $root('app');
    assert.deepEqual($outcome(() => 1), { status: 'ready', value: 1, error: null });
    assert.equal($outcome(() => {
      throw new Error('x');
    }).status, 'failed');
    assert.equal($pending(() => 1), false);
    assert.equal($untrack(root, () => $lastReady(() => 2)), 2);
    let ran = 0;
    $effect(root, () => {
      ran++;
    });
    assert.equal(ran, 1);
    $dispose(root);
  });

  it('relaunches when its work writes its own input while it starts, directly or through a memo', () => {
    for (const through of [false, true]) {
      const root = $root('app');
      const id = $signal(root, 1);
      const doubled = $memo(root, () => $read(id) * 2);
      const log = [];
      const task = $task(root, () => (through ? $read(doubled) : $read(id)), (input, complete) => {
        log.push(`start ${input}`);
        if ($untrack(root, () => $read(id)) === 1) $write(id, 2);
        return () => log.push(`cancel ${input}`);
      });
      $flush(root);
      const [first, second] = through ? [2, 4] : [1, 2];
      assert.deepEqual(log, [`start ${first}`, `cancel ${first}`, `start ${second}`], through ? 'through a memo' : 'directly');
      assert.equal($pending(task), true);
      $dispose(root);
    }
  });

  it('keeps its input observed while it lives, with no reader, and releases it when it stops', () => {
    const root = $root('app');
    const input = $signal(root, 1);
    let released = 0;
    let launched = 0;
    $whenUnwatched(input, () => void released++);
    const scope = $scope(root, null);
    $enter(scope, () => $task(root, () => $read(input), () => void launched++));
    $flush(root);
    $write(input, 2);
    $flush(root);
    assert.equal(launched, 2, 'it launches for every input with nothing reading it');
    assert.equal(released, 0);
    $stop(scope);
    $flush(root);
    assert.equal(released, 1, 'the task was the input\'s observer; stopping it releases the input');
    $dispose(root);
  });

  it('keeps its pending state across relaunches: readers do not rerun, late completions stay ignored', () => {
    const root = $root('app');
    const id = $signal(root, 1);
    const replies = [];
    const task = $task(root, () => $read(id), (input, complete) => void replies.push({ input, complete }));
    let runs = 0;
    /** @type {any[]} */
    const seen = [];
    $effect(root, () => {
      runs++;
      seen.push($outcome(task).status);
    });
    $flush(root);
    assert.deepEqual(seen, ['pending']);
    $write(id, 2);
    $flush(root);
    $write(id, 3);
    $flush(root);
    assert.equal(replies.length, 3, 'every input launched');
    assert.equal(runs, 1, 'a relaunch while pending shows the same state: no reader reran');
    replies[1].complete({ ok: true, value: 'two' });
    $flush(root);
    assert.equal($pending(task), true, 'a superseded completion is still ignored');
    assert.equal(runs, 1);
    replies[2].complete({ ok: true, value: 'three' });
    $flush(root);
    assert.deepEqual(seen, ['pending', 'ready']);
    $write(id, 4);
    $flush(root);
    assert.deepEqual(seen, ['pending', 'ready', 'pending'], 'ready to pending is seen');
    assert.equal($lastReady(task), 'three');
    $dispose(root);
  });

  it('cancels its work once, whether it stops, its owner stops or its root is disposed', () => {
    const cancels = [0, 0, 0];
    const a = $root('a');
    const stopped = $task(a, () => 1, () => () => void cancels[0]++);
    $flush(a);
    stopped.stop();
    $dispose(a);
    const b = $root('b');
    const scope = $scope(b, null);
    $enter(scope, () => $task(b, () => 1, () => () => void cancels[1]++));
    $flush(b);
    $stop(scope);
    $dispose(b);
    const c = $root('c');
    $task(c, () => 1, () => () => void cancels[2]++);
    $flush(c);
    $dispose(c);
    assert.deepEqual(cancels, [1, 1, 1]);
  });

  it('reaches its effect readers when its state changes, however it was refreshed, in the lane of the change', () => {
    const root = $root('app');
    const id = $signal(root, 1);
    const replies = [];
    const task = $task(root, () => $read(id), (input, complete) => void replies.push(complete));
    /** @type {string[]} */
    const seen = [];
    const lanes = [];
    $effect(root, () => {
      seen.push($outcome(task).status);
      lanes.push($deferred(root));
    });
    const pendingThroughMemo = $memo(root, () => $pending(task));
    $flush(root);
    replies[0]({ ok: true, value: 'one' });
    $flush(root);
    assert.deepEqual(seen, ['pending', 'ready']);
    // A lazy memo reader sees the launch at once, before any flush.
    $write(id, 2);
    assert.equal($read(pendingThroughMemo), true);
    // That read pulled the task; its effect reader still reruns in the flush.
    $flush(root);
    assert.deepEqual(seen, ['pending', 'ready', 'pending']);
    replies[1]({ ok: true, value: 'two' });
    $flush(root);
    // A deferred change reaches the reader in the deferred lane.
    $defer(root, () => $write(id, 3));
    $flush(root);
    assert.deepEqual(seen.slice(-1), ['pending']);
    assert.equal(lanes[lanes.length - 1], true, 'the reader ran as deferred work');
    $dispose(root);
  });

  it('reruns a value reader only when the value moves, a status reader when the status does', () => {
    const root = $root('app');
    const id = $signal(root, 1);
    const wantStatus = $signal(root, false);
    const replies = [];
    const task = $task(root, () => $read(id), (input, complete) => void replies.push(complete));
    let valueRuns = 0;
    let statusRuns = 0;
    let mixedRuns = 0;
    const value = $effect(root, () => {
      valueRuns++;
      $lastReady(task);
    });
    $effect(root, () => {
      statusRuns++;
      $pending(task);
    });
    // Reads the status only while wantStatus is true.
    $effect(root, () => {
      mixedRuns++;
      $lastReady(task);
      if ($read(wantStatus)) $pending(task);
    });
    $flush(root);
    replies[0]({ ok: true, value: 'one' });
    $flush(root);
    assert.deepEqual([valueRuns, statusRuns, mixedRuns], [2, 2, 2], 'a completion moves the value and the status: all rerun');
    $write(id, 2);
    $flush(root);
    assert.deepEqual([valueRuns, statusRuns, mixedRuns], [2, 3, 2], 'ready to pending, same value: only the status reader reruns');
    replies[1]({ ok: true, value: 'two' });
    $flush(root);
    $write(wantStatus, true);
    $flush(root);
    assert.equal(mixedRuns, 4);
    $write(id, 3);
    $flush(root);
    assert.deepEqual([valueRuns, statusRuns, mixedRuns], [3, 5, 5], 'a reader that now reads the status reruns for it');
    $write(wantStatus, false);
    $flush(root);
    replies[2]({ ok: true, value: 'two' });
    $flush(root);
    $write(id, 4);
    $flush(root);
    assert.equal(mixedRuns, 6, 'and stops rerunning once it no longer reads it');
    assert.equal($observers(value), 0, 'an effect has no observers');
    $dispose(root);
  });

  it('delivers a throwing cancel to the task owner boundary and relaunches all the same', () => {
    const root = $root('app');
    const id = $signal(root, 1);
    const caught = [];
    const scope = $scope(root, null);
    $catch(scope, error => void caught.push(/** @type {Error} */ (error).message));
    const launched = [];
    const task = $enter(scope, () =>
      $task(root, () => $read(id), input => {
        launched.push(input);
        return () => {
          throw new Error(`cancel ${input} failed`);
        };
      }),
    );
    $flush(root);
    $write(id, 2);
    $flush(root);
    assert.deepEqual(caught, ['cancel 1 failed']);
    assert.deepEqual(launched, [1, 2], 'the new input launched');
    assert.equal($pending(task), true);
    $dispose(root);
  });

  it("lets whenDone's done dispose the root, late or at once", () => {
    const root = $root('app');
    /** @type {any} */
    let complete = null;
    const task = $task(root, () => 1, (_input, c) => void (complete = c));
    const seen = [];
    $whenDone(root, task, outcome => {
      seen.push(outcome.status);
      $dispose(root);
    });
    complete({ ok: true, value: 5 });
    $flush(root);
    assert.deepEqual(seen, ['ready']);
    assert.equal(root.closed, true);
    const now = $root('now');
    const ready = $signal(now, 1);
    $whenDone(now, () => $read(ready), () => $dispose(now));
    assert.equal(now.closed, true);
  });

  it("stops whenDone's watch when its done throws, the error to $catch", () => {
    const root = $root('app');
    const s = $signal(root, 0);
    /** @type {any} */
    let complete = null;
    const task = $task(root, () => $read(s), (_input, c) => void (complete = c));
    /** @type {string[]} */
    const caught = [];
    $catch(root, e => void caught.push(/** @type {Error} */ (e).message));
    let reads = 0;
    let calls = 0;
    $whenDone(root, () => (reads++, $read(s), $read(task)), () => {
      calls++;
      throw new Error('boom');
    });
    $flush(root);
    complete({ ok: true, value: 0 });
    $flush(root);
    assert.deepEqual(caught, ['boom']);
    const before = reads;
    for (let i = 1; i <= 3; i++) {
      $write(s, i);
      complete({ ok: true, value: i });
      $flush(root);
    }
    assert.equal(calls, 1, 'done once');
    assert.equal(reads - before, 0, 'no longer watching');
    $dispose(root);
  });

  it("ends a $changing mark made in $act's start after it completed", () => {
    const root = $root('app');
    const base = $signal(root, 0);
    const shown = $optimistic(root, base);
    const task = $task(root, () => 1, (_input, c) => c({ ok: true, value: 1 }));
    $flush(root);
    const act = $act(root, shown, (_current, v) => v, (v, complete) => {
      complete({ ok: true, value: v });
      $changing(task);
    });
    act(3);
    $flush(root);
    assert.equal($untrack(root, () => $pending(task)), false, 'the mark ended with the action');
    $dispose(root);
  });

  it("ends $act's change marks even when committing its update throws", () => {
    const root = $root('app');
    const base = $signal(root, 0);
    const shown = $optimistic(root, base);
    const task = $task(root, () => 1, (_input, c) => c({ ok: true, value: 1 }));
    $flush(root);
    /** @type {any} */
    let complete = null;
    // The update refuses the base it is committed over later.
    const act = $act(root, shown, (current, v) => {
      if (current === 5) throw new Error('refused');
      return v;
    }, (_v, c) => {
      complete = c;
      $changing(task);
    });
    act(3);
    $flush(root);
    assert.equal($untrack(root, () => $pending(task)), true, 'marked while the action runs');
    $write(base, 5);
    $flush(root);
    assert.throws(() => complete({ ok: true, value: 3 }), /refused/);
    $flush(root);
    assert.equal($untrack(root, () => $pending(task)), false, 'the mark ended though the commit threw');
    $dispose(root);
  });

  it('raises a commit failure from a start that completes at once', () => {
    const root = $root('app');
    const base = $signal(root, 5);
    const shown = $optimistic(root, base);
    /** @type {any[]} */
    const seen = [];
    const act = $act(
      root,
      shown,
      (current, v) => {
        if (current === 5) throw new Error('refused');
        return v;
      },
      (v, complete) => complete({ ok: true, value: v }),
      result => void seen.push(result),
    );
    assert.throws(() => act(3), /refused/, 'the commit failure reaches the caller, not swallowed');
    assert.equal($untrack(root, () => $read(base)), 5);
    $dispose(root);
  });

  it("tells done of a commit failure, on a start that completes at once or later", () => {
    for (const when of ['at once', 'later']) {
      const root = $root('app');
      const base = $signal(root, 5);
      const shown = $optimistic(root, base);
      const task = $task(root, () => 1, (_input, c) => c({ ok: true, value: 1 }));
      $flush(root);
      /** @type {any[]} */
      const seen = [];
      /** @type {any} */
      let complete = null;
      const act = $act(
        root,
        shown,
        (current, v) => {
          if (current === 5) throw new Error('refused');
          return v;
        },
        (v, c) => {
          $changing(task);
          if (when === 'at once') c({ ok: true, value: v });
          else complete = c;
        },
        result => void seen.push(result),
      );
      if (when === 'at once') assert.throws(() => act(3), /refused/);
      else {
        act(3);
        $flush(root);
        assert.throws(() => complete({ ok: true, value: 3 }), /refused/);
      }
      $flush(root);
      assert.equal(seen.length, 1, `${when}: done observed the outcome`);
      assert.equal(seen[0].ok, false);
      assert.match(seen[0].error.message, /refused/);
      assert.equal($untrack(root, () => $read(shown.pending)), false, `${when}: the layer dropped`);
      assert.equal($untrack(root, () => $pending(task)), false, `${when}: the marks ended`);
      assert.equal($untrack(root, () => $read(base)), 5);
      $dispose(root);
    }
  });

  it("raises the commit failure and hands a failing done's own error to the root's handler", () => {
    const root = $root('app');
    /** @type {string[]} */
    const handled = [];
    $catch(root, e => void handled.push(e.message));
    const base = $signal(root, 5);
    const shown = $optimistic(root, base);
    const act = $act(
      root,
      shown,
      (current, v) => {
        if (current === 5) throw new Error('refused');
        return v;
      },
      (v, c) => c({ ok: true, value: v }),
      () => {
        throw new Error('done failed');
      },
    );
    assert.throws(() => act(3), /refused/, 'the commit failure is raised');
    assert.deepEqual(handled, ['done failed'], "done's own error is not lost");
    $dispose(root);
  });

  it("tells done the action's own outcome under $auto, and raises a failing effect's error after it", () => {
    for (const how of ['commit', 'settle']) {
      const root = $root('app');
      $auto(root, true);
      const base = $signal(root, 0);
      const shown = $optimistic(root, base);
      let armed = false;
      // An unrelated effect that fails once the action ends: the base moved (commit), or the layer went (settle).
      $effect(root, () => {
        const b = $read(base);
        const layered = $read(shown.pending);
        if (armed && (how === 'commit' ? b === 1 : !layered)) throw new Error('effect');
      });
      /** @type {any} */
      let seen = null;
      /** @type {any} */
      let finish = null;
      const act = $act(root, shown, (c, i) => c + i, (_i, complete) => void (finish = complete), result => void (seen = result));
      act(1);
      armed = true;
      const own = new Error('the action failed');
      assert.throws(() => finish(how === 'commit' ? { ok: true, value: 'v' } : { ok: false, error: own }), /effect/, `${how}: the effect's failure surfaces`);
      if (how === 'commit') {
        assert.equal(seen?.ok, true, 'done sees the commit succeed');
        assert.equal($untrack(root, () => $read(base)), 1);
      } else {
        assert.equal(seen?.ok, false);
        assert.equal(seen?.error, own, "done sees the action's own failure");
      }
      $dispose(root);
    }
  });

  it('lets done dispose the root under $auto', () => {
    const root = $root('app');
    $auto(root, true);
    const base = $signal(root, 0);
    const shown = $optimistic(root, base);
    /** @type {any} */
    let finish = null;
    const act = $act(root, shown, (c, i) => c + i, (_i, complete) => void (finish = complete), () => $dispose(root));
    act(1);
    finish({ ok: true, value: 'v' });
    assert.equal(root.closed, true);
  });

  it("cancels $act's work when its stop's settle fails, and raises the failure after", () => {
    for (const how of ['effect', 'custom']) {
      const root = $root('app');
      $auto(root, true);
      const base = $signal(root, 0);
      const real = $optimistic(root, base);
      let armed = false;
      // An unrelated effect that fails once the layer goes, or a settle of the caller's own that throws.
      $effect(root, () => {
        const layered = $read(real.pending);
        if (how === 'effect' && armed && !layered) throw new Error('unrelated effect');
      });
      const shown = how === 'effect' ? real : { ...real, settle: () => { throw new Error('custom settle'); } };
      let cancelled = false;
      const act = $act(root, shown, (c, i) => c + i, () => () => void (cancelled = true));
      const stop = act(1);
      armed = true;
      assert.throws(() => stop(), how === 'effect' ? /unrelated effect/ : /custom settle/);
      assert.equal(cancelled, true, `${how}: the work was cancelled`);
      $dispose(root);
    }
  });
});

// TypeScript consumers get inferred types from the generated declarations.
import { $root, $signal, $read, $write, $memo, $derive, $effect, $watch, $scope, $enter, $task, $store, $at, $keyed, $select, $outcome, $optimistic, $kind } from 'rrx-core';
import type { Readable, Writable } from 'rrx-core';

const root = $root('app');
const count = $signal(root, 1);
const n: number = $read(count);
$write(count, 2);
// @ts-expect-error a number signal refuses a string
$write(count, 'two');
const label = $memo(root, () => `n=${$read(count)}`);
const text: string = $read(label);
// @ts-expect-error a memo is not writable
$write(label, 'x');
const twice = $derive(root, () => $read(count) * 2);
$write(twice, 7);
const readable: Readable<number> = count;
const writable: Writable<number> = twice;
$effect(root, () => {
  $read(readable) + $read(writable);
});
const watching = $watch(root, () => $read(count) > 1, (big: boolean) => {
  void big;
});
watching.stop();
const scope = $scope(root, null);
const inside: string = $enter(scope, () => 'built');
const task = $task(root, () => $read(count), (input: number, complete: (result: { ok: true; value: string } | { ok: false; error: unknown }) => void) => {
  complete({ ok: true, value: String(input) });
});
const loaded: string = $read(task);
const outcome = $outcome(task);
const status: 'pending' | 'ready' | 'failed' = outcome.status;
const store = $store(root, { user: { name: 'ada' } });
const name: unknown = $at(store, ['user', 'name']);
const rows = $keyed(root, () => [{ id: 1 }], item => item.id, (item, index) => $read(item).id + $read(index));
const ids: number[] = $read(rows);
const is = $select(root, () => $read(count));
const selected: boolean = is(1);
const optimistic = $optimistic(root, count);
const ticket: number = optimistic.apply(value => value + 1);
const kind: 'signal' | 'memo' | 'effect' | 'scope' | 'task' | 'store' | null = $kind(count);
void [n, text, inside, loaded, status, name, ids, selected, ticket, kind];


// No timer papers over a design gap (.ai/config/design.md, from the rui root): a
// setTimeout, setInterval, setImmediate, requestIdleCallback or
// requestAnimationFrame (a frame count is a timer too) that waits for
// something to settle is naive design, and it fails here. Every timer call
// says why it is one, in a `timer: <kind>` comment on its line or the comment
// right above:
//   feature   the delay is what the code does (an interval, a debounce, an
//             auto-reset, a demo's simulated latency, an animation's or a
//             measurement's frame loop);
//   deadline  a bound that gives up or fails, never the path to success;
//   fallback  a scheduler's stand-in where the host lacks the real signal
//             (no requestAnimationFrame, no requestIdleCallback, no event
//             for a write to shared memory);
//   poll      re-checks a condition, with a bound, where what it waits on has
//             no event; the loop testing it is on that line or just above;
//   turn      ends the job so the engine does what it does only between jobs
//             (V8 releases what a WeakRef held), never to let work settle.
// A sleep helper (`sleep`, `delay`, `pause`, `tick`, `wait` made of a timer) is the
// pattern itself and fails whatever it says: await the event, the promise or
// the flush that reports the work done, or make its owner report it.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { readdirSync, readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const DIRS = ['src', 'test', 'scripts', 'bench'];
const SKIP = new Set(['node_modules', 'artifact', 'dist', 'build']);
const TIMER = /\b(?:setTimeout|setInterval|setImmediate|requestIdleCallback|requestAnimationFrame)\s*\(/;
const MARK = /\btimer:\s*(\w+)/;
const KINDS = new Set(['feature', 'deadline', 'fallback', 'poll', 'turn']);
const HELPER = /\b(?:const|let|function)\s+(?:sleep|delay|pause|tick|wait)\b/;
const LOOP = /\b(?:for|while)\s*\(/;

/** @param {string} dir @returns {string[]} */
function files(dir) {
  const out = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (SKIP.has(entry.name) || entry.name.startsWith('.')) continue;
    const path = join(dir, entry.name);
    if (entry.isDirectory()) out.push(...files(path));
    else if (/\.(?:m?js|jsx)$/.test(entry.name) && !entry.name.endsWith('.d.ts')) out.push(path);
  }
  return out;
}

/** The marker on a line or in the comment lines right above it. @param {string[]} lines @param {number} at */
function marker(lines, at) {
  const own = MARK.exec(lines[at]);
  if (own) return own[1];
  for (let i = at - 1; i >= 0 && /^\s*(?:\/\/|\*|\/\*)/.test(lines[i]); i--) {
    const found = MARK.exec(lines[i]);
    if (found) return found[1];
  }
  return null;
}

/** Every timer call that breaks the rule, as `file:line: why`. */
export function offences() {
  const out = [];
  for (const dir of DIRS) {
    if (!existsSync(join(ROOT, dir))) continue;
    for (const file of files(join(ROOT, dir))) {
      const lines = readFileSync(file, 'utf8').split('\n');
      const name = file.slice(ROOT.length);
      lines.forEach((line, at) => {
        if (!TIMER.test(line) || /^\s*(?:\/\/|\*)/.test(line)) return;
        const where = `${name}:${at + 1}`;
        if (HELPER.test(line)) return void out.push(`${where}: a sleep helper; await what reports the work done`);
        const kind = marker(lines, at);
        if (!kind) return void out.push(`${where}: no \`timer: <kind>\` saying why a timer and not an event`);
        if (!KINDS.has(kind)) return void out.push(`${where}: \`timer: ${kind}\` is not one of ${[...KINDS].join(', ')}`);
        if (kind === 'poll' && !lines.slice(Math.max(0, at - 3), at + 1).some(l => LOOP.test(l)))
          out.push(`${where}: \`timer: poll\` with no loop testing a condition`);
      });
    }
  }
  return out;
}

describe('timers', () => {
  it('every timer says why it is one, and none sleeps a guessed time', () => {
    assert.deepEqual(offences(), []);
  });
});

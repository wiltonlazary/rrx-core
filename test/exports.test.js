// The public contract, locked: every entry's export names and kinds must
// equal test/exports.lock.json, and the lock names every entry of package.json
// `exports` (a file shipped as is, a font, locked as "file"). A rename, a
// removal or an unplanned addition, an entry's or a name's, fails here.
// Changing the lock is a contract change, made only when the user asks
// (.ai/config/stability.md, from the rui repo root).

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const lock = JSON.parse(readFileSync(new URL('./exports.lock.json', import.meta.url), 'utf8'));
const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));

/** @param {unknown} value */
const kind = value => (typeof value === 'function' ? (/^class\b/.test(Function.prototype.toString.call(value)) ? 'class' : 'function') : typeof value);

describe(`${pkg.name} exports`, () => {
  it('locks every entry the package exports, and no other', () => {
    assert.deepEqual(Object.keys(pkg.exports).sort(), Object.keys(lock).sort());
  });
  for (const [entry, expected] of Object.entries(lock)) {
    const specifier = entry === '.' ? pkg.name : `${pkg.name}/${entry.slice(2)}`;
    if (expected === 'file') {
      it(`${entry} is a file the package ships`, () => {
        assert.ok(existsSync(fileURLToPath(import.meta.resolve(specifier))));
      });
      continue;
    }
    it(`${entry} exports exactly its locked contract`, async () => {
      const module = await import(specifier);
      const actual = Object.fromEntries(Object.keys(module).sort().map(name => [name, kind(module[name])]));
      assert.deepEqual(actual, expected);
    });
  }
});

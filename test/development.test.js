// Development mode: diagnostics for reads nothing tracks.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { $root, $signal, $read, $watch, $development, $dispose, $write, $flush } from '../src/index.js';

describe('rrx-core development mode', () => {
  it('reports an application that reads reactive state, and nothing in production', () => {
    /** @type {string[]} */
    const messages = [];
    $development(true, message => void messages.push(message));
    try {
      const root = $root('app');
      const a = $signal(root, 1);
      const b = $signal(root, 2);
      $watch(root, () => $read(a), value => void (value + $read(b)));
      assert.equal(messages.length, 1);
      assert.match(messages[0], /application .* read reactive state/);
      $watch(root, () => $read(a) + $read(b), () => {});
      assert.equal(messages.length, 1, 'reads in the read function are fine');
      $development(false);
      $write(a, 5);
      $flush(root);
      assert.equal(messages.length, 1, 'off: no checks');
      $dispose(root);
    } finally {
      $development(false, null);
    }
  });
});

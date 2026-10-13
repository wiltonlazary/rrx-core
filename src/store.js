// Stores: persistent (immutable) data with subscriptions
// indexed by explicit paths, without proxies.
//
// A trie of branches mirrors the paths that computations read. A branch's
// cell is an unowned signal created on the first tracked read and discarded
// when its last observer leaves. Put replaces the value at a path by copying
// the path only, then writes the cells of the ancestors and descendants.
// Descendant subtrees whose reference did not change are skipped: every cell
// below them would be written its own value (README §Design choices).

import { $signal, $read, $write, $watch, $memo, $untrack, $equals, $whenUnwatched, $discard, $unowned, hold, unhold, live, $update as change, Graph, pull } from './core.js';
import { isContext } from './context.js';

/** @typedef {string | number} Key */
/** @typedef {readonly Key[]} Path */

class Branch {
  /** @param {Branch | null} parent @param {Key | undefined} key */
  constructor(parent, key) {
    this.parent = parent;
    this.key = key;
    /** @type {import('./core.js').Writable<any> | null} */
    this.cell = null;
    /** @type {Map<Key, Branch> | null} */
    this.children = null;
  }
}

/**
 * Persistent data whose reads are tracked by path.
 * @template T
 */
export class Box {
  /** @param {import('./core.js').Graph} root @param {T} raw */
  constructor(root, raw) {
    /** @type {'store'} */
    this.kind = 'store';
    this.root = root;
    this.raw = raw;
    this.index = new Branch(null, undefined);
  }
}

/**
 * @template T
 * @typedef {Box<T>} Store
 */

/**
 * A store holding `initial`.
 * @template T
 * @param {import('./core.js').Graph} root
 * @param {T} initial
 * @returns {Store<T>}
 */
export function $store(root, initial) {
  live(root);
  return new Box(root, initial);
}

/**
 * The value of `key` in `value`, an own property only: a store's data is its
 * own keys, so an inherited member (`constructor`, `__proto__`, `toString`)
 * reads as missing, never as Object, Object.prototype or a method.
 * @param {any} value @param {Key} key
 */
function own(value, key) {
  return value != null && Object.hasOwn(value, key) ? value[key] : undefined;
}

/** @param {any} value @param {Path} path @param {number} offset */
function valueAt(value, path, offset) {
  for (let i = offset; i < path.length; i++) {
    if (value == null) return null;
    value = own(value, path[i]);
  }
  return value === undefined ? null : value;
}

/**
 * Reads the value at `path`, tracking that path in the running computation.
 * @param {Store<any>} store
 * @param {Path} path
 * @returns {any}
 */
export function $at(store, path) {
  const R = store.root;
  live(R);
  if (R.current === null) return valueAt(store.raw, path, 0);
  let b = store.index;
  for (let i = 0; i < path.length; i++) {
    // Keyed as a property is: `5` and `'5'` are one path.
    const key = String(path[i]);
    if (b.children === null) b.children = new Map();
    let c = b.children.get(key);
    if (c === undefined) {
      c = new Branch(b, key);
      b.children.set(key, c);
    }
    b = c;
  }
  let cell = b.cell;
  if (cell === null) {
    const target = b;
    /** @type {import('./core.js').Writable<any>} */
    const made = $unowned(R, () => $signal(R, valueAt(store.raw, path, 0)));
    // The last observer leaving frees the cell and prunes empty branches.
    $whenUnwatched(made, () => {
      target.cell = null;
      $discard(made);
      let current = target;
      while (current.parent !== null && current.cell === null && (current.children === null || current.children.size === 0)) {
        const parent = current.parent;
        /** @type {Map<Key, Branch>} */ (parent.children).delete(/** @type {Key} */ (current.key));
        current = parent;
      }
    });
    cell = b.cell = made;
  }
  return $read(cell);
}

/**
 * The index `key` names in `array`: a number, or a string that is one
 * written canonically (`'1'`, not `'01'`), within its bounds; else a
 * RangeError.
 * @param {readonly any[]} array @param {Key} key @returns {number}
 */
function indexIn(array, key) {
  const index = typeof key === 'number' ? key : String(Number(key)) === key ? Number(key) : NaN;
  if (!Number.isInteger(index) || index < 0 || index >= array.length) throw new RangeError('RRX: store array index out of bounds');
  return index;
}

/**
 * Sets `key` on a record made here: `__proto__` as an own key (an
 * assignment would make it the prototype), any other key as usual.
 * @param {any} out @param {string} key @param {any} value
 */
function define(out, key, value) {
  if (key === '__proto__') Object.defineProperty(out, key, { value, writable: true, enumerable: true, configurable: true });
  else out[key] = value;
}

/**
 * A shallow copy of a record: a null prototype is kept, any other becomes
 * Object.prototype (a primitive, a Date or a Map written into turns into a
 * plain record, not a broken instance), and every own key is kept.
 * @param {any} source
 */
function copyRecord(source) {
  const out = Object.getPrototypeOf(source) === null ? Object.create(null) : {};
  for (const key of Object.keys(source)) define(out, key, source[key]);
  return out;
}

/** @param {any} value @param {Path} path @param {number} offset @param {any} replacement @returns {any} */
function replaceAt(value, path, offset, replacement) {
  if (offset === path.length) return replacement;
  const key = path[offset];
  const base = value == null ? {} : value;
  const next = replaceAt(own(base, key), path, offset + 1, replacement);
  if (Array.isArray(base)) {
    const copy = base.slice();
    copy[indexIn(base, key)] = next;
    return copy;
  }
  const out = copyRecord(base);
  define(out, String(key), next);
  return out;
}

/** @param {Branch} b @param {any} before @param {any} after */
function descendants(b, before, after) {
  if (Object.is(before, after)) return;
  if (b.cell !== null) $write(b.cell, after === undefined ? null : after);
  if (b.children === null) return;
  for (const [key, child] of b.children) descendants(child, own(before, key), own(after, key));
}

/**
 * Replaces the value at `path`, or applies an updater to it, and notifies the
 * subscribed ancestors and descendants whose value changed.
 * @template V
 * @param {Store<any>} store
 * @param {Path} path
 * @param {V | ((current: any) => V)} replacement
 * @returns {V}
 */
export function $put(store, path, replacement) {
  const R = store.root;
  live(R);
  if (R.pure) throw new Error('RRX: computation cannot write a store');
  const old = valueAt(store.raw, path, 0);
  const next = typeof replacement === 'function' ? $untrack(R, () => /** @type {(current: any) => V} */ (replacement)(old)) : replacement;
  const updated = replaceAt(store.raw, path, 0, next);
  store.raw = updated;
  hold(R);
  try {
    /** @type {Branch | undefined} */
    let b = store.index;
    let value = updated;
    for (let i = 0; i < path.length && b !== undefined; i++) {
      if (b.cell !== null) $write(b.cell, value);
      value = own(value, path[i]);
      b = b.children === null ? undefined : b.children.get(String(path[i]));
    }
    if (b !== undefined) descendants(b, old, next);
  } finally {
    unhold(R);
  }
  return next;
}

/** The store's current data, untracked. @template T @param {Store<T>} store @returns {T} */
export function $raw(store) {
  return store.raw;
}

/** @param {any} value */
const plain = value => value !== null && typeof value === 'object' && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);

/** Structural equality of plain data. @param {any} a @param {any} b @returns {boolean} */
export function deepEqual(a, b) {
  if (Object.is(a, b)) return true;
  if (Array.isArray(a)) {
    if (!Array.isArray(b) || a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (!deepEqual(a[i], b[i])) return false;
    return true;
  }
  if (plain(a) && plain(b)) {
    const keys = Object.keys(a);
    if (keys.length !== Object.keys(b).length) return false;
    for (const k of keys) if (!Object.prototype.hasOwnProperty.call(b, k) || !deepEqual(a[k], b[k])) return false;
    return true;
  }
  return false;
}

/**
 * Explicit identity for persistent rows: an unchanged row keeps its previous
 * value.
 * @template T
 * @param {readonly T[]} current
 * @param {readonly T[]} replacement
 * @param {(item: T) => unknown} key
 * @returns {T[]}
 */
export function $reuse(current, replacement, key) {
  const indexed = new Map();
  for (const item of current) indexed.set(key(item), item);
  const seen = new Set();
  return replacement.map(item => {
    const name = key(item);
    if (seen.has(name)) throw new Error('RRX: duplicate $reuse key');
    seen.add(name);
    const old = indexed.get(name);
    return old !== undefined && deepEqual(old, item) ? old : item;
  });
}

/**
 * Writes something in place. On a store, merges `fields` into the record at
 * `path`; only the changed fields notify. An array at `path` stays an array:
 * `fields` are its indexes (`{ 0: 'q' }`), merged into a copy, and a key
 * that is not an index within its bounds throws, as `$put` refuses one. On a context, writes the nearest
 * provider (a value or a function of the previous one; see core `$update`).
 * @overload
 * @param {Store<any>} store
 * @param {Path} path
 * @param {Record<string, any>} fields
 * @returns {any}
 */
/**
 * @template T
 * @overload
 * @param {import('./core.js').Graph} root
 * @param {import('./context.js').Context<T>} context
 * @param {T | ((previous: T) => T)} value
 * @returns {T}
 */
/**
 * @template T
 * @overload
 * @param {import('./context.js').Context<T>} context
 * @param {T | ((previous: T) => T)} value
 * @returns {T}
 */
/**
 * @param {any} target @param {any} a @param {any} [b]
 * @returns {any}
 */
export function $update(target, a, b) {
  if (target instanceof Graph) return change(target, a, b);
  if (isContext(target)) return change(target, a);
  return $put(target, a, (/** @type {any} */ old) => {
    if (!Array.isArray(old)) {
      const out = old == null ? {} : copyRecord(old);
      for (const key of Object.keys(b)) define(out, key, b[key]);
      return out;
    }
    // An array stays an array: its index keys merge into a copy, as $put writes one.
    const copy = old.slice();
    for (const key of Object.keys(b)) copy[indexIn(old, key)] = b[key];
    return copy;
  });
}

/**
 * Keeps every reference of `a` whose content equals `b`'s, so identity
 * equality notifies exactly what structural equality would.
 * @param {any} a @param {any} b @returns {any}
 */
export function preserve(a, b) {
  if (Object.is(a, b)) return a;
  if (Array.isArray(a) && Array.isArray(b)) {
    let same = a.length === b.length;
    const out = new Array(b.length);
    for (let i = 0; i < b.length; i++) {
      out[i] = i < a.length ? preserve(a[i], b[i]) : b[i];
      if (out[i] !== a[i]) same = false;
    }
    return same ? a : out;
  }
  if (plain(a) && plain(b)) {
    const keys = Object.keys(b);
    let same = keys.length === Object.keys(a).length && Object.getPrototypeOf(a) === Object.getPrototypeOf(b);
    // The source's prototype (a null one too); an own __proto__ key stays a key.
    const out = Object.create(Object.getPrototypeOf(b));
    for (const k of keys) {
      const has = Object.prototype.hasOwnProperty.call(a, k);
      const value = has ? preserve(a[k], b[k]) : b[k];
      define(out, k, value);
      if (!has || value !== a[k]) same = false;
    }
    return same ? a : out;
  }
  return b;
}

/**
 * A store kept equal to `derive()`, a tracked computation (`$mirror`):
 * each change replaces it, and only the paths whose values changed notify.
 * @template T
 * @param {import('./core.js').Graph} root
 * @param {() => T} derive
 * @returns {Store<T>}
 */
export function $mirror(root, derive) {
  const store = $store(root, /** @type {T} */ (/** @type {unknown} */ (null)));
  const { observer } = $watch(root, derive, value => {
    $put(store, [], old => preserve(old, value));
  });
  // Inside a batch or a flush the first value would wait for the flush; it
  // applies now, so no reader ever sees the empty store.
  pull(root, observer);
  return store;
}

/**
 * @typedef {{ ticket: number, path: Path, update: (current: any) => any }} Layer
 */

/** Whether `a` is `b` or one of its ancestors (keys read as properties are). @param {Path} a @param {Path} b */
function prefix(a, b) {
  if (a.length > b.length) return false;
  for (let i = 0; i < a.length; i++) if (String(a[i]) !== String(b[i])) return false;
  return true;
}

/**
 * Optimistic updates over a store (`$overlay`): `read(path)`
 * applies every unsettled layer to the store's value; `apply(path, update)`
 * returns a ticket; `commit(ticket)` writes the layer into the store;
 * `settle(ticket)` drops it.
 * @param {import('./core.js').Graph} root
 * @param {Store<any>} store
 */
export function $overlay(root, store) {
  /** @type {import('./core.js').Writable<Layer[]>} */
  const layers = $equals($signal(root, /** @type {Layer[]} */ ([])), false);
  let serial = 0;
  const current = () => $untrack(root, () => $read(layers));
  /** @param {Path} path */
  const read = path => {
    const base = $at(store, path);
    const active = $read(layers);
    if (active.length === 0) return base;
    // A layer on an ancestor path (or the whole store) computes from fields beside `path`: track its subtree too.
    for (const layer of active) if (prefix(layer.path, path)) $at(store, layer.path);
    let value = $raw(store);
    for (const layer of active) value = replaceAt(value, layer.path, 0, layer.update(valueAt(value, layer.path, 0)));
    return valueAt(value, path, 0);
  };
  /** @param {number} ticket */
  const remove = ticket => {
    const found = current().find(layer => layer.ticket === ticket);
    if (found !== undefined) $write(layers, current().filter(layer => layer.ticket !== ticket));
    return found;
  };
  return {
    read,
    /** @param {Path} path @param {(current: any) => any} update */
    apply: (path, update) => {
      serial++;
      $write(layers, [...current(), { ticket: serial, path, update }]);
      return serial;
    },
    /** @param {number} ticket */
    settle: ticket => {
      remove(ticket);
    },
    /** @param {number} ticket */
    commit: ticket => {
      const found = remove(ticket);
      if (found !== undefined) $put(store, found.path, found.update);
    },
    pending: $memo(root, () => $read(layers).length > 0),
  };
}

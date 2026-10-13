// Selection, props helpers and list mapping.

import {
  $signal, $read, $write, $memo, $watch, $untrack, $equals, $discard, $writable, $observers,
  $stop, $cleanup, $owner, live, ScopeNode, $within, $attach, $deliver, later, $retire, $scope, $after,
} from './core.js';

/**
 * Selection in O(1): the returned reader answers `key === source()`, and a
 * change of selection notifies only the readers of the old and new keys. A
 * direct call of the reader first brings the selection up to date, so it
 * answers fresh between a write and the next flush; a memo or effect that
 * read it earlier follows at the next flush, or after any direct call.
 * @template K
 * @param {import('./core.js').Graph} root
 * @param {() => K} source
 * @returns {(key: K) => boolean}
 */
export function $select(root, source) {
  const R = root;
  let selected = $untrack(R, source);
  /** @type {Map<K, import('./core.js').Writable<boolean>>} */
  const entries = new Map();
  // Entry signals are derived from source, so moving them is allowed even
  // while a computation reads them.
  const sync = () => {
    const next = $untrack(R, source);
    const previous = selected;
    if (Object.is(next, previous)) return;
    selected = next;
    const a = entries.get(previous);
    if (a !== undefined) $write(a, false);
    const b = entries.get(next);
    if (b !== undefined) $write(b, true);
  };
  $watch(R, source, () => sync());
  // The keys live on the root (no hook per key) but end with the owner that
  // made the reader; a sweep due after that finds the table empty.
  const made = $owner(R);
  if (made !== null)
    $cleanup(made, () => {
      for (const entry of entries.values()) $discard(entry);
      entries.clear();
    });
  // Keys nothing observes any more are released in sweeps, each when the
  // table has doubled since the last: amortized O(1) per key, and no hook
  // (closure and record) per key. A sweep waits for the flush's end
  // ($after): run inside the computation making a key, it would free keys
  // that computation already read but does not observe yet (a memo's first
  // run subscribes when it returns).
  let limit = 64;
  let due = false;
  const sweep = () => {
    due = false;
    for (const [key, entry] of entries)
      if ($observers(entry) === 0) {
        entries.delete(key);
        $discard(entry);
      }
    limit = Math.max(64, entries.size * 2);
  };
  return key => {
    live(R);
    sync();
    if (R.current === null) return Object.is(key, selected);
    let signal = entries.get(key);
    if (signal === undefined) {
      if (entries.size >= limit && !due) {
        due = true;
        $after(R, sweep);
      }
      // Owned by the root, made without a closure (one per key).
      const owner = R.owner;
      R.owner = R.home;
      /** @type {import('./core.js').Writable<boolean>} */
      let made;
      try {
        made = $writable($signal(R, Object.is(key, selected)));
      } finally {
        R.owner = owner;
      }
      entries.set(key, made);
      signal = made;
    }
    return $read(signal);
  };
}

/**
 * Properties from several sources, later sources winning: a function of the
 * key. Sources may be records or functions returning records.
 * @param {readonly (Record<string, any> | (() => Record<string, any> | null | undefined) | null | undefined)[]} sources
 * @returns {(key: string) => any}
 */
export function $merge(sources) {
  return key => {
    let result = null;
    for (const source of sources) {
      const properties = typeof source === 'function' ? source() : source;
      if (properties != null && Object.prototype.hasOwnProperty.call(properties, key)) result = properties[key];
    }
    return result;
  };
}

/**
 * Splits a property accessor into one accessor per group of keys, plus one
 * for the remaining keys.
 * @param {(key: string) => any} accessor
 * @param {readonly (readonly string[])[]} groups
 * @returns {((key: string) => any)[]}
 */
export function $split(accessor, groups) {
  const picked = new Set();
  for (const group of groups) for (const key of group) picked.add(key);
  const parts = groups.map(group => {
    const set = new Set(group);
    return /** @param {string} key */ key => (set.has(key) ? accessor(key) : null);
  });
  parts.push(key => (picked.has(key) ? null : accessor(key)));
  return parts;
}

/**
 * @template T, U
 * @typedef {RowScope & { item: import('./core.js').Writable<T>, index: import('./core.js').Writable<number> | null, value: U }} Row
 */

/**
 * A list row is its own scope: the record that owns the row's nodes also
 * carries its item and index signals and its value (one record per row).
 */
class RowScope extends ScopeNode {
  /** @param {import('./core.js').Graph} root @param {import('./core.js').Owner | null} owner @param {any} item @param {number} position */
  constructor(root, owner, item, position) {
    super(root, owner ?? root.home, ++root.nextId, root.ctx);
    /** @type {any} */
    this.item = item;
    /** @type {any} */
    this.index = position;
    /** @type {any} */
    this.value = undefined;
  }
}

/**
 * A row of Rows: its item stays a plain value (`item`) until a tracked read
 * makes its signal (`cell`). The row does not keep the accessor it hands to
 * `build`, so a row that reads its item only while it builds keeps no
 * closure either.
 */
class LazyRow extends RowScope {
  /** @param {import('./core.js').Graph} root @param {import('./core.js').Owner | null} owner @param {any} item @param {number} position */
  constructor(root, owner, item, position) {
    super(root, owner, item, position);
    /** @type {import('./core.js').Writable<any> | null} */
    this.cell = null;
  }
}

/**
 * The item of a lazy row: the plain value untracked; tracked, its signal,
 * made (owned by the row) on the first tracked read.
 * @param {LazyRow} scope
 */
function itemOf(scope) {
  const R = scope.root;
  let cell = scope.cell;
  if (cell === null) {
    if (R.current === null) return scope.item;
    const previous = R.owner;
    R.owner = scope;
    try {
      cell = scope.cell = $writable($signal(R, scope.item));
    } finally {
      R.owner = previous;
    }
  }
  return $read(cell);
}

/** @param {LazyRow} scope @param {(item: () => any, index: any) => any} build */
function fillLazy(scope, build) {
  scope.index = build.length > 1 ? $writable($signal(scope.root, scope.index)) : null;
  scope.value = build(() => itemOf(scope), scope.index);
  return scope;
}

/**
 * @param {import('./core.js').Graph} root @param {import('./core.js').Owner | null} owner @param {any} item @param {number} position
 * @param {(item: () => any, index: any) => any} build
 * @returns {any}
 */
function lazyRow(root, owner, item, position, build) {
  const scope = new LazyRow(root, owner, item, position);
  $attach(scope);
  try {
    return $within(scope, fillLazy, scope, build);
  } catch (error) {
    drop(root, owner, scope, false);
    throw error;
  }
}

/**
 * Makes a row's signals and value inside the row (Within: untracked, owned
 * by the row). `item` and `index` hold the item and position until then.
 * @param {RowScope} scope @param {(item: any, index: any) => any} build
 */
function fill(scope, build) {
  const root = scope.root;
  scope.item = $writable($signal(root, scope.item));
  scope.index = build.length > 1 ? $writable($signal(root, scope.index)) : null;
  scope.value = build(scope.item, scope.index);
  return scope;
}

/**
 * A row owns a scope; its item and position are signals the row may read.
 * A `build` that declares one parameter gets no position signal:
 * it could not read it.
 * @template T, U
 * @param {import('./core.js').Graph} root @param {import('./core.js').Owner | null} owner @param {T} item @param {number} position
 * @param {(item: import('./core.js').Writable<T>, index: import('./core.js').Writable<number>) => U} build
 * @returns {Row<T, U>}
 */
function row(root, owner, item, position, build) {
  const scope = new RowScope(root, owner, item, position);
  $attach(scope);
  try {
    return /** @type {Row<T, U>} */ ($within(scope, fill, scope, build));
  } catch (error) {
    // A row whose build throws stops, with what it registered.
    drop(root, owner, scope, false);
    throw error;
  }
}

/**
 * How a list lets go of removed rows: `retire: true` retires them (inert at
 * once, freed by `$sweep` in the background) instead of stopping them; a number
 * retires the rows of an update that removes at least that many, and stops
 * the rows of smaller updates at once.
 * @typedef {{ retire?: boolean | number }} Removal
 */

/**
 * The fewest rows one update must remove for them to retire: 1 for
 * `retire: true`, Infinity (never) without it.
 * @param {Removal | undefined} removal
 */
function threshold(removal) {
  const retire = removal?.retire;
  if (retire === true) return 1;
  if (typeof retire === 'number' && retire === retire) return retire < 1 ? 1 : retire;
  return Infinity;
}

/**
 * Stops, or retires, a removed row. A failure never fails the list: the row
 * still stops whole (`$stop` runs every cleanup), and the failure goes to the
 * list owner's boundary, or is raised when the flush ends, after the list
 * committed. A retired row's failures go to the boundaries as it is swept.
 * @param {import('./core.js').Graph} root @param {import('./core.js').Owner | null} owner @param {RowScope} entry @param {boolean} retire
 */
function drop(root, owner, entry, retire) {
  if (retire) return $retire(entry);
  try {
    $stop(entry);
  } catch (error) {
    const left = $deliver(root, owner, error);
    if (left !== null) later(root, left);
  }
}

/**
 * True when a list update keeps none of the current rows: every key is new
 * (or there are no items). Keys that stay at either end rule it out without
 * a scan. @param {readonly any[]} items @param {((item: any) => unknown) | null} key @param {Map<unknown, any>} rows
 */
function gone(items, key, rows) {
  const n = items.length;
  if (n === 0) return true;
  if (rows.has(key === null ? items[0] : key(items[0]))) return false;
  if (rows.has(key === null ? items[n - 1] : key(items[n - 1]))) return false;
  for (let i = 1; i < n - 1; i++) if (rows.has(key === null ? items[i] : key(items[i]))) return false;
  return true;
}

/**
 * The scope a list's rows live in, one per list, made with its first row
 * (a child of the list's owner): a retiring list whose update keeps no row
 * retires it whole, in O(1), and makes a new one for the next rows.
 * @param {import('./core.js').Graph} root @param {import('./core.js').Owner | null} owner
 */
function container(root, owner) {
  return $scope(root, owner);
}

/** Rows compare by identity, so an unchanged mapping keeps its observers. @param {readonly any[]} old @param {readonly any[]} next */
function rowsSame(old, next) {
  if (old.length !== next.length) return false;
  for (let i = 0; i < old.length; i++) if (!Object.is(old[i], next[i])) return false;
  return true;
}

/**
 * Maps a list to rows that persist by key: `build(item, index)`
 * runs once per key in its own scope, and a later list moves or updates the
 * row through its item and index signals. Removed keys stop their rows. A
 * null key uses each item as its own key (For by identity). A `build` that
 * declares one parameter gets no index signal. `{ retire: true }` retires
 * removed rows (`$retire`) instead of stopping them; `{ retire: n }` retires
 * the rows of updates that remove at least n.
 * @template T, U
 * @param {import('./core.js').Graph} root
 * @param {() => readonly T[] | null | undefined} read
 * @param {((item: T) => unknown) | null} key
 * @param {(item: import('./core.js').Writable<T>, index: import('./core.js').Writable<number>) => U} build
 * @param {Removal} [removal]
 * @returns {import('./core.js').Readable<U[]>}
 */
export function $keyed(root, read, key, build, removal) {
  return keyed(root, read, key, build, false, threshold(removal));
}

/**
 * Keyed with accessors: `build(item, index)` receives a
 * function returning the row's item. The item's signal is made on the first
 * tracked read, so a row that reads its item only while it builds (untracked,
 * as components do) carries none; a later item under the same key still
 * reaches every tracked reader. `index` is a signal, as in Keyed, when
 * `build` declares it. `{ retire: true | n }` retires removed rows (see $keyed).
 * @template T, U
 * @param {import('./core.js').Graph} root
 * @param {() => readonly T[] | null | undefined} read
 * @param {((item: T) => unknown) | null} key
 * @param {(item: () => T, index: import('./core.js').Writable<number>) => U} build
 * @param {Removal} [removal]
 * @returns {import('./core.js').Readable<U[]>}
 */
export function $rows(root, read, key, build, removal) {
  return keyed(root, read, key, build, true, threshold(removal));
}

/**
 * @template T, U
 * @param {import('./core.js').Graph} root @param {() => readonly T[] | null | undefined} read
 * @param {((item: T) => unknown) | null} key @param {(item: any, index: any) => U} build @param {boolean} lazy @param {number} limit the fewest removed rows that retire
 * @returns {import('./core.js').Readable<U[]>}
 */
function keyed(root, read, key, build, lazy, limit) {
  const owner = $owner(root);
  /** @type {Map<unknown, Row<T, U>>} */
  let rows = new Map();
  /** The rows' container (see container). @type {import('./core.js').Owner | null} */
  let box = null;
  if (owner !== null) $cleanup(owner, () => {
    for (const entry of rows.values()) $stop(entry);
  });
  const memo = $memo(root, () => {
    const items = read() ?? [];
    // An update that keeps none of enough rows retires them all at once.
    if (rows.size >= limit && gone(items, key, rows)) {
      $retire(/** @type {import('./core.js').Owner} */ (box));
      box = null;
      rows = new Map();
    }
    /** @type {Map<unknown, Row<T, U>>} */
    const next = new Map();
    const mapped = new Array(items.length);
    /** Each kept row at its new position (none on a first pass). @type {(Row<T, U> | undefined)[] | null} */
    const moved = rows.size === 0 ? null : new Array(items.length);
    let kept = 0;
    try {
      // The kept rows take their items and places only once the whole pass
      // succeeded (every key unique, every build done), so a failed update
      // leaves them as they were.
      for (let position = 0; position < items.length; position++) {
        const item = items[position];
        const name = key === null ? item : key(item);
        if (next.has(name)) throw new Error('RRX: duplicate $keyed key');
        const old = rows.size === 0 ? undefined : rows.get(name);
        let entry;
        if (old !== undefined) {
          entry = old;
          kept++;
          /** @type {(Row<T, U> | undefined)[]} */ (moved)[position] = old;
        } else {
          if (box === null) box = container(root, owner);
          entry = lazy ? lazyRow(root, box, item, position, build) : row(root, box, item, position, build);
        }
        next.set(name, entry);
        mapped[position] = entry.value;
      }
    } catch (error) {
      // The update fails whole: the rows this pass built stop (a row whose
      // build threw stopped itself), and the list keeps its current rows,
      // unless this update already retired them all (see above).
      for (const [built, entry] of next) if (rows.get(built) !== entry) drop(root, owner, entry, false);
      throw error;
    }
    for (let position = 0; kept > 0 && position < items.length; position++) {
      const old = /** @type {(Row<T, U> | undefined)[]} */ (moved)[position];
      if (old === undefined) continue;
      const item = items[position];
      if (!lazy) $write(old.item, item);
      else {
        const lazyKept = /** @type {LazyRow} */ (/** @type {unknown} */ (old));
        lazyKept.item = item;
        if (lazyKept.cell !== null) $write(lazyKept.cell, item);
      }
      if (old.index !== null) $write(old.index, position);
    }
    if (kept < rows.size) {
      const retire = rows.size - kept >= limit;
      for (const [name, entry] of rows) if (!next.has(name)) drop(root, owner, entry, retire);
    }
    rows = next;
    return mapped;
  });
  return $equals(memo, rowsSame);
}

/**
 * Maps a list by position (`$positions`): `build(item, index)` runs once
 * per position, and the row's item signal follows the value at that position.
 * @template T, U
 * @param {import('./core.js').Graph} root
 * @param {() => readonly T[] | null | undefined} read
 * @param {(item: import('./core.js').Writable<T>, index: import('./core.js').Writable<number>) => U} build
 * @param {Removal} [removal] `{ retire: true | n }` retires removed rows (see $keyed)
 * @returns {import('./core.js').Readable<U[]>}
 */
export function $positions(root, read, build, removal) {
  const limit = threshold(removal);
  const owner = $owner(root);
  /** @type {Row<T, U>[]} */
  let rows = [];
  /** The rows' container (see container). @type {import('./core.js').Owner | null} */
  let box = null;
  if (owner !== null) $cleanup(owner, () => {
    for (const entry of rows) $stop(entry);
  });
  const memo = $memo(root, () => {
    const items = read() ?? [];
    const count = items.length;
    // An update to no items retires enough rows all at once.
    if (count === 0 && rows.length >= limit) {
      $retire(/** @type {import('./core.js').Owner} */ (box));
      box = null;
      rows = [];
    }
    const kept = Math.min(count, rows.length);
    const retire = rows.length - kept >= limit;
    for (let position = count; position < rows.length; position++) drop(root, owner, rows[position], retire);
    const next = rows.slice(0, kept);
    if (kept < count && box === null) box = container(root, owner);
    try {
      for (let position = kept; position < count; position++) next.push(row(root, box, items[position], position, build));
    } catch (error) {
      // The rows this pass built stop (a row whose build threw stopped itself).
      for (let position = kept; position < next.length; position++) drop(root, owner, next[position], false);
      throw error;
    }
    // The kept rows take their items only once every build succeeded, so a
    // failed update leaves them as they were.
    for (let position = 0; position < kept; position++) $write(rows[position].item, items[position]);
    rows = next;
    return rows.map(entry => entry.value);
  });
  return $equals(memo, rowsSame);
}

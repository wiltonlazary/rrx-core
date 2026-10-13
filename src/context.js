// Contexts: typed keys whose values scopes inherit, shared by the record core
// and the column arena. A context is itself the provider component:
// `<Theme value={…}>` evaluates to a `Provided` descriptor that the
// component layer builds under a provider (rrx-core stays host-free). Each root
// keeps its own chain of layers (`root.ctx`), captured by every scope created
// under it, so a context never crosses from one root into another.

/** Root ids: a lowercase letter, then up to 31 lowercase letters, digits or dashes. */
const ID = /^[a-z][a-z0-9-]{0,31}$/;

/** Checks a root id. @param {unknown} id @returns {string} */
export function checkId(id) {
  if (typeof id !== 'string' || !ID.test(id)) throw new Error(`RRX: a root id must match [a-z][a-z0-9-]{0,31}, not "${String(id)}"`);
  return id;
}

/** @type {WeakSet<object>} */
const contexts = new WeakSet();

/**
 * A typed context. Call it (or render it as `<Theme value={…}>`) to provide a
 * value to everything built under it; `$lookup(Theme)` reads the nearest one.
 * @template T
 * @typedef {((props: { value: T | (() => T) | import('./core.js').Readable<T>, children?: any }) => Provided<T>) & { readonly fallback: T }} Context
 */

/**
 * A provider descriptor: what `<Theme value={…}>{children}</Theme>` evaluates
 * to. The component layer builds `children` under `$provide`.
 * @template T
 */
export class Provided {
  /** @param {Context<T>} context @param {any} value @param {any} children */
  constructor(context, value, children) {
    this.context = context;
    this.value = value;
    this.children = children;
  }
}

/**
 * One layer of a root's context chain: a reactive node (a provider's signal
 * or writable derivation), or a fixed value when `node` is null. Only
 * provider layers are `writable` by `$update`; built-in layers, such as an
 * application root's id or its parent's props, are read-only.
 */
export class Layer {
  /** @param {Layer | null} parent @param {Context<any>} key @param {any} node @param {any} value @param {boolean} writable */
  constructor(parent, key, node, value, writable) {
    this.parent = parent;
    this.key = key;
    this.node = node;
    this.value = value;
    this.writable = writable;
  }
}

/**
 * Creates a context whose `$lookup` returns `fallback` where nothing provides it.
 * @template T
 * @param {T} fallback
 * @returns {Context<T>}
 */
export function $context(fallback) {
  /** @type {any} */
  const context = (/** @type {any} */ props) => new Provided(context, props == null ? undefined : props.value, props == null ? undefined : props.children);
  Object.defineProperty(context, 'fallback', { value: fallback, enumerable: true });
  contexts.add(context);
  return context;
}

/** True for a context made by `$context`. @param {unknown} value @returns {value is Context<any>} */
export function isContext(value) {
  return typeof value === 'function' && contexts.has(value);
}

/** The nearest layer of `context` in a chain, or null. @param {Layer | null} chain @param {Context<any>} context */
export function nearest(chain, context) {
  let c = chain;
  while (c != null) {
    if (c.key === context) return c;
    c = c.parent;
  }
  return null;
}

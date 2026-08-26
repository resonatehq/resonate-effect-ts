// The promise id format, in one place.
//
// The server reads a promise id as `<origin>:<lineage>`: the **origin** is
// everything before the first `:`, and the lineage below it is a Dewey path of
// `.`-separated segments:
//
//   foo -> foo:1 -> foo:1.2 -> foo:1.2.1
//
// The origin is load-bearing. `promise.register_callback` and `task.suspend`
// require an awaiter and its awaited promise to share one, and `promise.create`
// rejects an id that does not extend the `resonate:origin` / `resonate:branch` /
// `resonate:parent` it declares. So ids are minted with `join` and read back
// with `origin`, both mirroring the server's own rules.

import { ProtocolError } from "./errors.js";

/** Separates the origin from the lineage below it. */
export const ORIGIN_SEP = ":";

/** Separates Dewey segments below the origin. */
export const LINEAGE_SEP = ".";

/**
 * The first Dewey segment. The protocol specifies "sequential positive integers
 * starting at 1"; note the TypeScript SDK numbers its children from 0, so a
 * lineage minted here reads `foo:1.1` where that SDK writes `foo:1.0`. Ids only
 * have to be consistent within one lineage, and the server validates prefixes
 * rather than segment values, so both are legal — this one follows the spec.
 */
export const FIRST_SEGMENT = 1;

/**
 * Append a Dewey `segment` to `ancestor`.
 *
 * A bare origin joins its *first* segment with `:`; an ancestor that already
 * carries lineage joins deeper segments with `.`, keeping the subtree under one
 * origin:
 *
 *   join("foo", 1)     -> "foo:1"
 *   join("foo:1", 2)   -> "foo:1.2"
 *   join("foo:1.2", 3) -> "foo:1.2.3"
 */
export const join = (ancestor: string, segment: number): string =>
  `${ancestor}${ancestor.includes(ORIGIN_SEP) ? LINEAGE_SEP : ORIGIN_SEP}${segment}`;

/** The lineage origin of `id`: everything before the first `:`. */
export const origin = (id: string): string => {
  const at = id.indexOf(ORIGIN_SEP);
  return at === -1 ? id : id.slice(0, at);
};

/**
 * An id for a detached child: one segment past the *origin* rather than past
 * the spawning context, so recursive detaching stays bounded instead of growing
 * a segment per level.
 */
export const detached = (originId: string, seed: string): string => `${originId}${ORIGIN_SEP}d${hash(seed)}`;

/**
 * Validate a caller-supplied root id, which becomes the origin of its whole
 * lineage. Only `:` is reserved — the origin is everything before an id's first
 * `:`, so an origin holding one could never be split back out. `.` is fine: it
 * only separates segments *below* the origin, so `my.app.workflow` round-trips.
 */
export const validateRoot = (id: string): ProtocolError | undefined => {
  if (!id) return new ProtocolError({ message: "id must not be empty" });
  if (id.includes("\0")) return new ProtocolError({ message: `id '${id}' must not contain null bytes` });
  if (id.includes(ORIGIN_SEP)) {
    return new ProtocolError({
      message: `id '${id}' must not contain '${ORIGIN_SEP}': it separates the origin from the lineage below it`,
    });
  }
  return undefined;
};

// A short, stable, dependency-free hash. Only needs to spread detached siblings
// apart within one origin, not to resist anything.
const hash = (s: string): string => {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return (h >>> 0).toString(36);
};

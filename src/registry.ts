import { Context, Data, Effect, Layer } from "effect";

// =============================================================================
// REGISTRY
// =============================================================================
//
// A durable promise names the function to run as a string, because the process
// that runs it is not the process that created it. The registry is what turns
// that name back into code.

/** A workflow: the context first, then the caller's arguments. */
export type Workflow = (ctx: any, ...args: any[]) => Effect.Effect<any, any, never>;

export interface Registration<F extends Workflow = Workflow> {
  readonly name: string;
  readonly version: number;
  readonly workflow: F;
}

export class NotRegistered extends Data.TaggedError("NotRegistered")<{
  readonly name: string;
  readonly version: number;
}> {}

export interface RegistryService {
  readonly get: (name: string, version?: number) => Registration | undefined;
}

export class Registry extends Context.Tag("@resonatehq/effect/Registry")<Registry, RegistryService>() {}

/** Look a function up, failing rather than returning undefined. */
export const lookup = (name: string, version = 0): Effect.Effect<Registration, NotRegistered, Registry> =>
  Effect.flatMap(Registry, (registry) => {
    const found = registry.get(name, version);
    return found ? Effect.succeed(found) : Effect.fail(new NotRegistered({ name, version }));
  });

/** A registry over a fixed set of workflows. Version 0 means "the latest". */
export const make = (registrations: Iterable<Registration>): RegistryService => {
  const byName = new Map<string, Map<number, Registration>>();
  for (const r of registrations) {
    const versions = byName.get(r.name) ?? new Map<number, Registration>();
    versions.set(r.version, r);
    byName.set(r.name, versions);
  }
  return {
    get: (name, version = 0) => {
      const versions = byName.get(name);
      if (!versions) return undefined;
      if (version > 0) return versions.get(version);
      const latest = Math.max(...versions.keys());
      return versions.get(latest);
    },
  };
};

export const layer = (registrations: Iterable<Registration>): Layer.Layer<Registry> =>
  Layer.succeed(Registry, make(registrations));

/** Name a workflow so it can be called by name across processes. */
export const register = <F extends Workflow>(name: string, workflow: F, version = 1): Registration<F> => ({
  name,
  version,
  workflow,
});

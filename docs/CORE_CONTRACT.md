# Core ownership and compatibility

FastK is a local, single-writer, fixed-record storage engine for normalized data.
It is a technical/research MIT project, with no enterprise operation or service
level commitment. This narrows features, not the requirement to preserve data.

## Module responsibilities

| Layer | Owns | Does not own |
|---|---|---|
| Records / codecs | Signed integer representation, timestamp ordering, schemas | Source cleaning or indicator arithmetic |
| Chunk IO | Fixed records, sparse indexes, atomic file publication | Dataset identity or application routing |
| Manifest / filesystem | Published file inventory, checksum, sync and rename | Business state |
| Scalar index | Candidate selection and checked index loading | Indicator meaning |
| Store | Registration, append/replace/merge orchestration and ownership | SQL, strategies or market clients |
| Recovery | Storage inspection and explicit repair under ownership | Exchange gap repair |
| Registry / snapshot | Dataset resolution, immutable publication and verification | Which dataset a researcher should choose |
| Bridge / Python client | Integer-preserving transport and process lifecycle | A second storage implementation |

Write ownership and scalar append are separated from the store query paths;
the pre-existing store tests live in their own module. This is a bounded split,
not a new backend abstraction. More decomposition should follow actual changes.

## Public API tiers

| Tier | Surface | Policy |
|---|---|---|
| Recommended | `FastKStore`, records/keys, range/latest/query/inventory, explicit read-only handles, JSON contract v1, Python `Client` | Preserve current record layout and existing success payloads |
| Additive RC | Typed range append, `set_read_threads`, `fastk_native.Reader` / `Writer`, snapshots, publication, client packaging | Documented and tested; no 1.0 stability promise |
| Compatibility | Existing `put_*`, indicator wrappers, `BacktestStoreView`, `ScalarPredicate` | Keep available; new adapters use generic scalar APIs and `ScalarPredicateExpr` |
| Experimental | Exported chunk/index internals, high-frequency extensions, benchmark helpers | Retained to avoid breaking callers; not the recommended adapter dependency |

No existing Rust symbol is removed or newly deprecated in the compiler in this
release. New examples avoid internal types. A future removal requires a separate
migration and release decision. No generic engine trait or plugin architecture
is introduced.

## Ownership, cache and readers

- `.fastk.write.lock` is persistent. OS handle ownership determines the lock;
  old PID text is ignored, and process termination releases ownership. Never
  delete this file to unlock a running process.
- Mutations, startup recovery and manifest rebuilds require exclusive ownership.
  Metadata is reloaded after ownership is acquired, including alternating store
  objects. A bridge write holds one lease over preflight and all month commits.
- `open_read_only` takes shared ownership until dropped; multiple readers are
  allowed, and a writer fails fast while they exist. It never creates a store,
  migrates JSON manifests, or clears pending recovery. Long-lived readers can
  block updates: no MVCC/live-refresh promise is made.
  Initialize a store before retaining a long-lived reader; missing/empty-root
  inventory probes are stateless and do not reserve a future store location.
- Old `open` handles remain suitable for sequential reads and writes. Their
  cached sessions are not a live-refresh API. Version 0.2 uses positional IO
  with reader-local offsets: explicit read-only handles may now be shared by
  parallel callers. The [pre-v0.2 audit](RUST_CORE_REVIEW.md) preserves the
  cursor bug evidence fixed in this release.
- Pre-persistent-lock stores need explicit writable `init` once. Legacy JSON
  manifests migrate under writable initialization. Inspecting them must not
  silently rewrite them.
- Supported ownership is local-filesystem access by cooperating FastK callers.
  Do not share one mutable store between Windows and Linux or rely on network
  filesystem locking semantics that have not been tested.

## Write semantics

`put_kline_chunk` appends non-overlapping ordered records. `put_scalar_chunk`
retains its original whole-month replacement semantics. New
`append_scalar_chunk` preserves earlier rows and rejects overlapping timestamps.
The generic and indicator bridge writes use append semantics. Direct `put_*`
batches stay within one month. Typed range append validates ordered records,
splits by UTC month, and holds one stack-local OS guard across all commits.
The JSON bridge retains input sorting and duplicate rejection; the native module
requires sorted input. Mismatched timeframe/scales are rejected.

Scalar append currently rewrites the month and rebuilds indexes. Prefer bulk
chronological batches; repeated tiny same-month feature writes can be costly.
The extension benchmark separately measures a later month, not cheap same-month
updates. Snapshot sealing finalizes durable active deltas for complete replay.

Commit order is new chunk, complete atomic sidecars, manifest, obsolete-file
cleanup, recovery-marker removal. File contents are synced before publication;
POSIX directory entries are synced, while Windows directory sync remains
best-effort. A synced file rename is not a multi-file transaction by itself.

One month is the commit unit. Cross-month failures report `completed_months` in
the error message; the failed month can still require recovery. A timeout or
process exit is not evidence of rollback. Inspect/recover before retrying; the
client never automatically retries writes.

## Recovery and failure evidence

The fault-injection feature is test-only and off in normal release builds.
Tests cover process kill releasing ownership; competing recovery; alternating
writers; scalar exits after chunk, sidecars and manifest; and injected flush,
sync and rename failures. Reopen results must contain whole versions, never
mixed scalar values. Existing orphan, manifest and sidecar tests remain.

Recovery can adopt complete uncommitted files, so a failed write can become
visible after recovery. Ambiguous or superseded orphan artifacts may be retained
for inspection; recovery is not automatic destructive garbage collection.
Inspect `list-orphans`, `scrub` and overlap explanations before sealing. Corrupt
indexes produce errors; indexes absent from metadata can use the existing scan
fallback. Cached indexes are verified on load, not on every cached query.

Process-kill and injected IO tests do not prove physical power-loss durability.
No new power-loss guarantee or decade-long compatibility promise is claimed.
Rebuild preserves existing valid semantic metadata. Existing chunk headers do
not contain kline scales or every caller-selected partition policy: if their
manifest is missing/corrupt, recovery refuses to invent that information.
Restore the original manifest backup first. Checksums detect damage; they are
not backups. Scalar metadata can still be reconstructed from its fixed unit
layout, namespace and timeframe.

## Dataset publication

A committed manifest is authoritative. Recovery does not replace its chunk with
an older or equal-generation orphan, even when that orphan has more rows. This
prevents an acknowledged smaller replacement from resurrecting obsolete data.
Unadopted artifacts remain available for inspection; this is not automatic GC.

An explicit `DatasetRef` is necessary for reproducibility but does not freeze a
draft root. Use `seal_snapshot` and pin its SHA-256 digest, or use
`DatasetRegistry::publish_dataset`. Publication adds an immutable binding in the
additive `dataset_snapshot` catalog table; old arbitrary status strings remain
compatible and are not treated as proof of sealing.

Snapshots cover the complete sorted `series/` file inventory, including schema,
scales, manifests and indexes. They are physical snapshots: compaction or index
rebuild requires a new version. Root relocation preserves identity; any file
change or inventory change fails verification. Pinning the digest also detects
replacement of the snapshot manifest. Keep the control catalog outside the data
root. A failed catalog publication can leave a sealed root; publication is
retryable, but already-published metadata cannot be rebound.

All store mutation and repair APIs reject sealed roots. Explicit
`verify_snapshot` reads all files; ordinary range queries do not rehash the
dataset. Registry resolution of published bindings verifies the pinned digest.
An OS user manually modifying files can bypass API immutability; verification
detects such changes and is not a filesystem access-control mechanism.

## Format continuity

Chunk layouts and manifest magic remain unchanged (`FKMETA02` for binary
manifests). Existing roundtrip, legacy-JSON migration, unknown-version and
checksum tests remain release gates. A future format change must add explicit
reader/migration tests and document compatibility; do not silently reinterpret
old bytes or change numeric scales.

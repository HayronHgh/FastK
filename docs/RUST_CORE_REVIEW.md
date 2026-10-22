# Rust core review and optimization plan

Review date: 2026-10-21. Scope: native Rust reads/writes, chunk algorithms,
ownership, state transitions and internal boundaries. This is an audit and a
proposed implementation sequence; no engine optimization or fix ships with it.
The Python JSON studies measure a different boundary and do not establish the
native engine's limits.

There is useful work left in the existing implementation. The first priority is
a confirmed native concurrent-read correctness defect. Next come two confirmed
state-management problems, followed by unnecessary lookup, IO and allocation
work. None requires a public backend trait, new record types or a plugin layer.

## Confirmed findings

| Priority | Finding | Evidence | Smallest useful change |
|---|---|---|---|
| P0 | Cached file clones share a cursor | Two shared-store runs return 15,886 and 15,859 wrong results out of 16,000; independent stores return none | Explicit-offset reads and a private `Arc<File>` cache |
| P1 | Invalid input leaves recovery pending | Out-of-order input changes a previously readable root into one requiring writable recovery, without another chunk | Validate before mutation; arm the marker just before disk changes |
| P1 | Compaction uses lifetime generation as current debt | A freshly merged single chunk still requests another merge | Derive fragmentation from the current month inventory |
| P2 | Sparse lookup adds a linear search; short ranges reread boundaries | Binary search is followed by `.position()`; one-row range reads 12,336 bytes | Keep the anchor index and reuse a shared boundary window |
| P2 | Partial ranges reserve entire overlapping chunks | 2,049 slots / 2,039 rows reserve 44,428 records | Size full chunks exactly and bound partial-chunk reservation |
| P2 | Write validation repeats full scans and per-row month formatting | Store, writer and index builder repeat order checks | Retain safety boundaries, remove redundant inner work |

The race and state failures below were exercised through existing public Rust
APIs. The sparse-index complexity and repeated validation are source findings;
bytes read and returned-vector capacities were measured. No before/after
throughput improvement is claimed because no production fix was applied.

### 1. Shared cached file cursors: correctness before parallel speed

`ChunkFileCache::open` in `src/store_core/chunk/cache.rs` returns
`File::try_clone()` on both hits and misses. Readers then perform separate
`seek` and `read_exact` calls outside the cache mutex. Cloned files share the
underlying cursor, so another thread can change the offset between these calls.
The shared OS read lock excludes writers; it cannot prevent this reader/reader
race. This is consistent with the documented
[`File::try_clone` semantics](https://doc.rust-lang.org/std/fs/struct.File.html#method.try_clone).

The external reproduction uses one `Arc<FastKStore>` opened read-only, four
different same-month ranges, eight threads and 2,000 calls per thread. Before
concurrency, 400 serial reads match. All fields of the four references also
match the independently generated Arrow input. Two concurrent runs produced:

| Mode | Calls | Wrong successful results | Reported errors |
|---|---:|---:|---:|
| Shared store, run 1 | 16,000 | 15,886 | 0 |
| Shared store, run 2 | 16,000 | 15,859 | 0 |
| Independently opened store per thread | 16,000 | 0 | 0 |

These are observations under deliberate contention on Windows, not estimated
failure probabilities for other workloads. The shared-cursor source pattern
also warrants a Unix regression, which was not run in this audit. Existing
serial CLI/session execution is outside this reproduction. It does not
invalidate the earlier serial benchmark results.

Proposed change:

- Cache `Arc<File>` and perform shared data reads with explicit offsets. Use
  one private concrete helper in the existing filesystem/chunk IO layer; keep
  the public API and file format unchanged.
- Implement exact reads, including partial reads, EOF and offset arithmetic.
  Unix provides
  [`read_exact_at`](https://doc.rust-lang.org/std/os/unix/fs/trait.FileExt.html#method.read_exact_at).
  Windows
  [`seek_read`](https://doc.rust-lang.org/std/os/windows/fs/trait.FileExt.html#tymethod.seek_read)
  accepts an explicit offset but can return a partial read and updates the
  file cursor. Therefore **every shared read** must use explicit offsets;
  mixing positional reads with the old seek/read path is insufficient.
- Include cold header/index loading in the audit. It must use independent
  handles or the same explicit-offset discipline.
- Hold the cache mutex for lookup/insert/eviction only. Open a missed file
  outside it and recheck on insertion. Keep OS ownership and guard lifetimes.

Do not serialize all queries behind a new global mutex to hide the race.
Until fixed, use an independently opened read-only store in each concurrent
consumer. Do not delete the lock file to work around ownership.

### 2. Validation must not create a failed-write state

`FastKStore::put_kline_chunk` arms `RecoveryMarkerGuard` before validating
strict timestamp order. A two-record descending batch returns `InvalidInput`,
creates no new chunk, but leaves a pending marker. After dropping the store,
`open_read_only` rejects the root with `store requires explicit writable
recovery`. The isolated reproduction starts with one valid chunk and finishes
with that same single chunk.

Move pure input checks, including order and month membership, before acquiring
write ownership. Under exclusive ownership, recover if needed, reload metadata,
and check metadata-dependent constraints such as overlap and schema. Arm the
recovery marker immediately before the first actual disk mutation. Inspect
other mutation entrypoints for the same ordering.

This shortens lock-held validation work and avoids unnecessary marker syncs.
It must preserve recovery for uncertain partial writes: do not simply remove
the marker on every error. If an operation registers metadata before writing a
chunk, that registration is already a mutation and needs the marker.

### 3. Compaction generation is not outstanding work

`CompactionPolicy::evaluate_month` calculates `append_count` as the maximum
generation minus one. A merge also increments generation, so it does not reset
this calculation. The reproduction appends five one-row chunks, then merges:

| Point | Current chunks | Reported append count | Requests merge |
|---|---:|---:|---|
| Before merge | 5 | 4 | yes |
| Immediately after merge | 1 | 5 | yes |
| After one append | 2 | 6 | yes |
| After another append with auto-merge enabled | 1 | 8 | yes |

Automatic merging defaults to off. The incorrect advice exists regardless;
when enabled, subsequent appends can repeatedly rewrite the month.

Keep generation as file/version identity. Define merge pressure using the
currently published month fragments, excluding a compacted baseline. For the
existing ordered, non-overlapping append layout, derive this from the current
manifest rather than adding persistent counters. A single chunk has no merge
work. Validate legacy/recovered inventories before depending on a baseline
selection rule; document the meaning of the existing advice fields.

`merge_output_state` retains `Active` if any input is active. Thus counting all
active bytes as unmerged debt can also count the newly merged baseline. Fix
that accounting without changing `Active`/`Sealed` replay visibility. Making
all merged output sealed is a separate behavioral change, not a free speedup.
Keep the existing `Merging` checkpoint and crash publication ordering unless
a separate recovery proof justifies changing them.

### 4. Sparse indexes should not search the same data again

`anchor_for_ts` does a binary search, but `lookup_window` in the kline, scalar
and fixed readers then uses `.iter().position()` to rediscover its index. For
M anchors this adds O(M) work to an O(log M) lookup. Retain the index from the
binary search, using a private helper in the existing sparse-index module.
There is no need for a generic reader strategy or another public interface.

The kline short-range path independently reads an anchor window for its lower
and upper bounds, then rereads the selected span. On the prepared fixture a
one-row range reads 128 records twice, then its one output record:
`128 * 48 * 2 + 48 = 12,336` bytes. These are application read bytes from the
existing metrics, not physical disk traffic; cached header/index reads are
excluded from this warmed measurement.

First optimize the simple case where both bounds fall in one sparse window:
read once, find both bounds in memory, and append the selected slice into the
existing output vector. Cross-window boundary reuse can follow if measurement
justifies its extra code. Preserve gaps, inclusive endpoints, empty results
and the existing full-chunk path.

### 5. Capacity should follow the requested range

`get_kline_range_prepared` reserves a time-slot estimate for short requests,
then switches to the sum of whole overlapping chunk counts for longer ones.
Public `Vec` capacities from the same prepared read-only store:

| Requested slots | Returned rows | Capacity | Application bytes read |
|---:|---:|---:|---:|
| 1 | 1 | 1 | 12,336 |
| 127 | 126 | 127 | 18,336 |
| 2,048 | 2,038 | 2,048 | 110,112 |
| 2,049 | 2,039 | 44,428 | 98,112 |
| 4,096 | 4,076 | 44,428 | 195,888 |

At 2,049 slots, record capacity represents 2,132,544 bytes for 97,872 bytes of
returned records, about 21.8 times as much. This is vector capacity, not an RSS
measurement. The read strategy also switches near this threshold, so fewer
bytes here do not establish lower latency.

Reserve exact counts for fully covered chunks. Bound initial reservation for
partial chunks, then use actual span bounds and existing vector growth. Do not
assume all timestamp slots exist or change the threshold merely to conceal the
cliff. Consider a single output vector for latest-N and scalar row-range reads
after this change: their temporary per-part vectors add avoidable allocations
and copies, but require separate measurement.

### 6. Remove repeated write work at real boundaries

Kline order is checked in the store, chunk writer and sparse-index builder.
The writer also formats a UTC month string for every record. With strict
ordering already established, first/last month equality is sufficient to prove
that all records belong to that month.

Start with endpoint month checks and redundant inner validation. Retain
validation at genuinely callable safety boundaries. Do not introduce public
unchecked writers or `Validated<T>` wrappers to save a scan. The store's early
validation is also necessary to reject input before leaving recovery pending.
If retaining both store/writer checks is the cleanest safe implementation,
keep them and remove the third scan only.

Chunk writers currently assemble a whole encoded buffer, traverse it for the
checksum, then write it. Bounded encoding with the same incremental checksum
could reduce peak copies later. It needs a measured win; changing the checksum
algorithm, on-disk layout or sync guarantees is outside this plan.

## State, lock and encapsulation boundaries

Keep four concrete responsibilities: store orchestration; chunk IO/codecs;
index algorithms; filesystem publication and OS ownership. The CLI adapts the
same store API. Refactor only the functions touched by the fixes above.

- Preserve the persistent OS lock implementation. Exclusive mutation/recovery
  and shared read-only ownership already address the original stale-lock and
  competing-recovery concerns; an old review of lock-file deletion does not
  describe the current code.
- Keep pure validation outside ownership, fresh metadata and conflict checks
  inside ownership, and uncertainty markers around disk changes. Avoid adding
  a new lock hierarchy or converting the engine to multiple writers.
- Separate `generation`, visibility state and compaction pressure. They serve
  different purposes even when currently represented by the same manifest.
- Remove unused private `query_full_scan` / `query_short_range` wrappers marked
  `allow(dead_code)` if still unused when implementing the read changes.
  Retain existing exported symbols for compatibility; add no speculative ones.
- Revisit duplicate global/per-store manifest caches only after defining one
  clear owner and proving freshness across alternating writable handles.
  Removing invalidations without a replacement could introduce stale reads.
- Gate detailed clock reads by the existing `MetricsLevel`; currently some
  `Instant::now()` calls execute even when detailed recording is disabled.
  Measure with identical metrics settings before reporting a speedup.

Two lower-priority ownership checks remain source observations, not reproduced
failures: the private batch guard is manually taken after a closure (review
unwinding if the library is used with `catch_unwind`), and an owned replay cursor
can outlive its store (review its read lease). Resolve only actual supported
lifecycles; do not add a lifetime framework for hypothetical future use.

`append_scalar_chunk` currently rereads and rewrites the entire existing month
and rebuilds both sidecars. Repeated small appends therefore amplify work with
the month size. Prefer chronological bulk batches now. A later append-only
scalar path could reuse the existing immutable delta approach, but requires
sidecar/recovery analysis and must retain `put_scalar_chunk` replacement
semantics. It should not be bundled with the small read-path fixes.

## Implementation order and acceptance

1. Fix explicit-offset reads and cache ownership. Run concurrent public-API
   range/point/latest/scalar/fixed checks on Windows and Linux, with cold and
   warm layouts. Include independent-handle and serial controls, EOF/corrupt
   inputs, and existing ownership/recovery tests. No engine test hooks.
2. Move validation/marker boundaries and correct compaction accounting. Rejected
   pure input must leave a clean root; injected partial writes must still need
   recovery. A merge must reset fragmentation; append/merge/seal/reopen must
   preserve records and replay visibility. Reuse the current fault suite.
3. Remove linear anchor rediscovery, reuse same-window reads, and bound range
   capacity. Compare identical native queries and canonical row hashes,
   including sparse gaps and the 2,048/2,049 boundary. Measure query latency,
   bytes read and allocation/memory separately. No assumed percentage gain.
4. Simplify write scans and touched private wrappers. Broader cache/streaming
   writes or scalar deltas wait for evidence from a real workload.

Native benchmarks should open the engine outside timed warm queries, report
reopen separately, repeat/shuffle the same query set and state cache conditions.
Use a consumed result/hash so the workload is comparable. CLI startup/output
and Python decoding remain separate measurements. Preserve public methods,
record order/values, file format, CLI pretty JSON, session timeout, ownership
and failure-without-automatic-replay behavior. The existing 1,430 Python short
queries remain an integration regression check, not the native speed metric.

## Evidence and reproduction

Raw verified outputs, independent canonical reference checks, the external
reproducer source/lockfile and source hashes are in
[`evidence/2026-10-21/rust-core-review`](evidence/2026-10-21/rust-core-review/README.md).
The reproducer uses only the existing public library API. Its files live
outside production modules, with no feature flags or injected engine behavior.
The production source and existing release binary were unchanged during this
audit. The full engine test suite was not rerun for documentation-only changes;
the external crate was compiled against the current source and its scenarios
were executed. Windows correctness observations do not substitute for a Linux
run or physical power-loss testing.

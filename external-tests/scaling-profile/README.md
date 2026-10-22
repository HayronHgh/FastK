# Rust scaling diagnostics

This separate, unpublished crate uses public FastK APIs. It adds no product hooks.
It diagnoses scheduling, shared-state pressure and effective copy throughput; it is
not a database ranking or the release acceptance benchmark.

Build on Linux with `RUSTFLAGS='-C force-frame-pointers=yes' cargo build --release
--locked --manifest-path external-tests/scaling-profile/Cargo.toml`. Use the pinned
toolchain and keep compilation separate from measurements. The release profile
retains debug symbols. Frame pointers and perf may affect absolute times.

`run.py --help` lists binary, immutable store, short-query, bulk-query and output
paths. Run with perf access. `run.py` randomizes 78 counter processes (three repeats
of 26 settings), runs 26 no-perf controls, and records seven cycle profiles. Perf
control FIFOs enable counters only after setup and warmup. Record files are written
on native `/tmp` then copied out: recording directly to a Windows bind mount failed
with `Bad address` in the recorded environment. `--records-only` resumes the profile
stage after checking previous counter/direct outputs; it does not rerun them.

Executable syntax: `fastk-scaling-profile MODE STORE QUERY_JSON THREADS GROUPS CYCLES`.

- `engine`: original bulk Kline query, 437115 rows / 20981520 bytes; actual engine
  chooses work groups. The GROUPS argument is only a label for this mode.
- `outer`: 1430 original queries per cycle on one shared `Arc<FastKStore>`.
- `private`: same queries, one store per worker. All handles are warmed with the
  full query list, then workers warm their assigned queries before counting.
- `copy`, `pread`: same byte volume, initialized reusable destination, no output
  allocation in the measured loop. Pread uses the unchanged product positional IO
  implementation against one diagnostic flat file, not the production chunk layout.
- `schedule`: empty parallel group dispatch, a negative control.

Engine warm/final results are fully checked against canonical SHA-256 outside the
counter window. Hot loops check lengths, not every output byte. The phase includes
result destruction; individual engine read samples exclude destruction. Do not mix
phase time with read time, or one bulk call with one 1430-query outer pass. The
controls have different allocation/IO layouts; their times cannot be subtracted
to assign exact additive fractions to engine costs.

`cache_counts STORE SHORT_JSON THREADS shared|private CYCLES` uses Basic metrics
only to count hits/misses and stable Linux FD counts; do not time those runs. It
warms only each worker's assigned queries, unlike the profile's full-handle warmup.
`bandwidth BYTES THREADS GROUPS` performs 32 warmups and 128 copies into a reused
buffer, validates final bytes, and reports effective memcpy throughput, not a
physical DRAM bandwidth counter.

`controls.py --help` runs paired randomized 64/512 capacity diagnostics, the copy
size curve, and larger-cache counters. Build the 512 binary in a separate frozen
source copy with only the private cache constant changed. Prove that exact diff;
do not modify the product or add a public cache setting for this experiment.
Serial bandwidth controls use the same binary with THREADS=GROUPS=1.

The `shared_lifetime` integration test exercises first-touch sharing, repeated
queries and last-owner lease release on a temporary 240-file fixture. Set
`FASTK_LIFETIME_WAVES` to extend it. Linux counts root-specific FDs; Windows uses
`GetProcessHandleCount` and checks a stable baseline+65 after workers join and
return to baseline after the last owner drops. These are quiescent counts, not
in-flight peaks. The Windows API dependency is test-only.

Use `summarize.py --profile DIR --controls DIR --serial DIR --output FILE` to
recompute process medians. Preserve raw JSON, perf counters, manifests and hashes.
Perf cycle percentages are CPU sample shares, not query wall-time percentages.
If kernel names are restricted, preserve original reports; a nearest-text-symbol
mapping requires a nonzero `/proc/kallsyms` from the exact same kernel boot. Label
that mapping explicitly instead of presenting it as perf's resolved symbols.

The captured environment is WSL2 on Ryzen 9 9900X. It cannot establish native Linux
or Windows MSVC behavior, physical NUMA/CCD mapping, or a DRAM controller ceiling.
See `docs/SCALING_LIMITS.md` for findings and evidence locations.

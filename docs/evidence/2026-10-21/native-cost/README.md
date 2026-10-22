# Native bulk / Linux concurrency attribution

Measurement and proposal only. Production source and released wheels were not
changed. Read ../../../NATIVE_COST_ANALYSIS.md for Chinese interpretation and
the unimplemented optimization plan. The diagnostic source is opt-in, isolated
from production APIs, and included in diagnostic-source.zip. Start with the
existing native-engine/after-source.zip, then overlay the diagnostic archive to
rebuild against the exact measured core; source SHA lists identify both.

windows/ and linux/ contain the final primary runs. Bulk is 32 warmups and
30 measured repetitions per case, three process rounds. Twelve GIL-only jobs
are negative controls, not read validations. warm-controls-linux/ separates
same-handle replay and external USS sampling. fd-controls-linux/ pairs normal
descriptor tables with pre-expansion only in disposable diagnostic processes.
Every actual read result was validated against frozen canonical data.

Record-to-word buffers differ; Vec ownership handed to NumPy retains its pointer.
The optional FD control opens/closes dummy descriptors before creating workers;
it is not an engine change or recommended dummy-file workaround. Kernel fdtable
RCU behavior is inferred from the observed openat boundaries, causal controls and
Linux source, not directly measured with a kernel probe. All strace throughput
is excluded: tracing itself substantially changes timings.

Source versions: primary-source-freeze.json describes the seven files used for
the final instrumented module/primary jobs. Additional standalone controls were
added afterward without changing those files. controls/core_control-original.rs
is the executable source for the warm/monitor and syscall controls. The current
source adds only the FD preparation control used by fd-controls-linux. Binary
hashes are in builds.json. diagnostics/ preserves the initial wrong Rust group
timer source, summary and sample raw data. Those group times were invalid and
were not used in the final conclusions; the full primary experiment was replayed.

analysis.json contains derived distributions; raw JSON is gzip compressed. Query
and manifest files are included without datasets, stores, compilers or caches.
The original 52 manifests / 4,719 entries and 143 prior source files were checked
unchanged. verification.json records commands/check boundaries and skipped work.
Rerunning full product suites was not needed because no product code changed;
the prior v0.2 correctness evidence is retained, not claimed as newly executed.

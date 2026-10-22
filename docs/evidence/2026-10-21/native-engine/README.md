# v0.2 local evidence

All seven final runs have verified results. Rust gates use the same public-API
probe and a 5% median regression limit; they are separate from Python transport.
Matrices use the final guarded native wheels. Standard/real comparisons use the
same packed NumPy output for all five engines/formats. Delivery compares the
preserved compact CLI with the native module, outside the primary ranking.

Raw worker JSON is gzip compressed without datasets. Source archives include the
110-file immutable before state, final runtime/harness/build sources and measured
frozen harness. Wheel hashes and extension-member hashes identify measured builds.
The legacy CLI SHA is recorded separately in legacy-bridge.json. The prior
audit and 2026-10-20 evidence remain unchanged.

Earlier allocator-transition gate summaries are retained under diagnostics/;
the rust-core-review evidence documents the original cursor race. They are not
substituted for these final measurements. OS cache is uncontrolled; process
restart does not establish a cold cache. See ../../../NATIVE_VALIDATION.md
for interpretation, limitations and verification commands.

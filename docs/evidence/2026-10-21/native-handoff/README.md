# Native allocation handoff evidence

See [report](../../../NATIVE_HANDOFF_RESULTS.md). All initial warning data remains.
Per OS: initial24 + fixed serial36 =60processes,38640fully checked timed outputs.
Serial summaries combine12pairs; parallel summaries retain3pairs. Combined JSON
keeps every per-process median/p95/p99, cohort, sampled peak and paired bootstrap.
Raw large JSON is gzip compressed; matching configuration/log/PID remains beside it.
NumPy delivery and materialization contracts are identical on both sides.
*.whl are diagnostic CP311/312 before/after packages; no old release is replaced.
Installed after-extension SHA is checked against all four exported after wheels.
Before63-source snapshot precedes all product edits. After-source contains current
code and documentation; README text was clarified after the tested wheel build,
so wheel package-description text can predate that clarification. Rust/Python
implementation inputs are the tested code. Runtime registry versions retained;
root addsbytemuck1.25.2+derive1.12.1, standalone locks add syn3where required.
Core Rust gate:12pairs/OS,10queries in6case groups, all median ratios<=1.05.
The Windows raw summary still says three in one old static limits string; actual
round_orders and24outputs prove12. Wrapper fixed to interpolate round count before
Linux run; rust-wrapper-windows.py preserves the exact earlier wrapper source.
Native unit tests prove pointer/capacity preservation. Contract suites pass9tests
for each installed CP311/312 wheel perOS. Core138+doc1, framing4, Client7, session23
perOS; allocation tests3perOS. Initial Windows allocation-test launch missed the
Python DLL on PATH; final log includes the corrected runtime path and passes.
Initial benchmark attempt used a relative config path with a child cwd change;
it failed before any query. Verified runs resolve output/config paths and reran all
jobs. Initial setup expected7Rust cases but filtering the frozen input yielded10;
the actual gate used and validated all10, whose input is preserved here.
No native Linux or Windows MSVC run is claimed; user explicitly deferred those
environments. No hard native timeout is introduced; existing CLI/session behavior
is retained. Windows short-query CI remains broad; sampled peaks are lower bounds.
SHA256SUMS.json covers every other file in this directory.

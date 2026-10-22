# Windows GNU handle lifecycle evidence

The public Arc<FastKStore> fixture uses GetProcessHandleCount on the current process.
Each of 1/4/8 workers configurations executes576000queries over120waves.
All360quiescent measurements are134handles, baseline69; after each last owner
drops, count returns69. Total1728000full-record comparisons,90.70s test duration.
Read lease blocks append while retained; explicit append after last owner succeeds.
Owned records survive store drop. Root FD remains null onWindows and is not used
as a Windows counter. This is whole-process handles, not a per-path file list or
an in-flight peak; finite duration does not establish indefinite leak freedom.

Rerun with FASTK_LIFETIME_WAVES=120:
cargo test --release --locked --offline --manifest-path external-tests/scaling-profile/Cargo.toml --test shared_lifetime -- --nocapture

Only the independent test manifest/fixture/documentation changed. Windows API
dependency is a target-specific dev dependency. Product/native implementation
remains identical to all77frozen source hashes in the preceding native-handoff
evidence. No additional package/lockfile change was needed. Prior evidence stays
unchanged; before fixture/manifest/report and new exact sources are included.
This run is GNU, not MSVC. User deferred nativeLinux/MSVC until available.
SHA256SUMS.json covers every other file.

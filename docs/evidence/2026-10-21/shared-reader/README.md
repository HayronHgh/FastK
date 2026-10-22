# Shared Reader validation

See [report](../../../SHARED_READER_LIFETIME.md).
Frozen consumer-source.zip includes final harness, fixture and matrix runner.
CP312 wheels use the unchanged current core (the earlier scaling source.zip
contains all 143 product source files and the provenance hash manifest).
These wheels are diagnostic before-copy baselines, not new release bundles.
Both 1/4/8 shared-reader matrices verify all 1430 original queries per configuration.
Native suites: nine tests per OS. Consumer lifecycle suites: four tests per OS.
Linux sustained fixture: 17,280,000 record-verified queries, 3600 root-FD snapshots
all65, each last-owner drop returns0; total63.29s.
Windows GNU fixture: 1,728,000 queries over120waves/configuration,64.49s;
root FD counting unsupported/null. No Windows handle-leak claim is made.
All fixture first_wave_ns includes20query passes and thread startup, not one read;
it is not a platform speed comparison. Initial Windows12-wave fixture was run
while another build could overlap, so use only its correctness assertions.
The initial shared-lifetime-linux.log failed a wrong test assumption (writable
open must fail). Corrected public-API test checks actual append takes the lock;
all later final/stress/sustained runs pass. Product locks were not changed.
WSL2 is not native Linux; Windows GNU is not MSVC. No source format, return layout,
native Record-to-Vec conversion, timeout or replay implementation changed.
Finite stress tests establish this observation window, not indefinite safety.
SHA256SUMS.json hashes every other file.

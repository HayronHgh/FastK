# Scaling diagnostics evidence

See [findings](../../../SCALING_LIMITS.md) and the diagnostic README in source.zip.
Primary: 78 counter processes, 26 no-perf controls, seven successful native-filesystem
recordings; the earlier failed bind-mount recording is retained with its failure log.
Capacity-bandwidth: 30 paired randomized capacity processes, 36 copy-size processes,
six larger-cache counter runs. Cache-counts: six unchanged-core counter runs.
Serial-bandwidth: 12 supplemental serial-copy processes (different time period).
Source.zip freezes all 143 unchanged product source files plus diagnostic sources.
The larger-cache build differs only by DEFAULT_FILE_CACHE_CAPACITY 64 -> 512;
scaling-capacity-proof.json records that checked source difference.
*.data.gz are raw perf recordings. *-self.txt remain original; *-self-kallsyms.txt
explicitly label manual same-boot nearest text-symbol mapping. The attempted
*-resolved-self.txt reports still contain unknown kernel symbols; do not use them
as resolved evidence. The full kernel map is not exported; its SHA is in provenance.
*.bin.gz are diagnostic Linux executables, not distributable release artifacts.
No new full product regression is claimed: the isolated diagnostic test passed one
positional-IO test; bandwidth/cache_counts targets currently contain zero unit tests.
All data belongs to WSL2, not native Linux. No MSVC verification is included.
SHA256SUMS.json covers every other file in this directory.

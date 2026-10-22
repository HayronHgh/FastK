# Bulk bridge output diagnostic

One scalar range from the standard input: S00, feature/ma20, 1m,
1704067260000 through 1730419140000 inclusive. Each response contains 437,115
rows and 30,161,245 bytes. Before/after each have three samples, with identical
SHA-256 output across all six responses. See comparison.json for binary hashes
and every sample.

Median process startup plus full pipe transfer fell from 2.5433782s to
0.080323s after adding a 64KiB stdout buffer with explicit flush. Parsing is
outside the timer. OS cache is uncontrolled, order is before then after, and
this is a local diagnostic, not an independent general engine speedup claim.

Reproduce against a prepared standard store using the public bridge:

```sh
python external-tests/probe_bulk_output.py --bridge /path/to/fastk_bridge --root results/fastk --symbol S00 --start-ts 1704067260000 --end-ts 1730419140000 --output probe.json
```

The original binary is identified by its hash but is not shipped as a second
supported release. Only the buffered version is included in development wheels.

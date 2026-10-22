# Native allocation handoff comparison

This opt-in harness compares two locally built CPython wheels against the same
prepared read-only store and query lists. It does not instrument production code
or compare different engine revisions. Keep the before wheel built from the
current core before changing record-to-word conversion.

Run from the project root with `external-tests` on `PYTHONPATH`:

```text
python -m native_handoff.run --before BEFORE.whl --after AFTER.whl --store STORE --manifest MANIFEST.json --bulk BULK.json --short SHORT.json --output NEW_DIRECTORY
```

Use matching Python/ABI wheels. The harness extracts them into separate module
directories and each worker verifies its actual extension SHA before querying.
The current interpreter's NumPy and other benchmark dependencies are reused.
There is no pip reinstall between measured processes, silent fallback or replay.

The workload is the frozen four bulk cases at internal budgets 1/4/8 plus all
1,430 short queries at budget one. Three independent process pairs per setting
are randomized in pair order and before/after order. Each process uses the public
matrix worker: 32 untimed warm reads per query, 30 measured repetitions per bulk
case, one measured repetition per short query. Canonical warm validation and full
packed-byte validation of every measured result run outside the query timer.
Each result is destroyed outside the query timer. OS cache is uncontrolled.

The existing process monitor samples RSS and private memory from the actual
worker PID at 5 ms. Peaks include backend imports, warmup and validation, are
sampled lower bounds, and cannot identify an exact transient allocation by
themselves. Native unit tests separately establish pointer/capacity preservation,
including reserved spare capacity and empty results.

`manifest.json` records wheel/module SHA, job order and output filenames. Preserve
all raw outputs and logs. Summaries should compare medians of per-process medians,
retain all three process pairs and report noise; do not pool individual samples
as independent process repetitions or claim native Linux/MSVC from WSL2/GNU data.

Use `--serial-only --rounds 9` for a fixed supplemental sequence after a short-read
warning. Keep the original three pairs; summarize both directories with
`python external-tests/native_handoff/summarize.py PRIMARY SUMMARY.json --supplement SUPPLEMENT`.
This reports every retained process/cohort and does not discard the warning.

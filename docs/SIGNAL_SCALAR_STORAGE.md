# Signal-as-Scalar Bridge

FastK does not define signal semantics.

This document only describes a caller-side convention for storing signal-like
scalar series using the existing scalar storage API.

The values `-1`, `0`, and `1` are not interpreted by FastK. They are ordinary
scalar values. Any meaning such as short, neutral, long, inactive, active,
approval state, rule kind, feature version, report identity, or strategy usage
belongs to the caller's registry and research layer.

## Physical Shape

Use the existing scalar bridge commands:

- `write-scalar-range`
- `read-scalar-range`
- `query-scalar-predicate`
- `scalar-inventory`

Caller convention:

```text
symbol = BTCUSDT
timeframe = 15m
category = signal
name = sigvec_test
records = [{ ts, value }]
```

`category = "signal"` is a scalar namespace selected by the caller. FastK must
not branch on it or apply signal-specific logic. For FastK, `feature`, `factor`,
`signal`, `risk`, and `metric` are all scalar categories.

## Caller Encoding Convention

Binary convention:

```text
0 = inactive
1 = active
```

Ternary convention:

```text
-1 = short
 0 = neutral
 1 = long
```

These meanings are caller conventions only. FastK only stores and compares
integer scalar values.

## Bridge Examples

Write a caller-provided signal vector:

```bash
cargo run --example fastk_bridge -- write-scalar-range \
  --root ./data/store \
  --symbol BTCUSDT \
  --timeframe 15m \
  --category signal \
  --name sigvec_test \
  --input-json ./signal_rows.json
```

Example `signal_rows.json`:

```json
{
  "timeframe_ms": 900000,
  "records": [
    { "ts": 1706745600000, "value": 0 },
    { "ts": 1706746500000, "value": 1 },
    { "ts": 1706747400000, "value": -1 }
  ]
}
```

Read the stored scalar rows:

```bash
cargo run --example fastk_bridge -- read-scalar-range \
  --root ./data/store \
  --symbol BTCUSDT \
  --timeframe 15m \
  --category signal \
  --name sigvec_test \
  --start-ts 1706745600000 \
  --end-ts 1706747400000
```

Find non-zero values:

```bash
cargo run --example fastk_bridge -- query-scalar-predicate \
  --root ./data/store \
  --symbol BTCUSDT \
  --timeframe 15m \
  --category signal \
  --name sigvec_test \
  --start-ts 1706745600000 \
  --end-ts 1706747400000 \
  --predicate ne \
  --value 0 \
  --return-values
```

Find rows whose value equals `1`:

```bash
cargo run --example fastk_bridge -- query-scalar-predicate \
  --root ./data/store \
  --symbol BTCUSDT \
  --timeframe 15m \
  --category signal \
  --name sigvec_test \
  --start-ts 1706745600000 \
  --end-ts 1706747400000 \
  --predicate eq \
  --value 1 \
  --return-values
```

Find rows whose value equals `-1`:

```bash
cargo run --example fastk_bridge -- query-scalar-predicate \
  --root ./data/store \
  --symbol BTCUSDT \
  --timeframe 15m \
  --category signal \
  --name sigvec_test \
  --start-ts 1706745600000 \
  --end-ts 1706747400000 \
  --predicate eq \
  --value -1 \
  --return-values
```

`eq 1` only means the stored scalar value equals `1`. The caller may interpret
that as a long or active state. FastK does not.

## Ownership Boundary

FastK owns:

- accepting scalar records
- chunk writes
- range reads
- scalar predicate queries
- scalar inventory
- checksum, sidecar, zmap, and vix storage behavior

The caller owns:

- `signal_vector_id` generation
- feature or factor lineage
- report identity
- rule kind and rule parameters
- approval state
- strategy eligibility
- whether a stored value should be interpreted as long, short, active, or neutral

Do not add signal registries, SignalResearch parsers, approval workflows,
strategy adapters, or bitset encodings to FastK core.

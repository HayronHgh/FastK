# Scoped Python bridge sessions

`with Client(root) as store` reuses one local worker. Existing methods, integer
values, metadata, return dictionaries and error codes stay unchanged. Calls
outside a context retain one process per operation. `version --json` and the
one-shot CLI's pretty JSON bytes are unchanged.

The constructor still performs its one-shot version check. Context entry starts
and validates a separate worker once; exit terminates and reaps it. `close()` is
idempotent and permanently closes the Client. Nested entry is rejected without
disrupting an outer context. A closed Client requires a new instance.

## Ownership and failure contract

The worker dispatches through the same CLI adapter and library functions. Each
request opens and releases its own store and OS guard before responding. An
idle session retains no read/write lock or stale store handle. External writes,
pending recovery and newly sealed roots remain visible on subsequent requests.
Application errors return the existing FastK error and leave the worker usable;
they do not trigger a retry. Explicit `init()` remains the recovery operation.

One request runs at a time per Client. The request deadline covers waiting for
that slot, writing the frame and receiving the response. Payload serialization
and Python response decoding remain outside the IO timeout, as in one-shot
mode. Timing out before dispatch sends nothing and leaves an active call alone.
After dispatch, timeout terminates/reaps the worker and joins its IO threads.
The interrupted write may already have committed months: inspect/recover before
making a new explicit write. No operation is automatically replayed.

Worker exit, truncated/oversized frames, invalid JSON, wrong response IDs and
malformed error envelopes poison the session. The failed call raises TIMEOUT
or PROTOCOL; subsequent calls raise SESSION_BROKEN without starting another
worker. Startup failure closes the Client. Context exit and explicit close also
interrupt an active call; concurrent callers are never silently redirected to
one-shot execution. Ordinary business errors remain recoverable in-session.
Closing during frame construction cannot enqueue a new request; closing with
a queued request wakes its caller without waiting for the request deadline.

## Private transport

The private `--session-stdio` entrypoint is used only by matching client/bridge
artifacts. It is separate from the public one-shot command catalog. Frames have
an eight-byte little-endian unsigned length followed by UTF-8 JSON. The maximum
request/response body is 1GiB; invalid lengths fail closed. Larger operations can
use existing one-shot calls or split ranges. No default storage/record format
or public result type changes.

Handshake: session protocol 1, public contract 1 and the frame limit. Requests
carry an increasing integer ID, existing command arguments and an optional
existing JSON payload. Responses wrap the ID and either the unchanged result
object or existing `{code, message}` error. Session results now use compact JSON
with the same typed response serializer and field order. The public one-shot
CLI still emits its original pretty JSON, including the trailing newline.
Only whitespace changed in the private wire; no protocol version bump is needed.

The receiver validates the frame length before allocating its body bytearray,
then fills it with bounded `readinto` calls. The existing decoder consumes that
buffer directly. Rust builds the envelope and compact result in one Vec; an
application error clears any partial success before serializing the error into
the same allocation. Nothing reaches stdout before the frame is complete.
There is no buffer pool; the IO loop can retain its last completed exchange
until the next request or context close, as before. See the
[buffer verification](BUFFER_REUSE_RESULTS.md) for paired latency and memory
measurements before compaction. The [compact session verification](COMPACT_SESSION_RESULTS.md)
compares the next step against that preserved buffer build, including exact
CLI bytes and private response value/encoding checks.

One dedicated IO thread handles framed writes/reads so blocked pipes can be
interrupted. A separate drainer continuously consumes stderr, retaining at most
64KiB. No process-wide pool, background service or core engine transport trait
is introduced. Failed sessions require a new Client; they never auto-reconnect.

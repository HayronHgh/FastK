// Node example, not a supported SDK. Preserve JSON integer tokens until the
// application selects a lossless i64 parser; JSON.parse can round OHLCV values.
import { execFile } from "node:child_process";
const [binary, root] = process.argv.slice(2);
if (!binary || !root) {
  console.error("node node_bridge.mjs /absolute/fastk_bridge /absolute/store");
  process.exit(2);
}
execFile(binary, ["kline-inventory", "--root", root, "--symbol", "BTCUSDT", "--timeframe", "1m", "--error-format=json"],
  { windowsHide: true, timeout: 30_000, maxBuffer: 16 * 1024 * 1024 },
  (error, stdout, stderr) => {
    if (error) { process.stderr.write(stderr || error.message); process.exitCode = 1; return; }
    process.stdout.write(stdout); // Forward the original bytes as text, without numeric conversion.
  });

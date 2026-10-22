# 熱讀修正與 lazy 開啟／讀取／關閉實驗

已實作短範圍讀取與 read-only metadata 修正。正式三輪完整短查詢週期由 611.05 µs 降至 440.30 µs，降低 27.9%。真正熱檔案命中由 7.20 µs 降至 4.90 µs。

你的目標流程是每次需要資料才建立接口、讀完即關閉。這個流程的排名，與長時間保留 Arrow mmap 的暖讀排名不同；兩種結果均保留。

## 為什麼先前 Arrow 熱讀快四倍

上一輪比較的是 Arrow 256-file mmap cache（足以保留 240 個月檔）與 FastK 預設 64-file handle cache。在整個工作集隨機重播時，FastK 反覆開檔；Arrow 多數工作可直接使用已保留的映射與欄位視圖。OS page cache 的暖機沒有消除 CreateFile／關閉 handle 的成本。

本次將『同一 query 暖機 32 次後立即量測』與『暖機整個工作集後隨機重播』分開：

| 狀態 | 原版 FastK µs | 新版 FastK µs | Arrow µs |
|---|---:|---:|---:|
| 即時命中同一熱檔案 | 7.20 | 4.90 | 14.00 |
| 240-file 工作集重播 | 78.85 | 80.85 | 18.60 |

所以四倍差距不能歸因成 FastK 的 NumPy 轉換或熱檔案讀取必然較慢。工作集重播的 handle 淘汰成本仍然存在；本次沒有擴大預設快取、加入常駐 mmap，或在 Reader 關閉後偷偷保留檔案。這些是現有 adapter 的 NumPy 交付時間，不是 Rust 與 Arrow C++ 的理論上限比較。

## 實作

1. Kline 小範圍跨同一或相鄰 sparse blocks 時，合成一次連續 span，直接讀到輸出 Vec，二分查找後原地裁切。保留前面月份已寫入的前綴，省下暫存 Vec 與重複讀取。無號差值使用 checked_sub，損壞的逆序 window 回 INVALID_DATA。
2. read-only metadata 僅保留在 store 本地快取，關閉即釋放；不讀取或發布 process-global writable metadata cache，略過首次讀取前後重複的 fingerprint／exists。現有 shared OS lease、開啟時的 root invalidation 與 pending-recovery 檢查保留。Writable cache 與 migration 行為維持原樣。

沒有新增公開接口或依賴。i64、Kline 48 bytes、Scalar 16 bytes、NumPy readonly／精度／順序、close 等待讀取、讀寫互斥及 CLI JSON 格式維持。原地裁切保留 Vec spare capacity，未宣稱每個小結果的 backing allocation 都精確等於 nbytes。

## 每次開啟 → 請求 → 關閉

同一 Python 程序、程式庫已 import，但每個 query 都建立新的 Reader／Connection。Close timer 包含 close() 與 adapter 物件銷毀；回傳陣列在關閉後做完整驗證。沒有先暖機該 Reader。

下表為三個獨立程序各自 p50 的中位數。短查詢仍是原本 1,430 筆、14–128 rows；兩個 bulk 各 437,115 rows。bulk 每程序每 case 五個完整週期。

| 後端 | 短查詢完整週期 ms | Kline bulk 完整週期 ms | Scalar bulk 完整週期 ms |
|---|---:|---:|---:|
| FastK | 0.440 | 5.989 | 2.457 |
| Arrow IPC | 0.747 | 17.901 | 8.998 |
| Parquet Zstd | 3.539 | 53.112 | 22.888 |
| DuckDB | 24.100 | 78.951 | 66.024 |
| SQLite WITHOUT ROWID | 1.416 | 529.091 | 257.653 |

短查詢相位拆解，µs；相位中位數相加不必等於總時間中位數：

| 設定 | Open | Read／NumPy | Close／銷毀 | Total | p95 Total | p99 Total |
|---|---:|---:|---:|---:|---:|---:|
| 原版 FastK | 234.55 | 352.95 | 15.40 | 611.05 | 956.75 | 1273.48 |
| 新版 FastK | 228.95 | 189.35 | 15.90 | 440.30 | 696.65 | 949.07 |
| Arrow IPC | 147.70 | 549.25 | 46.05 | 746.65 | 902.44 | 1055.06 |
| Parquet Zstd | 167.10 | 3322.20 | 34.20 | 3539.00 | 4511.65 | 5197.89 |
| DuckDB | 13016.60 | 6318.35 | 4495.55 | 24099.80 | 31660.33 | 34710.05 |
| SQLite WITHOUT ROWID | 1168.25 | 168.90 | 69.30 | 1415.65 | 1977.53 | 2303.68 |

關閉接口釋放 Reader、檔案 handle、metadata 與 read lease；使用端仍持有的 NumPy 資料不會被釋放。這讓回測可以在接口關閉後繼續運算。

## SQLite 路徑解析對照

在 open 相位發現既有 SQLite adapter 每次呼叫 Path.resolve()，會額外做 filesystem canonicalization。測量前確認 DB path 已為 absolute、resolve 與 absolute 指向完全相同的路徑，再只於隔離測量程序把該呼叫改為 Path.absolute()；原始 adapter 檔案不改，SQL、PRAGMA、NumPy 轉換與 close 邊界維持。

三個新程序、同批 1,430 個 query，4,290 個完整輸出通過。這是另一個獨立 cohort，不覆寫原主表：

| SQLite 設定 | Open µs | Read µs | Close µs | Total µs |
|---|---:|---:|---:|---:|
| 原 adapter | 1168.25 | 168.90 | 69.30 | 1415.65 |
| 略過重複 canonicalization | 811.25 | 160.00 | 65.00 | 1039.85 |

這部分差距屬於我們的 Python adapter，不能歸功於 FastK 核心。控制後的 SQLite 完整週期約 1.040 ms，仍高於本輪 FastK 的 0.440 ms，但不能推論所有 SQLite bindings／連線設定都如此。

## 檔案清單與第一次 worker 請求

主比較沿用既有 file adapter：呼叫端提供已讀取的外部 manifest，Arrow／Parquet 在每次 open 重建分區清單。FastK 則在 query 中載入要求的 binary series manifest。為避免把免費常駐的清單當作格式本身的能力，另外把 file adapter 的 JSON catalog 讀取／解析也放進每次 open：

| 後端 | 清單已在呼叫端的週期 ms | 每次重新讀取清單的週期 ms |
|---|---:|---:|
| Arrow IPC | 0.747 | 2.239 |
| Parquet Zstd | 3.539 | 6.056 |

這是本專案 adapter 的 JSON 清單實作成本，不能宣稱是所有 Arrow／Parquet 系統不可避免的成本。

以下另取每個新 worker 的第一個完整短查詢週期，三個樣本的中位數，µs。它包含第一次後端操作，但程式庫 import 仍在計時外；不是 Python 程序啟動時間，也不是 OS cold-cache：

| 後端 | 新 worker 第一個週期 µs |
|---|---:|
| FastK | 842.30 |
| Arrow IPC | 2162.20 |
| Parquet Zstd | 6450.60 |
| DuckDB | 32687.50 |
| SQLite WITHOUT ROWID | 2176.20 |

## Bulk 警訊與固定補測

主三輪 Kline bulk 的原版 5.6633 ms、新版 5.9892 ms，曾顯示約 5.8% 退步。保留這個結果，隨後預先固定六組相鄰 before／after 配對，每個 case 每程序改為 20 次，組內版本順序隨機；沒有重跑到想要的結果才停止。

| Case | 原版完整週期 ms | 新版完整週期 ms | 六組 paired ratio 的中位數 |
|---|---:|---:|---:|
| small_kline | 0.9309 | 0.7313 | 0.789 |
| medium_kline | 0.8648 | 0.7148 | 0.792 |
| bulk_kline | 5.7529 | 5.5884 | 0.962 |
| bulk_scalar | 2.5642 | 2.3512 | 0.936 |

補測未重現同等幅度的 Kline bulk 退步，但不抹去主測試警訊。兩組 repetition 數與執行順序不同，分開呈現；ratio of medians 與 median of paired ratios 也不是同一估計量。尚未證明所有機器與每次執行都不退步。

## Worker 記憶體

三個程序的取樣峰值中位數，MiB。包含 bootstrap 後模組常駐、快取、暖機與驗證，不是單次 read 的配置增量。Windows private commit 不含 mmap file pages；RSS 含駐留映射頁，但不等於整個 OS page cache。5 ms 取樣峰值是下界。

| 後端 | Short private | Short RSS | Bulk private | Bulk RSS |
|---|---:|---:|---:|---:|
| FastK | 26.56 | 39.71 | 44.57 | 58.29 |
| Arrow IPC | 39.48 | 60.78 | 56.57 | 96.82 |
| Parquet Zstd | 64.20 | 69.10 | 124.02 | 135.23 |
| DuckDB | 52.62 | 74.52 | 102.78 | 122.83 |
| SQLite WITHOUT ROWID | 27.45 | 41.17 | 197.54 | 208.82 |

## 驗證與使用方式

- 最終 60 個程序、60,420 個計時輸出；bulk 補測另 12 個程序、960 個輸出。全部 canonical／完整 byte hash 與相同 NumPy 契約通過。初始診斷 8 程序與中間 v1 60 程序也保留，加上 SQLite 路徑對照 3 程序、4,290 個輸出，總計 143 程序、134,710 個計時輸出。
- Windows GNU：142 個 Rust tests、1 個 doc test；CPython 3.11／3.12 的最終安裝 wheel 各 9 個 native tests；最終 CLI 4、Client 7、Session 23，以及量測 ownership／失敗不重播 3 tests。
- 保留跨月前綴、缺口、逆序 sparse window 損壞、readonly／int64 精度、close race、truncation、讀寫 lease 與 hard timeout／no replay 驗證。
  Hard timeout 是 subprocess Client／session 的行為；native API 維持同進程呼叫，沒有新增 hard timeout。
- 有一次回歸因漏設 workspace TEMP／TMP，遭 sandbox PermissionDenied；失敗 setup log 與改回 workspace temp 後的通過 log 一併保留，不視為成功執行。
- 三輪為描述性比較；bulk 每程序只有五個主測試樣本，沒有 latency SLA 或統計顯著性保證。OS cache 未控制、資料是含缺口的合成行情；不是實際市場資料、磁碟冷讀或端到端回測。原生 Linux／MSVC 仍依先前安排待有環境再測。

| Check | Command | Exit／Result | Evidence |
|---|---|---|---|
| Rust | cargo test --locked --offline | 0／PASS：142 + doc 1 | logs/lazy-read-verified-core-tests.log |
| Native wheels | build_native_wheel.py --offline，CP311／312 | 0／PASS | logs/lazy-read-final-build311.log、build312.log |
| Native contract | unittest discover -s external-tests -p test_native.py，各 ABI | 0／PASS：各 9 | logs/lazy-read-final-native311.log、native312.log |
| CLI | cargo test --example fastk_bridge --locked --offline | 0／PASS：4 | logs/lazy-read-verified-cli-tests.log |
| Client／Session | unittest test_client.py、test_session.py，實際新版 bridge | 0／PASS：7／23 | logs/lazy-read-verified-client-tests.log、session-tests.log |
| Measurement ownership | unittest read_lifecycle.test_worker | 0／PASS：3 | logs/lazy-read-harness-tests.log |

以一個月或一批需要的資料作為請求單位，讀完後即關閉，再進行運算。逐根 Kline 反覆開關會反覆支付約數百 µs 的開啟／metadata 成本，與此設計的批次用途不同：

```python
with Reader(root) as reader:
    bars = reader.read_kline_range(symbol, "1m", start_ts, end_ts)
# Reader 已關閉，read lease 已釋放；bars 仍是有效的 readonly NumPy array。
run_backtest(bars)
```

接口與回傳格式沒有更換；原始資料不因關閉接口而失效，也沒有隱藏的重開或重播。

## 原始證據與重跑

實測：Ryzen 9 9900X、Windows GNU、CPython 3.12.14，NumPy 2.3.5、PyArrow 26.0.0、DuckDB 1.5.6，單執行緒預算 1。
[完整摘要與原始結果](evidence/2026-10-21/lazy-read/lazy-read-verified/summary.json)；同目錄保留 before／v1／final source、實際 wheels、queries、logs、補測及 SHA256。
重跑指令與相位定義見 [read_lifecycle README](../external-tests/read_lifecycle/README.md)。資料及 venv 位於分析副本；Desktop 原始專案同步程式碼、文件及證據，不複製大型 stores。

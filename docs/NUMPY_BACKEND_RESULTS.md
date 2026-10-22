# 最新 FastK：相同 Python／NumPy 契約的五後端比較

FastK 已有低 Python worker 記憶體與直接 range 交付的優勢，但這次沒有證明它比 Arrow 更快。Kline bulk：FastK 4.212 ms、Arrow 3.545 ms。額外交錯三輪、讓 Arrow mmap cache 容納全部月檔後，短查詢 p50：FastK 70.80 µs、Arrow 16.90 µs；Arrow 約快 4.19 倍。

主比較的 Arrow 64-file cache 會讓它在 240-file 短查詢工作集中反覆重新映射，因而顯示 FastK 大幅領先。這個配置相依的結果必須保留，但不能拿來宣稱 FastK 具有普遍的純讀速度優勢。

實測日期：2026-10-21。Windows、Ryzen 9 9900X，CPython 3.12.14；FastK 為 Windows GNU build。
本輪不修改引擎、native 接口、CLI 或既有後端 adapter。

## 比較方式

- 五個後端都在 Python 同進程呼叫；單一 handle、每次一個 query、內部執行緒預算 1。
- 回傳已物化、C-contiguous、readonly 的 NumPy structured array；每欄 little-endian int64。Kline 是 timestamp + OHLCV，共 48 bytes/row；Scalar 是 16 bytes/row。
- 計時包含公開 Python 呼叫、後端查詢與 NumPy 物化。開啟、暖機、正確性驗證與結果釋放不在 query timer 內。沒有要求原本就能交出目標 layout 的 FastK 再複製一次。
- 每個 backend × workload 跑五個獨立程序；每輪隨機排列後端／workload。每 query 先暖機 32 次，bulk 計時 30 次；短查詢每程序計時同一批 1,430 筆一次。
- 表中延遲是五個程序各自中位數的中位數。p95／p99 亦先在各程序計算，再取五個程序的中位數；不是將全部樣本混成一次測試。
- 每個 query 首次結果對 canonical row hash；之後所有暖機與計時結果都驗證 dtype、順序及完整 packed-byte hash。38,750 個計時輸出全部通過。
- 同一組預先建立的 standard stores：24 個標的、10 個月，10,490,760 筆 Kline、41,963,040 筆特徵；固定 seed 的合成資料含缺口。這不是實際市場資料。
- OS cache 未控制，不能標成 cold cache。暖機不保證整個工作集仍在各後端的有界快取裡。

## Bulk 與小範圍讀取

Kline bulk 與 Scalar bulk 各 437,115 rows，輸出分別為 20.01 MiB 與 6.67 MiB。

| 後端 | Kline bulk ms | Scalar bulk ms | 127-row Kline µs | 4,077-row Kline µs |
|---|---:|---:|---:|---:|
| FastK | 4.212 | 1.006 | 72.10 | 101.25 |
| Arrow IPC | 3.545 | 1.171 | 113.50 | 110.80 |
| Parquet Zstd | 48.908 | 20.686 | 3080.60 | 3833.95 |
| DuckDB | 48.394 | 36.526 | 4752.15 | 4925.30 |
| SQLite WITHOUT ROWID | 501.209 | 248.014 | 229.40 | 3952.50 |

## 原本的 1,430 筆短查詢

全部是 Kline range，每次 14–128 rows，共 181,812 rows，橫跨五組各 286 筆的原始查詢。
「總 query 時間」是 1,430 個 query timer 的加總，再取五個程序的中位數；不包含迴圈、驗證與結果釋放，不是完整任務 wall time。

| 後端 | p50 µs | p95 µs | p99 µs | 總 query 時間 ms |
|---|---:|---:|---:|---:|
| FastK | 69.65 | 92.90 | 118.66 | 83.62 |
| Arrow IPC | 473.60 | 617.05 | 742.38 | 553.61 |
| Parquet Zstd | 2220.30 | 2888.74 | 3260.59 | 3312.88 |
| DuckDB | 3652.45 | 5171.06 | 5856.50 | 5551.62 |
| SQLite WITHOUT ROWID | 122.30 | 179.26 | 199.87 | 192.49 |

短查詢 p95／p99 混合不同標的與 range；它們描述這批查詢的尾部，不代表單一 query 的重複延遲分布。

## Arrow 快取敏感度：另行對照

主比較對 Arrow adapter 設定 64-file mmap cache，對應 FastK 預設的 64-file handle budget；這不代表兩者消耗相同記憶體。短查詢的工作集跨 240 個月檔，因此另外在測量程序中只把 Arrow cache cap 改為 256，涵蓋全部月檔。
初次三個新的獨立 Arrow256 程序得到 p50 16.90 µs，4,290 個計時輸出通過。再交錯重跑三輪 FastK／Arrow64／Arrow256，各設定每輪一個新程序，設定順序每輪隨機，共 9 個程序、12,870 個計時輸出通過。
兩個對照都仍使用相同 1,430 個查詢、32 次暖機、單執行緒、相同 NumPy 格式與完整驗證。只在隔離測量程序中改變 Arrow cap 常數；產品與既有 adapter 檔案未改動。下表使用交錯對照，並非混入主比較五輪。

| 設定 | 程序數 | p50 µs | p95 µs | p99 µs | Short private MiB | Short RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| FastK 預設 64-file handles | 3 | 70.80 | 93.20 | 122.03 | 26.27 | 40.71 |
| Arrow 64-file mmap | 3 | 474.40 | 617.75 | 712.98 | 40.10 | 65.46 |
| Arrow 256-file mmap | 3 | 16.90 | 20.30 | 26.17 | 42.61 | 119.46 |

交錯對照 Arrow256 p50 / FastK p50 = 0.24×。擴大 cache 後排序翻轉，確認主比較的 Arrow 短查詢劣勢高度依賴快取設定。兩者的 64-file budget 並非記憶體等價，這裡也沒有推估相同 MiB 限額下的最優設定。

## 記憶體

實際 Python worker 每 5 ms 取樣，表中是五個程序峰值的中位數，單位 MiB。
包含 bootstrap 後的模組常駐、後端快取、暖機與驗證；不是單次 read 的增量配置。bulk 欄涵蓋四個 case，不能分配成各個 case 的峰值。
Private 是 Windows private commit，不含檔案映射頁；RSS 是 working set，含駐留的 mmap 頁，但不是整個 OS page cache。兩者要一起解讀，取樣峰值均為下界。

ReadFile 與 mmap 都使用 OS page cache；worker 用量較低不等於整機實體 RAM 等比例減少，本輪未隔離量測整個系統的檔案快取。

| 後端 | Bulk private | Bulk RSS | Short private | Short RSS |
|---|---:|---:|---:|---:|
| FastK | 44.48 | 57.98 | 26.11 | 40.38 |
| Arrow IPC | 57.48 | 105.47 | 39.95 | 65.98 |
| Parquet Zstd | 135.01 | 147.53 | 77.77 | 83.46 |
| DuckDB | 126.21 | 145.60 | 178.87 | 200.25 |
| SQLite WITHOUT ROWID | 212.39 | 225.77 | 46.89 | 60.98 |

## 能支持的優勢與代價

- Kline bulk：Arrow IPC / FastK = 0.84×、Parquet Zstd / FastK = 11.61×、DuckDB / FastK = 11.49×、SQLite WITHOUT ROWID / FastK = 118.99×。比值 >1 表示 FastK 較快，<1 表示該後端較快。
- Scalar bulk：Arrow IPC / FastK = 1.16×、Parquet Zstd / FastK = 20.56×、DuckDB / FastK = 36.30×、SQLite WITHOUT ROWID / FastK = 246.51×。比值 >1 表示 FastK 較快，<1 表示該後端較快。
- 短查詢 p50：Arrow IPC / FastK = 6.80×、Parquet Zstd / FastK = 31.88×、DuckDB / FastK = 52.44×、SQLite WITHOUT ROWID / FastK = 1.76×。比值 >1 表示 FastK 較快，<1 表示該後端較快。

相對 SQLite，FastK 主比較短查詢 p50 約快 1.76 倍；相對 Arrow，Scalar bulk 延遲較低，但 Kline bulk 與充分快取的短查詢都落後。因此目前較扎實的使用理由是低 worker 用量及固定格式的直接交付，而不是『所有讀取都更快』。

FastK 的設計理由是直接取得固定 schema、依 timestamp 定位的精確 OHLCV／Scalar 批次。核心按 chunk／固定 record 讀取到 owned buffer，native 把同一份配置交給 NumPy，省下 SQL 執行與欄式到 row layout 的搬運；這是原始碼支持的機制解釋，本輪沒有重新拆解各階段的因果成本。
Arrow 仍使用 mmap、時間欄二分定位與 64-file cache，沒有強迫它改成慢速逐列 API。Parquet 使用 Zstd、8,192-row row groups、時間 filter 與 projection；SQLite 是 WITHOUT ROWID、(symbol, ts) primary key、64 MiB page cache；DuckDB 是官方 fetchnumpy 路徑再封裝成共同 structured dtype。SQL 的 ORDER BY ts 與輸出順序契約一致。
因此結果回答的是「這套 adapter 把同樣資料交到 NumPy 手上的時間」，不是 SQL 引擎的理論上限。Arrow 原生欄式、DuckDB 聚合／join、Parquet 壓縮／跨工具分析有不同的最佳使用方式；本輪沒有測這些任務。

完整 prepared store 的磁碟大小如下，包含所有 Kline、Scalar 及各自索引／metadata，不能解讀成 Kline bytes/row：

| 後端 | Store MiB | Files |
|---|---:|---:|
| FastK | 1773.75 | 3241 |
| Arrow IPC | 1121.38 | 1200 |
| Parquet Zstd | 473.29 | 1200 |
| DuckDB | 4058.26 | 1 |
| SQLite WITHOUT ROWID | 2866.08 | 3 |

## 穩定性與範圍

| 後端 | 五輪 Kline bulk p50 範圍 ms | 五輪 Short p50 範圍 µs |
|---|---:|---:|
| FastK | 4.004–4.503 | 68.50–79.50 |
| Arrow IPC | 3.360–3.629 | 469.00–529.85 |
| Parquet Zstd | 46.814–54.020 | 2198.15–2851.20 |
| DuckDB | 45.850–54.419 | 3612.10–3891.90 |
| SQLite WITHOUT ROWID | 499.067–506.357 | 116.50–129.30 |

五輪是描述性比較，沒有證明所有機器、cache 狀態或每次執行都維持相同比值。此輪沒有測 cold start、並行 scaling、寫入／修訂、predicate、projection、聚合、真實市場資料或完整回測。也沒有補上仍待環境的原生 Linux／MSVC 驗證。

原始 open_ns 僅是各 adapter 建構時間；FastK worker 為驗證 binary hash 提前 import native，各庫 import 時點不同，不得拿此欄當完整啟動速度排名。

## 重跑與原始證據

使用現有 prepared stores，不在讀取測試中 ingest 或維護資料：

```powershell
Set-Location 'C:\Users\a3204\Documents\Codex\2026-10-20\fastk-coding-2026sideproject-db-fastk-fastk'
$env:PYTHONUTF8="1"
target/v02-venv/Scripts/python.exe external-tests/compare_numpy_reads.py `
  --stores target/external-standard-evidence `
  --manifest target/external-standard-final/input/manifest.json `
  --bulk target/v02-final-linux-matrix/bulk.json `
  --short target/v02-final-linux-matrix/short.json `
  --output target/numpy-five-backends-win-new --process-rounds 5
```

依賴：NumPy 2.3.5、PyArrow 26.0.0、DuckDB 1.5.6、psutil 7.2.2；SQLite 版本及 extension SHA 見 manifest。
本次虛擬環境與大型 prepared stores 留在上述分析副本；原始 Desktop 專案同步 runner、報告與證據，不複製大型 stores。
[summary.json](evidence/2026-10-21/numpy-five-backends-win/summary.json) 保留每程序 p50／p95／p99、記憶體、開啟時間；同目錄保留全部原始輸出、configs、查詢、輸入 manifest、來源快照與 SHA256。
分析副本與原始 Desktop 專案的 77 個既有來源雜湊一致；本輪僅新增外部 runner 與證據。舊 benchmark 和 release bundles 保留。

Runner 後續僅經 Ruff 排版，AST 比較一致；計時時的原始 bytes 另存 measured-runner.py，符合 manifest 的 harness SHA，交付排版後版本見 source.zip。計時 worker 與所有 adapter 未改動。

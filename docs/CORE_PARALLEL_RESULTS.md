# Rust 核心並行修正與驗證

這次修改核心快取 `chunk/cache.rs` 與內部讀取排程 `engine/parallel.rs`。Python binding、資料搬運、公開 Rust API、Kline48／Scalar16 磁碟格式及數值精度都維持原樣。測量主體是獨立 Rust executable，回傳既有 `Vec<Record>`；Python 腳本只負責啟動前後版本，未參與任何計時中的查詢。

## 修正內容與邊界

原本 header 命中取得 Mutex；file 命中也取得 Mutex，並在線性 LRU 佇列內尋找、刪除及重新複製 key。並行讀取即使完全命中快取，也必須輪流通過這些獨占區段。

檔案快取的命中改用 RwLock 共用讀鎖；header 保留既有短 Mutex，沒有為統一風格全面換鎖。檔案快取維持每個 store 最多 64 個 cached files，改採 second-chance 淘汰：命中只檢查／標記 referenced bit，只有 miss 的插入及淘汰才修改佇列。開檔、positional IO，以及淘汰／失效／清除時的關檔，都在檔案快取寫鎖外執行；進行中的 reader 仍持有 Arc<File>，淘汰不會使它失效。

這是以近似使用近期程度取代精確 LRU 的交換；不同工作集的命中率仍可能不同。沒有增加快取策略接口、全域 FD pool、dummy FD 預擴、公開原始指標接口或新產品依賴。內部排程另依 payload 限制有效 worker 批次，最後只改兩個產品模組，另增加測試與外部量測程式。

原本只要超過 4 MiB，就把約 1 MiB tasks 全部交給指定的 pool。新版本保留 4 MiB 進入並行的邊界，並依實際 bytes 限制有效工作批次：Windows 約 1 MiB／有效 worker，其他平台採保守的 4 MiB。既有 tasks 再分組，最多形成這個有效 worker 數；序列路徑直接填入各 span，省下中介 task Vec 與切分。threads 仍是上限，沒有新 API 或自動改寫使用者設定。

Linux 6.7 MiB Scalar 因工作量不足留在序列；Windows 的同筆讀取仍保留並行收益。曾試過全平台統一 4 MiB 粒度，Windows Scalar 明顯退步，因此不採用。這是已量測環境支持的內部保守政策，其他 Unix kernel／硬體仍需實測；不保證這組粒度在所有系統最優。

## 共用同一個 store 的純 Rust 暖讀

沿用凍結的 1,430 筆短查詢。每個獨立程序先對每位 worker 預讀一筆，再完整執行兩遍；表列同 handles 第二遍、三輪配對程序吞吐的中位數。每組最後完成 worker 的時間作為終點；校驗與上一組的等待均不混入計時。全數計時結果在計時外對照既有 canonical SHA256，並驗證完整欄位及順序。

| 環境 | callers | 修改前 queries/s | 修改後 queries/s | 比例 |
|---|---:|---:|---:|---:|
| windows | 1 | 22,406 | 22,139 | 0.99× |
| windows | 2 | 30,844 | 37,252 | 1.21× |
| windows | 4 | 51,070 | 54,250 | 1.06× |
| windows | 8 | 48,542 | 69,254 | 1.43× |
| linux | 1 | 123,292 | 126,195 | 1.02× |
| linux | 2 | 197,030 | 203,174 | 1.03× |
| linux | 4 | 156,313 | 311,903 | 2.00× |
| linux | 8 | 112,074 | 257,400 | 2.30× |

這輪 Linux 的 8 callers 相對修改前提升約 2.30 倍，Windows 約 1.43 倍。這是核心共享快取路徑的收益，不涉及 GIL、NumPy 或 JSON。不同輪次中 4／8 callers 的排名會變動，不能保證加倍執行緒就加倍吞吐。raw 保留所有單次查詢分布及每輪總時間，沒有只挑最佳輪。

## 首次讀取與獨立 handle 的資源成本

每個獨立 store 仍有自己的快取與 OS shared lease。這次不引入以 root 為 key 的全域資源登錄，也沒有將 kernel FD 表預擴藏到初始化。Linux 首次開檔的 FD 表成長停頓仍存在；之前的 [歸因報告](NATIVE_COST_ANALYSIS.md)保留因果控制與 kernel 機制推論。下面把首次遍歷與暖讀分列，不能以新程序或第一遍宣稱 OS cold-cache。

| Linux callers | sharing | 第一遍前→後 queries/s | 第二遍前→後 queries/s | 修改後組末 FD 計數 |
|---:|---|---:|---:|---:|
| 1 | shared | 26,668 → 33,814 | 123,292 → 126,195 | 69 |
| 1 | independent | 46,803 → 37,778 | 117,505 → 118,364 | 69 |
| 2 | shared | 36,605 → 35,858 | 197,030 → 203,174 | 69 |
| 2 | independent | 28,582 → 30,483 | 241,213 → 235,168 | 134 |
| 4 | shared | 52,830 → 35,640 | 156,313 → 311,903 | 69 |
| 4 | independent | 19,937 → 15,160 | 378,908 → 374,723 | 264 |
| 8 | shared | 35,397 → 37,473 | 112,074 → 257,400 | 69 |
| 8 | independent | 12,093 → 13,304 | 311,830 → 339,407 | 524 |

組末 FD 計數包含標準 IO、lease 及讀取 `/proc/self/fd` 的目錄 descriptor，不等於 cached files 數量。8 個獨立 store 可保留 512 個 cached files；共用一個 store 仍是 64。獨立 handle 的結果沒有普遍改善，Linux 首次停頓也沒有被這個快取鎖修正消除。

同進程並行讀取應沿用既有 `Arc<FastKStore>`：

```rust
let mut store = FastKStore::open_read_only(root)?;
store.set_metrics_level(MetricsLevel::Off); // 明確選擇效能量測的觀測等級
store.set_read_threads(1)?; // 小查詢由 caller 並行，不另開每人一組 pool
let store = std::sync::Arc::new(store);
// 將 Arc::clone(&store) 交給 worker，直接呼叫既有 get_* 方法。
```

最後一份 Arc 被 drop 前，原有讀取 lease 持續有效；writer 仍需等 reader 釋放。獨立開啟的 store 仍各自管理生命週期，沒有自動被合併成隱藏的共享狀態。

## 大型查詢與序列退步檢查

同一份資料、同一組 7 筆 range/latest probes，包括原本四個 bulk-suite 查詢；每個 case 每程序 32 次暖機、30 次計時、三個配對程序。Linux serial medium 在初輪約慢 7.3%，因此事先固定追加九輪 serial 配對，與原三輪合併；序列表用全部 12 輪共 360 個 observations／版本／case，不丟掉警示資料。第一份結果做 canonical SHA，之後每次讀取與它完整比對，校驗均在計時外。主矩陣 90 筆／case 不報 bulk p95/p99。序列補測另以 paired process median ratio 做 bootstrap，避免把同一程序的 30 次讀取當成完全獨立程序。

| 環境 | serial case | 前 ms | 後 ms | 後／前 |
|---|---|---:|---:|---:|
| windows | short | 0.0043 | 0.0043 | 1.000 |
| windows | medium_kline | 0.0371 | 0.0372 | 1.003 |
| windows | bulk_kline | 3.3999 | 3.3664 | 0.990 |
| windows | bulk_scalar | 0.8470 | 0.8312 | 0.981 |
| windows | latest | 0.0020 | 0.0018 | 0.900 |
| windows | bulk_latest | 3.4030 | 3.3916 | 0.997 |
| linux | short | 0.0025 | 0.0025 | 0.984 |
| linux | medium_kline | 0.0441 | 0.0444 | 1.007 |
| linux | bulk_kline | 0.9799 | 0.9496 | 0.969 |
| linux | bulk_scalar | 0.1433 | 0.1409 | 0.983 |
| linux | latest | 0.0017 | 0.0017 | 0.980 |
| linux | bulk_latest | 0.9785 | 0.9706 | 0.992 |

兩平台這輪所有 serial cases 均在 5% 退步門檻內；不宣稱 serial bulk 已變快。這是合併資料的中位點估值門檻；paired-process bootstrap 區間保留於 serial-gate summary，部分上界仍跨過 1.05，不宣稱每次執行都沒有退步。這個門檻只用於明列的 serial cases，不代表所有 threads 或獨立 handle 的分布都沒有退步。

| 環境 | 內部 workers | 修改後 Kline bulk ms | Scalar bulk ms |
|---|---:|---:|---:|
| windows | 1 | 3.3516 | 0.8091 |
| windows | 2 | 2.0179 | 0.5178 |
| windows | 4 | 1.4614 | 0.3967 |
| windows | 8 | 1.3753 | 0.3603 |
| linux | 1 | 0.9652 | 0.1400 |
| linux | 2 | 0.7561 | 0.1448 |
| linux | 4 | 0.7130 | 0.1401 |
| linux | 8 | 0.8860 | 0.1422 |

主矩陣的 Linux 8-worker Kline bulk 從 1.1856 降至 0.8860 ms（約縮短 25%），Scalar 從 0.7194 降至 0.1422 ms（約縮短 80%），移除了這批資料上明顯的過度分派退步。Windows 同 budget 的 Kline 約持平，Scalar 從 0.4413 降至 0.3603 ms。Linux Kline 仍以 4 workers 最快，8 沒有勝過 4；Scalar 也只是避免退步，沒有線性擴展。保留預設 1，依工作量選擇 budget，不把支援 8 解讀成 8 一定更快。

此 Rust bulk harness 保留第一份結果作校驗，會改變 allocator／頁面重用狀態；不能將本表絕對耗時與先前 Python attribution 的 core ms 直接相減。兩邊交付既有 Record Vec，沒有引入 mmap 或變更資料格式。

## 獨立記憶體採樣

另跑同一組 probes，對實際 Rust PID 約每 5 ms 採樣；採樣工作的延遲全部排除於上方效能表。每個記憶體情境各一個程序，不提供統計顯著性或無漏峰保證。Windows private 是 private commit，Linux 是 USS，不能跨平台直接比較。

| 環境 | 8 workers workload | RSS 前→後 MiB | private 前→後 MiB |
|---|---|---:|---:|
| windows | short-shared | 10.27 → 10.66 | 5.91 → 6.48 |
| windows | short-independent | 16.00 → 15.61 | 12.00 → 11.56 |
| windows | bulk-shared | 52.98 → 52.93 | 48.57 → 48.52 |
| linux | short-shared | 7.51 → 7.30 | 5.90 → 5.77 |
| linux | short-independent | 11.96 → 11.83 | 10.41 → 10.30 |
| linux | bulk-shared | 49.92 → 49.95 | 48.36 → 48.25 |

這組採樣沒有看到 payload 級的額外記憶體增加。Linux 共用與獨立 store 的 FD 峰值也單獨記錄；短暫重複開檔、進行中讀取所保留的 Arc<File> 可能讓程序總 FD 超過 cache capacity，64 不是整個程序的硬上限。

## 驗證與交付範圍

| Check | Command | Exit/Status | Evidence | Result |
|---|---|---|---|---|
| Windows GNU、Linux Rust 全套 | `cargo test --locked --offline --features fault-injection` | 各 0 | verified-*-tests.log；各 142 tests | PASS |
| CLI JSON/framing | `cargo test --locked --offline --example fastk_bridge` | 各 0 | verified-*-framing.log；各 4 tests | PASS |
| 本機 release CLI | `cargo build --release --locked --offline --example fastk_bridge --example fastk_admin` | 各 0 | verified-*-cli-build.log | PASS |
| 純 Rust 配對量測 | `external-tests/core-parallel/run.py`，每平台 72 primary＋18 serial-supplement processes | 各 0 | windows/linux summary + raw | PASS |
| 記憶體與 FD | `external-tests/core-parallel/memory.py`，每平台 6 processes | 各 0 | memory raw + samples | PASS |
| 樣式 | root／standalone cargo fmt；Ruff check／format | 0 | style.log | PASS |
| Python／native wheel 套件重打 | 本次優先處理核心，沒有修改 binding | NOT RUN | 舊 wheel／dist 證據保持原樣 | NOT RUN |
| MSVC／其他 Linux kernel | 本機只有 Windows GNU 與 WSL2 | NOT RUN | 不外推測試覆蓋範圍 | NOT RUN |

新增回歸涵蓋：淘汰、invalidate／clear 後既有讀取仍有效；失敗開檔可由明確後續呼叫再嘗試；8 threads 同時 miss／hit／eviction 保持有界；各 budget 的共享 Kline／Scalar bulk 與 latest 完整一致；檔案截斷時任何 budget 都回傳錯誤，不公開半成品。原有 16,000 筆共享並行回歸及 crash／recovery／讀寫排他測試一併重跑。

Windows 最初在受限沙箱跑測試時有 PermissionDenied；使用已授權測試執行及專案內 TMP 重跑通過，原失敗 log 保留。初期 cache 探索有一批在編譯尚未結束時執行，其 timings 無效且未採用。最後採用的 verified runs 均在最後版本編譯成功後重跑，測量期間無本任務編譯工作；最後採用的是 file-cache CLOCK、原有 header Mutex、工作量分批與直接序列填入；全面 header RwLock、過早的 serial cleanup 及全平台統一粒度等嘗試不當成交付結果。

既有 Python bridge 的 timeout、失敗不重播及 pretty JSON 程式碼完全未改；沒有把 native 同步呼叫改成可強制中止，也沒有新全域 cache／lease 行為。沒有改版號、發布套件或重寫舊效能報告。

[可重建來源、完整 raw 與 SHA256](evidence/2026-10-21/core-parallel/README.md)。

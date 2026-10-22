# FastK 核心並行擴展邊界：2026-10-21

本輪只診斷 Rust 核心，未修改正式引擎、native binding 或 Python 傳輸。
8 workers 輸給 4 的原因有兩種：短查詢遇到固定容量檔案快取的淘汰與共享狀態競爭；
bulk 則出現額外 pool 排程成本，且主要 CPU 樣本落在 kernel 複製路徑。
不能把兩種現象合併歸因為一把讀鎖，也不能由此宣稱測到了硬體 DRAM 上限。

## 範圍與方法

環境是 Ryzen 9 9900X、24 logical CPUs、Docker/WSL2，kernel
`6.18.33.2-microsoft-standard-WSL2`，Rust 1.97.1。
不是原生 Linux；Windows MSVC 與原生 Linux 的平台粒度結論仍待驗證。
guest topology 不足以推論實體 CCD 配置。容器沒有 CPU quota throttling。

使用既有 1,430 筆查詢及 bulk Kline 437,115 rows / 20,981,520 bytes，
沿用 canonical SHA-256。資料在容器原生檔案系統；未修改原始資料。
獨立診斷 crate 開啟 frame pointers，perf FIFO 僅包住 warm 後重複讀取階段，
排除開啟、完整驗證與輸出 JSON。熱迴圈檢查列數，warm/final 完整驗證內容。
phase 包含結果釋放；個別 read 計時不含釋放。下表採三個獨立程序結果的中位數。

主實驗有 78 個 counter 程序、26 個同 executable 的無 perf 對照、7 個 cycle profiles。
capacity 對照另有 30 個隨機配對程序；Basic cache metrics 使用獨立的非計時程序。
這些是診斷數值，不應與 release benchmark 的絕對時間混用。
完整重跑方法見 [診斷工具](../external-tests/scaling-profile/README.md)。

## 1. Bulk：額外執行緒沒有帶來對等工作量

| Pool threads | 引擎最多有效工作組 | phase ms / bulk call | CPU ms / call | context switches / call |
|---:|---:|---:|---:|---:|
| 1 | 1 | 1.3492 | 1.349 | 0.01 |
| 2 | 2 | 1.0774 | 2.035 | 4.30 |
| 4 | 4 | 0.9447 | 3.120 | 6.70 |
| 5 | 5 | 1.0039 | 3.593 | 13.95 |
| 8 | 5 | 1.0478 | 3.820 | 20.77 |

目前 Unix 每 worker 至少 4 MiB，因此這筆 bulk 的 8-thread pool 最多只有 5 組工作。
5→8 沒增加有效工作組，CPU 與切換次數卻提高；擴大 pool 本身有代價。
控制在相同 4 組工作時，pool 4→8 仍退步：

| 同資料量控制 | pool 4 / groups 4，ms | pool 8 / groups 4，ms |
|---|---:|---:|
| 空 group dispatch | 0.1991 | 0.2272 |
| reusable-buffer memcpy | 0.6690 | 0.7355 |
| reusable-buffer flat-file pread | 0.5776 | 0.6501 |

這些控制沒有 FastK 檔案快取，證明 bulk 的退步不能全部歸咎快取鎖。
控制的配置、配置器與 IO layout 不同，不能直接相減算出各部分精確佔比。

cycle profile 在 pool 4 的單一 `_copy_to_iter+0x8b` IP 佔 78.53%，pool 8 為 67.93%。
這是 CPU cycle 樣本佔比，既不是整個函式加總，也不是 wall-time 佔比。
kernel 名稱以同 boot 的非零 kallsyms 最近 text symbol 映射，原始 perf 報告保留。
所有七個 profiles 的 lost samples 為 0。steady page faults 接近零；
pool 8 的 generic cache misses 沒有高於 pool 4，沒有證據支持「8 threads cache miss 暴增」的說法。

## 2. 短查詢：64 檔快取發生 churn

每一 cycle 是完整 1,430 查詢，不是一筆 query：

| Store ownership | 1 worker，ms/pass | 4 workers | 8 workers |
|---|---:|---:|---:|
| 共用一個 Arc Store | 10.3707 | 2.8310 | 3.5381 |
| 每 worker 一個 Store | 10.3693 | 2.7282 | 2.3323 |

共用 Store 的 CPU 成本由 10.911 增至 21.633 ms/pass，context switches
由 2.90 增至 136.55。private Store 在 8 workers 仍有擴展，但開更多 Store 會增加 FD 與生命週期成本。
profile 同時看到檔案快取、header lookup、RwLock contention、配置器及 kernel FD/page-cache 路徑；
不能把全部差距指派給單一函式。

實際工作集是 240 個檔案，超過正式快取容量 64。
獨立 Basic metrics 的 shared 8-worker run 在 143,000 次查詢中
miss 105,613 / hit 37,387，miss rate 73.86%；header miss 為 0。
private Store 也有近似 miss rate，並非因它能更常命中才較快。
miss 會走 open、寫鎖發布與淘汰／close，shared Store 將這些操作集中在共同狀態。

為檢驗因果，只在 ignored 的凍結診斷副本把容量常數 64 改為 512，正式引擎保持不變：

| cache capacity | shared 1，ms/pass | shared 4 | shared 8 | private 4 | private 8 |
|---:|---:|---:|---:|---:|---:|
| 64 | 10.2774 | 2.7866 | 3.4578 | 2.6987 | 2.2787 |
| 512（診斷） | 4.9354 | 1.4635 | 1.4311 | 1.3509 | 0.9387 |

512 的計數對照全數 143,000 hits / 0 misses，共用 Store 的 8-worker 退步消失，
但吞吐接近平台期，且仍慢於 private 8。剩餘差距可能包含 shared header/metadata、Arc 與 read-lock 成本；
本輪未把這些逐項隔離，不能宣稱已全部解決。

FD 計數亦有代價：shared 64 為 69→69，shared 512 為 245→245。
assigned-query warmup 的 private 4/8 在容量 64 時為 264/524，容量 512 時為 762/1023，
各自讀完仍相同。這是 steady FD，不是首次開啟成本或跨整天無洩漏證明。
private profiling 會先完整 warm 每個 handle，與此 FD 計數的 warmup 不同。

## 3. 複製吞吐：工作切分要有成本依據

另以 32 warmups / 128 copies、三程序中位數量測 initialized buffer，
時間內無配置／IO／引擎鎖，結尾驗證完整 bytes。
數值是 bytes/ns 換算的 decimal GB/s，不是 GiB/s 或實體記憶體控制器計數：

| 資料量 | serial 1/1 | pool 4 / groups 4 | pool 8 / groups 4 | pool 8 / groups 8 |
|---|---:|---:|---:|---:|
| 1 MiB | 25.58 | 6.56 | 3.95 | 2.08 |
| 4 MiB | 47.82 | 24.35 | 11.50 | 7.82 |
| 20 MiB | 32.26 | 30.56 | 29.66 | 27.73 |
| 80 MiB | 20.19 | 19.74 | 19.51 | 20.75 |

serial 控制在另一時間段補跑，未與平行組逐程序交錯；應視為方向證據。
小資料容易由 dispatch 主導，80 MiB 多執行緒的有效複製吞吐則約 20 GB/s 平台。
這不代表引擎 bulk 應全面改成 serial：實際引擎還有多檔讀取、配置與 page-cache 工作。

## 接下來的實作方向與未完成項目

1. 先讓實際消費端共用單一 Reader/Arc Store，測首次使用、重複使用、最後 owner 釋放與 writer lease；
   不以每 worker 私開 Store 掩蓋成本。其他 DB 若需要 thread-owned connection，應明確揭露。
2. 以實際 chunk 工作集與 FD 預算評估 cache，不直接把診斷 512 當正式答案；
   避免為 benchmark 增加公共配置介面或全域 dummy-FD pool。
3. 保留 payload 驅動的工作組數，測原生 Linux、Windows MSVC 再決定平台 grain。
   目前沒有完成這兩個環境的驗證。
4. 核心生命週期收斂後，再移除 native Record→word Vec 的額外配置／複製。
   現有 public API、i64 磁碟格式、NumPy layout/readonly/lifetime、讀寫 lease、失敗不重播、CLI JSON/timeout 保持不變。

本輪未重跑整套產品回歸，因產品來源未改；獨立診斷工具測試與格式檢查另行記錄。
本輪原始資料、source hashes、建置記錄、對照與統計位於
[scaling-profile evidence](evidence/2026-10-21/scaling-profile/README.md)。

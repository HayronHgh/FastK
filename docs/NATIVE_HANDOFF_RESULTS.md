# Native allocation handoff：2026-10-21

已移除 Kline range/latest 與 Scalar range 的第二個 Record→word Vec 配置和逐欄複製。
引擎原本的配置經 checked ownership cast 交給 NumPy；公開 signatures、欄位順序、
readonly、空結果、int64 精度與 reader 關閉後的資料存活方式維持原樣。
本輪優化的是記憶體所有權交接，磁碟讀取仍將資料複製進引擎 buffer。

## 實作與邊界

只有一個私有 `words` helper 使用 bytemuck 1.25.2 的
[try_cast_vec](https://docs.rs/bytemuck/1.25.2/bytemuck/allocation/fn.try_cast_vec.html)。
它檢查 alignment、length 與 capacity 的位元組相容性。
兩個既有 `repr(C)` record 加上
[NoUninit derive](https://docs.rs/bytemuck/1.25.2/bytemuck/derive.NoUninit.html)，
在編譯時拒絕 padding；native 另固定 size、alignment 與各欄 offset。
引擎保持不依賴 PyO3／NumPy。新 registry dependencies 是 bytemuck 與 bytemuck_derive；
獨立測試 lockfiles 亦同步必要的 syn 3，既有 registry 版本未更新。

cast 仍在既有 Reader read guard 與 GIL detach 的範圍內完成。失敗只回傳錯誤，
不重新讀取。NumPy 接手配置後不保留 store lease。predicate matches 包含 Option 值，
仍驗證每個 value 並建立 word 輸出。沒有修改讀寫鎖、close、CLI session 或 timeout。

## 修改前後量測

兩側都使用先前已修正核心並行的版本，只差本輪 ownership handoff；
不是拿舊核心的 release wheel 當基準。每個 child 在測試前驗證實際 extension SHA。
環境為 Ryzen 9 9900X、Rust 1.97.1、CPython 3.12.14、NumPy 2.3.5，
Windows GNU 與 WSL2。原生 Linux、Windows MSVC 依使用者安排留待有環境時驗證。

沿用四個 bulk case 與 1,430 筆短查詢，bulk budgets 1/4/8；每 query 先 warm 32 次，
bulk 計時 30 次，短查詢計時一次，內容完整驗證在 query timer 外。
初始每設定三組隨機配對程序。Windows 首輪短查詢 +12.8%、small case +7.6% 的警訊保留；
再固定追加九組序列配對，兩環境都保留原始三組與新增九組。
所以 budget 1 是 12 組、budget 4/8 是三組；統計採程序中位數的中位數。
每環境 60 個程序、38,640 個計時輸出全部通過完整內容驗證，兩側查詢與 dtype 一致。
OS cache 未控制，不標為 cold cache。

### Bulk（budget 1，12 組配對）

| 環境 | Case | Before ms | After ms | After / Before |
|---|---|---:|---:|---:|
| Windows GNU | bulk_kline | 8.3342 | 4.6993 | 0.564 |
| Windows GNU | bulk_scalar | 2.3385 | 1.2257 | 0.524 |
| WSL2 | bulk_kline | 12.5653 | 2.3489 | 0.187 |
| WSL2 | bulk_scalar | 4.8960 | 0.5401 | 0.110 |

437,115-row Kline 的輸出約 20 MiB。此 workload 的 bulk 改善明顯；
4/8-thread 與 small/medium 的全部結果保留在 combined summaries，不能由這張表推論通用資料庫排名。

### 短查詢（budget 1，12 組配對）

| 環境 | Before µs | After µs | After / Before | 配對 median ratio 的 bootstrap 95% interval |
|---|---:|---:|---:|---|
| Windows GNU | 77.20 | 78.15 | 1.012 | 0.982–1.080 |
| WSL2 | 12.82 | 12.77 | 0.996 | 0.983–1.023 |

短查詢接近持平，沒有宣稱同等倍數改善。Windows 信賴區間仍包含超過 5% 的退步；
點估計通過 5% 門檻不代表已證明每次執行都不退步。
interval 使用 5,000 次配對程序重抽樣，估計的是 paired median ratio，
與表中 ratio of medians 是不同估計量；小樣本、系統負載與 OS cache 均限制外推。

## 記憶體

下面是實際 Python worker 的 private memory peak，程序三／十二組中位數，MiB：
Windows 是 private commit，Linux 是 USS，不能直接把兩平台欄位當相同指標。
5 ms 取樣包含 warmup 與 validation，峰值是 sampled lower bound，不能單靠它判定精確的瞬時配置數。

| 環境 | Workload / budget | Before MiB | After MiB |
|---|---|---:|---:|
| Windows GNU | bulk / 1 | 64.46 | 44.31 |
| Windows GNU | short / 1 | 26.09 | 26.18 |
| WSL2 | bulk / 1 | 69.99 | 50.77 |
| WSL2 | short / 1 | 33.08 | 33.82 |

Bulk 約少 19–20 MiB；WSL2 短查詢約增加 0.7 MiB 的程序 private peak，未將它隱藏。
更直接的配置證據來自 Rust 單元測試：轉換前後的指標相同，capacity bytes 相同，
也涵蓋 spare capacity、零長度與不相容型別的失敗路徑。

## 回歸驗證

- Windows GNU／WSL2 各通過 138 個核心 Rust tests、1 個 doc test、3 個 native allocation tests。
- CPython 3.11／3.12 × 兩平台，四個實際安裝 wheel 各通過 9 個 native contract tests：
  layout、int64 極值、readonly、空結果、關閉後陣列存活、close race、lease 與截斷資料。
- 各平台 4 個 CLI framing tests、7 個既有 Client tests、23 個 session tests 通過，
  含 hard timeout、失敗不重播、讀寫互斥與現實子程序故障。
- 純 Rust 公開 probe 對凍結 baseline 與修改後來源各跑 12 組程序配對。
  固定 10 個非 short query（含五種 latest limit），六種 case 聚合中位數均未超過 +5%。
  此處亦只有點估計門檻，不是所有機器無退步保證。

舊 release bundles 與先前證據保留；此次 wheel 是驗證產物，版本仍為 0.2.0rc1。
原始輸出、所有程序樣本、module/wheel SHA、來源、lockfiles 與重跑工具見
[native-handoff evidence](evidence/2026-10-21/native-handoff/README.md)。


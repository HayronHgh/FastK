# 檔案快取容量對照：64／128／256

本次只做實驗，不修改產品預設、公開接口或安裝中的 native 模組。五輪相同 Python／NumPy 查詢顯示：240 月檔隨機工作集重播的主要差距來自檔案保留策略。FastK 256 在本次工作集完整命中，p50 7.10 µs、p95 10.40 µs；FastK 64 為 69.80／89.95 µs，同期 Arrow 256 為 17.10／20.70 µs。這解釋先前這個工作集的反超，不證明任意資料、欄位選擇與資源預算下 FastK 格式全面優於 Arrow。

## 方法與來源

- 來源為最新 lazy-read 核心；三份 native source 唯一語意差異是私有 DEFAULT_FILE_CACHE_CAPACITY 的 64／128／256。產品 62 份來源 checksum 前後一致，native 公開來源沒有更動，三個實驗 DLL 各自核對 SHA。容量並非環境變數注入正式產品。
- 同一批 1,430 short queries，14–128 rows，共 181,812 rows；同四個 bulk cases，含 Kline／Scalar 各 437,115 rows。沿用 24 symbols、10 months 的合成含缺口 standard stores（10,490,760 Kline、41,963,040 features），未更換資料。
- 五輪，每容量每 workload 一個新程序，輪內順序隨機。FastK 60 程序，另 Arrow256 熱讀參考 10 程序，共 70 程序、78,950 個計時輸出。第一份 query output 核對 canonical rows SHA，其後完整 packed-byte hash；全部通過 dtype、順序、readonly 與精度契約。
- 主計時包含相同公開 Python adapter 與 NumPy 交付，內部執行緒預算 1。沒有 Rust 探針。hot-local 每 query 32 次暖機後立即測；hot-replay 暖完整工作集再依原種子隨機重播。兩者另量一次 Reader open／close，未把這個一次性 close 混入每 query 延遲。
- cycle-short 每 query 重新開啟、讀取、close／銷毀 adapter；cycle-bulk 同四個 case、每程序各五次。結果在關閉後驗證，熱讀則保留最後一份輸出在 close 後再核對。資源快照在計時外，外部 monitor 以 5 ms 取樣實際 worker 記憶體。
- OS cache 未控制，import 與 Python 程序啟動在計時外；不是冷磁碟。Windows GNU、Ryzen 9 9900X、CPython 3.12、NumPy 2.3.5、PyArrow 26.0.0；非原生 Linux／MSVC 驗證。
- 所列延遲為各程序 p50 的中位數，p95／p99 亦先程序內計算再取中位數，不是合併樣本或信賴區間。主計時、歸因計時分開，不能用前後不同日期環境的絕對數字宣稱又有核心加速。

## 1. 熱讀與工作集重播

| 設定 | 同檔 p50 | 工作集 p50 | 工作集 p95 | 工作集 p99 | file hit rate | 重播開檔次數 |
| --- | --- | --- | --- | --- | --- | --- |
| 64 | 4.70 | 69.80 | 89.95 | 129.26 | 26.15% | 1056 |
| 128 | 4.60 | 11.60 | 78.28 | 103.14 | 53.08% | 671 |
| 256 | 4.60 | 7.10 | 10.40 | 12.97 | 100.00% | 0 |
| Arrow 256 | 13.70 | 17.10 | 20.70 | 26.74 | 未做同類 counter | — |

單位 µs。Hit rate 與 miss 數來自另三個 instrumented Rust 程序的中位數，每程序同批 1,430 queries；分母是 file-cache lookup 次數，不是一般性的 query 命中率。此熱重播中每 query 恰為一次 lookup。Header／sparse index 暖後零 header misses。

128 的中位數看似接近全命中，但 p95 仍接近 64：有 671 次 miss／1,430 次 lookup。256 則零 miss，尾延遲才一起降低。不能只看 128 的 p50 就認為淘汰問題已解決。

## 2. 分開量測開檔、位置 IO 與淘汰關閉

診斷另三輪、36 程序、38,790 個完整 canonical 檢查輸出；使用相同查詢與順序，但繞過 Python，以 Detailed metrics 及獨立探針觀察 Rust。下列是每程序「總 ns／query 數」的平均成本，再對三程序取中位數；不是每 query p50，也不能直接和上一表相減。

| 容量 | Hits | Misses | 開檔／query | IO／query | 淘汰關閉／query | 其餘 cache／query | Header misses |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 64 | 374 | 1056 | 30.26 | 3.73 | 3.99 | 0.35 | 0 |
| 128 | 759 | 671 | 20.71 | 3.35 | 2.86 | 0.30 | 0 |
| 256 | 1430 | 0 | 0.00 | 2.55 | 0.00 | 0.07 | 0 |

時間單位 µs／query。File::open 僅包開檔；IO 包位置讀取與 read-loop，冷路徑也會包含 header／index IO；淘汰關閉包 drop(Arc<File>)，在單執行緒、沒有其他 in-flight 引用的這組實驗釋放舊檔案，並非獨立 kernel CloseHandle profiler。Cache management = cache-path span − open span − eviction-drop span，避免重複加總。

開檔、淘汰次數隨 miss 降低，IO 平均成本仍在同一數量級；256 沒有開檔與淘汰，而仍需真正讀取與交付記錄。這支持先前差距主要由 handle churn 驅動。探針時鐘、原子 counter 會擾動短操作，不能把上述數字冒充原生 Python latency 的精確百分比分解，亦未證明多執行緒鎖競爭已解決。

## 3. 保留更多 handle，代價放在哪裡

| 設定 | Private MiB | RSS MiB | Active handle delta | 各程序 close 後 delta |
| --- | --- | --- | --- | --- |
| 64 | 26.49 | 41.09 | 65 | 0,0,0,0,0 |
| 128 | 26.37 | 41.08 | 129 | 0,0,0,0,0 |
| 256 | 26.60 | 41.09 | 241 | 0,0,0,0,0 |
| Arrow 256 | 41.97 | 119.54 | 241 | 1,1,1,1,1 |

MiB；記憶體為 hot-replay 的五程序取樣峰值中位數，含 import、暖機、驗證與 retained output。Active handle delta 為 query 階段相對 import 後 baseline 的程序 handle 數差。FastK 分別是 64／128／240 chunk handles 加一個 store lease；256 不會預先開滿 256 個。三種容量 close 後 delta 均為零。

Arrow 五程序 close 後皆比 import baseline 多一個程序 handle；其增加的 240 個檔案 handle 已釋放，剩下一個的來源未獨立鑑別，不能稱為 mmap 或 Reader leak。Native FastK 的 ownership／close 結果是本次直接驗證的範圍。

本批 FastK private 峰值 64→256 差約 0.11 MiB，RSS 幾乎重疊；不等於額外 handle 沒有 OS kernel 資源成本，也不是所有工作集、同時多 Reader 的總資源保證。記憶體保留與 OS page cache 不是同一件事。

| 容量 | 一次 Open | 一次 Close | 五程序 read p50 |
| --- | --- | --- | --- |
| 64 | 325.40 | 642.90 | 69.80, 71.15, 72.30, 63.10, 63.20 |
| 128 | 326.90 | 1063.70 | 23.10, 11.50, 13.95, 11.60, 11.40 |
| 256 | 341.50 | 1336.60 | 7.00, 7.10, 6.90, 7.10, 9.10 |

時間 µs。這是各 hot-replay worker 一次 open／close 的五程序中位數；close 包整個 adapter 關閉與銷毀。256 要集中釋放更多 handle，close 約 1.34 ms，64 約 0.64 ms。主 query timer 沒有藏掉這個成本，但它在表中獨立呈現，不能把高容量說成無代價。同期 Arrow 的一次 close 中位數約 12.76 ms，僅指本 adapter 與此工作集。

## 4. 每次重新開關接口，不會因 cache 容量大就自動變快

| 容量 | Open | Read／NumPy | Close | Total |
| --- | --- | --- | --- | --- |
| 64 | 192.90 | 154.30 | 15.00 | 364.10 |
| 128 | 189.70 | 150.60 | 14.90 | 356.30 |
| 256 | 194.90 | 159.00 | 15.00 | 370.40 |

單位 µs；相位各自中位數相加不必等於 Total。每個短 query 只觸及一個 chunk，同一 Reader 內沒有後續跨檔重用，三容量在診斷中都是一個 miss 加一次 payload open hit／query，容量不改變開檔次數。因此這些完整週期差異不能歸因成擴大容量的改善。

| 容量 | Small Kline | Medium Kline | Bulk Kline | Bulk Scalar |
| --- | --- | --- | --- | --- |
| 64 | 0.61 | 0.50 | 5.10 | 2.16 |
| 128 | 0.58 | 0.49 | 5.04 | 2.19 |
| 256 | 0.60 | 0.50 | 5.23 | 2.22 |

單位 ms，四個 case 的完整開啟／讀取／關閉週期，Small=127 rows、Medium=4,077 rows，兩個 bulk 各 437,115 rows。每個 bulk query 最多涉及十個月檔，三種容量皆足以容納，沒有容量淘汰。256 的 Kline p50 比 64 略高，資料不支持 bulk 因容量增加而更快，也不能用五輪證明所有執行皆不退步。原始分布保留。

這個區分直接影響產品選擇：每次一個 symbol range 後就 close 的用途，不應用本次長駐工作集數字宣稱收益；同一 Reader 完成一批跨標的／月份 query，才會支付或省去此處的重開檔成本。

## 核心優化方案，待評估而不實作

1. 優先修檔案保留策略，暫不引入 mmap、多執行緒或新資料快取。本次 256 在可容納工作集時已低於同期 Arrow 的 NumPy 交付延遲，尚沒有「必須換 IO 模型才能消除四倍差距」的證據。
2. 最小候選是調整 Reader 內現有有界 file-handle cache 的保留上限；以 lazy 觸及 chunk 開檔，保持現有 second-chance 機制、Reader close 釋放及 shared lease。不上 process-global cache，不預載整個 store，不新增公開 policy/provider 接口。
3. 256 是這批 240 檔工作集的有效候選，不是通用最佳值。決定產品預設前，先驗證 32／64／128／240／512／更大工作集，以及多 Reader 同時存在時的 handle、close、private／RSS 與失敗行為，再決定一個簡單且有資源上限的內部策略。先不寫自動調參器或多層 cache 框架。
4. 保留三個驗收面：同檔命中、跨工作集 p95／p99、完整 open/read/close；加 bulk 和多 Reader 資源。128 的好看 p50 不應取代尾部與 miss 判斷。若工作集在可接受 handle 預算下仍無法保留，才另測 mmap 或其他資料路徑。

## 驗證、版本與重現

- 三個獨立 native DLL 各通過現有 9 項 native tests（27 total）；沒有改測試條件或新增公開方法。
- 70 個未加探針的 Python 程序＋36 個歸因 Rust 程序，117,740 個計時輸出全數驗證。不是把兩種 timing 混成 106 輪相同測試。
- 所有 native FastK worker 的 Reader close 後 handle delta=0；Rust 診斷也都回到進程 baseline。產物 hash、query hashes、原始 samples、計畫、三份來源與 probe 修改均歸檔。
- 最早的工具鏈 temp 權限、snapshot 缺 README，以及共用 Cargo target 重用同 DLL 均保留為無效建置紀錄；正式量測只使用不同 target 產生的三個 DLL，實際載入 SHA 核對通過。沒有用建置成功代替容量已生效的檢查。
- 正式產品 DEFAULT_FILE_CACHE_CAPACITY=64、native 公開源碼與已安裝 binary 仍保持原樣。沒有修改網站文章或替換先前證據。

來源與重跑方式：`docs/evidence/2026-10-21/cache-capacity/` 的 experiment.zip、SHA256SUMS.json 與 study/README.md。大型 standard stores／Python venv 不在壓縮包，需要先依前次方法準備；全部來源 snapshot、runner、輸入查詢、真實量測與 DLL 在附件中。不得與同目錄初期 invalid artifacts 混用。

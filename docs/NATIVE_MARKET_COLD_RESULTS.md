# 原生接口冷讀：FastK 與金融時序儲存對照

2026-10-26。FastK 核心與公開接口未修改；測量來源為 main 的 `7645033`，實際編譯核心的每個原始碼雜湊均與該 checkout 相符。本次公開結果報告；測試程式、原始樣本、部署設定與實驗環境保留在本機，未隨報告發布。

這批結果支持 FastK 在**按標的讀取固定整數 K 線、取完關閉 reader**的用途。437,115 筆 bulk 的開讀關中位數是 17.41 ms；market-data 為 220.83 ms，約 12.7 倍。短查詢為 0.721 與 0.810 ms，領先幅度約 11%，沒有 bulk 那麼大。

FastK 的代價也很明確：本輪沒有壓縮，檔案比 market-data 與 HistoryVault GZip 大；TimescaleDB 的壓縮資料表更小。這不是「所有查詢都更快」或「所有格式都更省空間」的證據。

## 同一批資料與實際交付邊界

原資料是 24 個標的、2024 年 1–10 月、10,490,760 筆合成一分鐘 K 線，保留原本缺口與週末。價格、數量皆保留 scale 1e8 的精確值。1,430 筆短查詢與原始四個 bulk 案例的 清單逐位元組沿用，沒有重選範圍。

主比較是三個共同 K 線案例（127、4,077、437,115 筆）；Scalar 另測。每個設定與 workload 五個獨立程序，序列隨機排程，短查詢每輪完整重播，bulk 每案例每程序五次。主比較共 **80 組、57,800 次**，Scalar 共 **25 組、125 次**，全部逐筆雜湊相符。沒有背景編譯或資料匯入與正式測量重疊。

Rust FastK 回傳 `Vec<KlineRecord>`；market-data 回傳含 Decimal 價格的 `Vec<Candle>`；HistoryVault 回傳含 decimal OHLCV 與額外 CloseTime 的物件。TimescaleDB 使用原生 PostgreSQL binary protocol `Vec<Row>`，QuestDB 使用官方 QWP Arrow batches，MarketStore 使用官方 SDK 的 MessagePack → NumPy structured array。這些都是可使用的原生結果，物件配置與可表達的數值範圍不同。

FastK 使用公開 MetricsLevel::Off，未開啟內部讀取執行緒池；SQL 服務採固定 schema 與單工作執行緒設定，完整版本與參數保存在原始結果和部署紀錄。

計時包含公開 reader／連線的開啟、讀取到原生結果與關閉；結果關閉後仍可讀。完整內容驗證、統一整數格式轉換與雜湊在計時之外，RSS 在轉換之前取樣。控制管道的 JSON 只傳查詢、GO barrier 與測量資訊，**資料列不經 JSON bridge**。程式載入、JIT warmup、回傳結果的後續運算與銷毀不在這個讀取計時內。

MarketStore SDK 沒有公開 close 操作，本輪每程序重用其連線；該列是**僅查詢到 NumPy 可用**，不是完整開讀關，也不是免費的開關成本。資料庫服務本身持續運作，因此含服務傳輸的數字不等於純儲存核心耗時。

## 冷讀條件

所有資料在獨立 Linux named volume。每次測量前先清除目標檔案頁，`mincore` 必須確認選定頁面零駐留，讀取後 kernel storage read bytes 必須增加。失敗或 timeout 即停止、保留紀錄，不重播查詢。沒有全系統 drop_caches、記憶體壓力驅逐或停止其他專案服務。

本機函式庫每次建立新 reader，處理該標的檔案與共享根目錄中繼資料。TimescaleDB 先 CHECKPOINT、執行公開 pg_buffercache_evict，再清 bench 資料庫目錄的檔案頁；這包含資料表與 catalog，其條件比保留 catalog 的服務熱開啟更嚴格。QuestDB 清分區資料與 bitmap index，符號／交易根目錄中繼資料持續運作。MarketStore 清自己的儲存檔與映射。映射頁使用 scoped MADV_COLD／MADV_PAGEOUT，最多 16 次**準備**確認，未達零駐留不送資料請求。

這是 **Linux guest 資料頁冷讀**。Windows 主機 VHDX、裝置快取、程式碼與 inode/dentry 快取不受控制；不能稱為實體 NVMe 完全冷啟動。AMD Ryzen 9 9900X、WSL2 Linux 6.18.33.2、Rust 1.97.1、Python 3.12.14；主控 2 CPU／8 GiB，各服務 1 CPU／2 GiB，查詢引擎單工作執行緒。其他專案服務仍在運作，這是共享機器上的實驗。

## 本機函式庫

時間單位 ms；中位數為五個程序中位數的中位數，p99 為各程序 p99 的中位數。bulk 的 25 次結果不宣稱 p99。

| 原生接口 | 短查詢中位數 | 短查詢 p99 | 127 筆 | 4,077 筆 | 437,115 筆 |
|---|---:|---:|---:|---:|---:|
| FastK Rust | 0.721 | 0.942 | 0.687 | 2.970 | 17.406 |
| market-data Rust | 0.810 | 1.783 | 0.750 | 3.085 | 220.828 |
| HistoryVault raw | 6.448 | 9.516 | 9.238 | 8.806 | 138.205 |
| HistoryVault GZip | 9.547 | 14.048 | 13.278 | 12.323 | 208.636 |

market-data 的 read_range 預設跳過週末；本測試組合其公開 read_time_range 日內接口，包含週末並保留精確邊界，日期路由與組合成本計入讀取。上游原始碼未修改。它代表該專案的原生 Candle 交付路徑，不能外推成所有 Parquet 使用方式都慢。HistoryVault 使用公開 LoadAsync、單 IO 工作槽，分 raw 與 GZip；舊物件釋放與主動 GC 在冷讀 barrier 之前，這不是含 GC 的全應用吞吐測量。

## 服務型原生接口

這裡包含原生服務協定、結果建立與新連線成本；MarketStore 是明列的連線重用例外。不可用這一表宣稱 FastK 的查詢演算法領先相同倍數。

| 接口與設定 | 短查詢中位數 | 短查詢 p99 | 127 筆 | 4,077 筆 | 437,115 筆 |
|---|---:|---:|---:|---:|---:|
| TimescaleDB row | 36.368 | 42.561 | 35.551 | 39.336 | 487.966 |
| TimescaleDB compressed | 39.535 | 46.422 | 39.544 | 42.580 | 378.193 |
| QuestDB QWP / Arrow | 26.622 | 45.405 | 26.870 | 27.046 | 335.392 |
| MarketStore SDK（僅查詢） | 1.911 | 3.000 | 1.830 | 2.191 | 58.381 |

## 記憶體、磁碟與實際讀取量

下表是 437,115 筆 K 線 bulk。客戶端 RSS 是結果仍存活、轉換前每程序最大取樣值的中位數，含 runtime 與 allocator 保留；程序歷史高水位另外保留，包含先前驗證。服務欄是 cgroup 匿名＋共享記憶體的 2 ms 取樣峰值與邊界取樣，含小型觀測器，不是精確的瞬間峰值。共享記憶體不能漏算，也不能把多程序 RSS 直接相加。

磁碟排除輸入、執行檔、建置與測試結果；函式庫含自己的資料與中繼資料，服務含整個指定資料目錄與 WAL。MarketStore 有 sparse files，因此邏輯大小與實際配置差異很大。服務表中的數字不等於單一資料表大小。單位 MiB（2^20 bytes）。

| 設定 | 客戶端 RSS | 服務匿名＋共享 | 邏輯磁碟 | 已配置磁碟 | 每次 guest 讀取 |
|---|---:|---:|---:|---:|---:|
| FastK Rust | 24.01 | — | 481.54 | 482.82 | 20.10 |
| market-data Rust | 63.96 | — | 429.51 | 447.98 | 18.61 |
| HistoryVault raw | 239.71 | — | 960.47 | 961.41 | 40.05 |
| HistoryVault GZip | 250.75 | — | 195.00 | 195.84 | 8.15 |
| TimescaleDB row | 156.60 | 160.95 | 2323.38 | 2323.64 | 190.08 |
| TimescaleDB compressed | 156.77 | 162.63 | 1075.71 | 1075.94 | 9.14 |
| QuestDB QWP / Arrow | 104.99 | 404.06 | 636.90 | 637.18 | 561.25 |
| MarketStore SDK（僅查詢） | 171.41 | 174.73 | 1158.16 | 486.01 | 20.15 |

TimescaleDB 自身 hypertable_detailed_size 回報：row 的 K 線資料＋索引為 1264.68 MiB，compressed 為 16.85 MiB；整個 instance 的 WAL／系統資料配置另外占空間。壓縮的空間優勢確實存在，不能拿完整 instance 大小掩蓋。

guest 讀取量來自同一個 8:48 虛擬 block device：函式庫記錄程序 read_bytes，服務記錄整個 cgroup 的增量，包含該服務期間的 I/O，不能當成 Windows 實體裝置流量。檔案快取與共享記憶體另行記錄，未混入匿名記憶體數字。

## 為什麼出現這些差距

這次最有力的證據是交付成本和資料布局。FastK bulk 約讀 20.10 MiB，接近實際需要的固定記錄大小；QuestDB 同一查詢約 561.25 MiB。FastK 以標的／月份分檔，這個 QuestDB schema 則把 24 個標的放在共同月份分區、以 SYMBOL index 選取；時間排序後單一標的值散布在共用欄位頁。這是本 schema 下讀取放大的解釋，並非 QuestDB 所有 schema 都會有相同比例。

另一方面，market-data bulk 只讀約 18.61 MiB，仍比 FastK 慢，顯示讀取量不足以單獨解釋差距。原生 Decimal 解碼、逐日公開接口呼叫與 Candle／物件建立，是從程式結構可推論的成本來源；本輪沒有把它們個別拆時計時，不能把全部差額指定給其中某一項。TimescaleDB 壓縮版讀取量也小，但仍有 SQL 執行、結果交付與驅動配置成本。

因此使用 FastK 的理由是：當資料已正規化成固定整數，主要需求是按標的、時間範圍取 K 線供本機程式計算，它能以小型 reader 與接近所需大小的讀取量交付 owned records；不必部署常駐查詢服務。若目標是 SQL、多客戶端寫入或高壓縮比，這份實驗沒有提供 FastK 可取代那些能力的證據。

## 原始 Scalar bulk 補充

保留原 S00 ma20 的 437,115 筆、相同時間界線與完整預期雜湊；每接口五程序、每程序五次。market-data／HistoryVault 沒有對應 Scalar 公開操作，列為不支援；不製造假 K 線。FastK 使用新 Scalar store，服務使用原 instance 的獨立 features table／attribute group，因此這是補充讀取比較，記憶體不作獨立部署排名。

| 原生接口 | 437,115 筆 ms | 客戶端 RSS MiB |
|---|---:|---:|
| FastK Scalar Rust | 8.595 | 9.17 |
| TimescaleDB row | 243.751 | 85.09 |
| TimescaleDB compressed | 189.067 | 85.23 |
| QuestDB QWP / Arrow | 29.489 | 22.02 |
| MarketStore SDK（僅查詢） | 23.072 | 120.98 |

FastK Scalar store 邏輯大小 13.46 MiB，原始 timestamp＋value 記錄約 6.67 MiB，含原生 .vix、.zmap 與中繼資料後接近兩倍。這個成本保留下來，不能沿用 K 線的每筆大小。

QuestDB 剛匯入 Scalar 時有一頁 bitmap index 持續駐留，推測與尚存活的匯入寫入器有關；嚴格 cold preflight 因此停止，沒有排除該頁或重播測量。只重啟獨立 QuestDB fixture，再做原生 warmup、逐次零駐留驗證，後續 smoke 與五輪均通過。WAL、schema 與冷讀規則沒有因此更換。

## 未測項目與公開範圍

klinehouse 是 TimescaleDB 上的應用，其 schema 還要求 quote volume、trades、taker buy base／quote 等原輸入沒有的必填欄位。本輪不捏造這些值，也不修改原 schema；直接 TimescaleDB 的結果已列出，不能據此宣稱 klinehouse 應用的速度。

本輪 57,800 次 K 線請求與 125 次 Scalar 請求都核對了完整內容、筆數與冷讀條件；未完成或前置檢查失敗的試跑未混入排名。FastK 核心與公開接口未修改。

本次只公開這份結果報告，未上傳測試程式、原始樣本包、來源快照或部署環境，因此不能把本文當成已公開完整重現包。主機／裝置完全冷啟動、超過記憶體容量、持續寫入與多人併發仍需另外實驗。

[FastK 原始碼](https://github.com/HayronHgh/FastK) · [網站研究篇](https://www.hayronhgh.com/research/fastk-read-lifecycle) · [作品頁](https://www.hayronhgh.com/projects/fastk-store-engine)

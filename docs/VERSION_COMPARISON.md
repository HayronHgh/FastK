# 公開初版與目前版本：相同回測的交付路徑對照

日期：2026-10-22。GitHub 沒有正式 `1.0` tag；此處將口語的「FastK 1.0」對應到當時公開 `main` 的 commit `814c5667aa25c89373bffc0582cfbb5f49c9d066`，Cargo 版本是 `0.1.0-rc.1`。目前工作版是 `0.2.0-rc.1`，不擅自建立 1.0／2.0 版本承諾。

## 先分清楚比較的是什麼

初版已提供 Rust library，也能用 CLI 輸出 JSON。新的同進程 Python native／NumPy 模組是後來加入的能力。本輪比較「Python 使用各版本實際提供的路徑，完成同一份回測」，不是假設初版已有 native，也不是 Rust read 核心的隔離比較。

沿用 [固定回測](BACKTEST_RESULTS.md) 的同一個 `engine.py`、策略、資金、撮合、費用與輸出契約。獨立選用 2024 年 1–3 月 BTC／ETH 一分鐘 K 線，合計 **262,080 筆**；每月讀兩標的，讀完關接口再運算，跨月延續指標、持倉與待成交目標。每版本／策略五個獨立程序，組內版本順序隨機，共 **30 次完整回測**。

這是第一季 cohort，不能拿下表直接除以全年五後端實驗的 539 ms，也不由第一季外推全年倍率。

## 實測結果

| 案例 | 公開初版 CLI／JSON，總 ms | 目前 native，總 ms | 總時間比，初版／目前 | 初版載入 ms | 目前載入 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| RSI(14) 30／70 | 3333.029 | 130.897 | 25.46 | 3201.095 | 5.738 |
| SMA 20／60 | 3221.826 | 34.717 | 92.80 | 3190.146 | 5.471 |
| 60 根動能 | 3267.605 | 76.682 | 42.61 | 3187.120 | 5.618 |

表格是各五程序總時間與載入時間各自的中位數；倍率由兩個總時間中位數相除。交易、整條資金曲線、費用、期末持倉與權益在全部 30 次完全一致。兩版本合計 16 個預驗證查詢也與完整 canonical packed bytes 一致，包含每月與第一季一次讀取。

初版一次 read 呼叫就建立程序、開 store、讀取、輸出 pretty JSON、傳輸、Python 解析及逐列組成 NumPy；每月兩標的就是兩次命令。目前每月一個 native Reader，讀兩份 readonly int64 array，關閉後運算。前者約 3.19–3.20 s 的資料準備與後者約 5.5–5.7 ms 不是純磁碟 IO，而是完整公開交付成本。

初版的 open／close 發生於命令內，因此其 `read_ns` 包含內部開關；只比較 `load_ns` 與 `total_ns`，不將零值 adapter open 欄誤解成開 store 免費。程序啟動與退出仍在初版 load 計時內，Python 主程序及依賴匯入則兩邊都在應用 timer 外。

**這些倍率不能寫成 Rust 查詢演算法提升 25–93 倍。**固定記錄仍是六個 i64、48 bytes／row；同一份實體資料兩邊都能讀。本輪證明的是 Python 公開交付能力與整體應用成本的差距。若上層從一開始就直接呼叫初版 Rust library，不能套用此 CLI／JSON 倍率。

## 源碼可以支持的版本差異

| 面向 | 公開初版 0.1 rc | 目前 0.2 rc |
| --- | --- | --- |
| Rust／CLI | Rust library 與單次 JSON CLI | 保留兩者；另有 scoped Client 與 native NumPy |
| Python 資料路徑 | CLI JSON，由上層解析／轉換 | 同進程 readonly NumPy；舊 JSON 契約保留 |
| 資料 layout | Kline 六個 i64，48 bytes | 維持 layout 與精度，沒有靠縮成 i16 換速度 |
| 讀取／關閉 | 原始 store 與檔案 cursor 路徑 | positional IO、有界讀取、read-only ownership，close 後輸出仍有效 |
| 鎖與 recovery | 檔案存在式 writer guard、原始 recovery 流程 | OS ownership guard，writer／recovery 共用互斥與明確釋放 |
| 版本重現 | DatasetRef 與可更新 registry metadata | 另有 immutable publication／sealed snapshot 與驗證 |
| 證據 | Rust 格式比較、CSV／JSONL／SQLite | 五後端同 NumPy、cache／生命週期控制、固定回測與完整等價驗證 |

這張表描述能力與源碼差異，不把可靠性補強換算成效能百分比。並行的修正、有界工作與 metadata／sparse window 優化各有獨立證據；本輪沒有替每一項做單獨消融。自適應持續通道目前仍是設計計劃，不列為已交付能力。

初版「一千萬筆 Rust full read 約 0.150 s」仍是 [歷史實驗](KLINE_STORAGE_COMPARISON.md) 的紀錄：資料、接口、cache 與輸出邊界都不同，不能直接和此處的 Python 回測秒數相減。

## 可追溯性與限制

初版 executable 直接由 Git archive 的公開 commit 建置，沒有套入後來的 buffering、session 或 native 修正。獨立 Cargo target，Windows GNU release、locked/offline；舊 exe SHA256：`d9cb787fa7f8ad7a452501ebd1cc28283a280967171ad8e2700b2f800d2fafe8`。目前 native SHA256：`69781ff8ebbcdcab364642b2b3aaff0e3ca76aad51fa7693281dc297eaa9d4b1`。

使用隔離的同格式 store 副本，保留 current Reader 所需的 persistent ownership lock 檔。初版不提供目前的 read-only／ownership 保證，因此沒有讓它直接操作原始 benchmark store。比對前後，副本所有原有資料與 metadata SHA256 不變。初次複製漏掉該 lock 檔造成的 native 初始化失敗保留；補齊後預驗證通過才開始正式五輪，不混入失敗樣本。

OS cache 未受控，預驗證已讀取資料，不能稱為 cold disk。RSS 與程序樹 RSS 另存於 raw samples；子程序觀測為每 5 ms 取樣下界，不能把 parent-only RSS 當成初版全部成本。

後續發布僅在 runner 內補一個空行通過 Ruff；測量時的 exact bytes 與發布版 AST 等價核對一併保存。source manifest 對應實測檔，不因格式整理覆寫舊 hash。

[摘要](evidence/2026-10-22/version-comparison/summary.json)｜[原始程序、建置／驗證記錄與初版源碼](evidence/2026-10-22/version-comparison/experiment.zip)｜[實測 harness](evidence/2026-10-22/version-comparison/measured-source.zip)｜[驗證](evidence/2026-10-22/version-comparison/audit.json)

重跑命令與限制見 [backtest README](../external-tests/backtest/README.md)。這份對照與 [全年五後端回測](BACKTEST_RESULTS.md) 各自保留原始樣本，不拼成單一排名。

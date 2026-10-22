# Shared Reader ownership

FastK 的外部 matrix 使用端現在於主執行緒建立一個 native Reader，交給所有
outer workers 共用；每個 query 仍走原本公開接口，全部 workers 結束後才 close。
Rust 消費端用單一 `Arc<FastKStore>` 達成相同生命週期。
不用新增引擎接口，也不改 Reader 的回傳格式或 native binding。

SQLite 等 adapter 保留 thread-owned connection，開啟與關閉都在 worker 內。
結果增加 `reader_ownership` benchmark metadata，清楚揭露所有權差異。
先前每 worker 一個 FastK Reader 的歷史結果保留，不能與新結果無標示混合。

worker 開啟失敗或讀取失敗會 abort barrier；父執行緒失敗也會解除 worker 等待。
共用 Reader 在 pool join 後才關閉，沒有 retry/replay 路徑。
四個獨立 consumer 測試涵蓋 shared close-once、SQL thread ownership、read failure
不重播及 worker open failure。測試 adapter 只存在於外部測試。

## 已驗證範圍

- 公開 Rust API 建立 240 個檔案的暫存 fixture，未 warm 即開始並行查詢。
  1/4/8 workers 各做 12 waves、57,600 次完整 record 比對；Windows GNU、WSL2 均通過。
- WSL2 追加 120 waves，各 576,000 次查詢、合計 1,728,000 次內容比對。
  每 wave 完成後只計此 store root 的 FD，36 次初始樣本及追加樣本均為 65；
  最後一個 Arc 釋放後為 0。另追加每組 1,200 waves 的 sustained run，
  約 63 秒完成合計 17,280,000 次查詢比對，3,600 個 quiescent FD 樣本均為 65，
  各組最後 owner 釋放後均回到 0。
- Windows GNU 同樣追加每組 120 waves，約 64 秒完成合計 1,728,000 次比對；
  lease 與 owned records 的斷言均通過。此 run 沒有 Windows handle 計數。
- reader 的最後一個 Arc 存活時，實際 append 取得寫鎖失敗；drop 最後 owner 後
  同一 append 成功。回傳的 owned records 在 store drop 後仍然有效。
  Rust `FastKStore::open` 本身不是持續 writer lease，禁止的是實際 mutation，
  不應用「open 必須失敗」作為判準。
- 更新核心的 CPython 3.12 Windows GNU／Linux wheel，各通過 9 個既有 native tests，
  包括 int64/layout/readonly、陣列存活、close race、共享讀取與截斷資料失敗。
- 兩個環境的實際 shared-Reader matrix，在 1/4/8 worker 配置各完整驗證原本
  1,430 筆查詢；每環境 4,290 個計時結果全部通過 canonical validation。
  這是 consumer 正確性驗證，不是新一次五後端效能排名。

Windows fixture 的 root FD 欄位仍為 null，另有下述 Windows API handle 計數補測。
`first_wave_ns` 包含 thread 建立與 20 個 pass，並非單筆首次讀取延遲；
本輪不拿它比較平台效能。有限壓力測試也不代表數日運行保證。

## Windows GNU handle 補測

更新後的獨立測試以 `GetProcessHandleCount` 計算整個程序的 kernel handles。
1/4/8 workers 各讀取 576,000 次、120 waves，約 90.70 秒完成合計 1,728,000 次內容比對。
三組程序基線都是 69，每輪 workers 完成並 join 後都是 134（固定增加 65），
各組最後一個 Arc 釋放後都回到 69；360 個 quiescent 樣本沒有累積增長。
writer 在 reader 存活期間仍被拒絕，最後 owner 釋放後可寫入，owned records 保持有效。

這是整個程序的 handle 計數，不是 Linux 的 root-filtered FD 計數，也不是讀取中的瞬時峰值。
單一測試程序沒有其他並行測試；計數的穩定性與釋放後回到基線共同支持本次觀察窗內的回收行為。
僅獨立測試增加 Windows API dev dependency，產品引擎與 native binding 來源保持原樣。
原始記錄與來源見 [Windows handles evidence](evidence/2026-10-21/windows-handles/README.md)。

## 重跑

```text
cargo test --release --locked --manifest-path external-tests/scaling-profile/Cargo.toml --test shared_lifetime -- --nocapture
python -m unittest discover -s external-tests -p test_matrix_lifecycle.py -v
python -m unittest discover -s external-tests -p test_native.py -v
```

Rust fixture 可設定測試專用環境變數 `FASTK_LIFETIME_WAVES` 增加 waves，
每個 worker 配置的查詢數為 waves × 20 × 240；這不進入引擎配置。
執行 native 測試前必須重建並安裝目前核心的 wheel。

原生 Linux、Windows MSVC 依使用者安排留待有環境時驗證。
Record→word Vec 的額外配置／複製已另行移除，見 [native handoff 結果](NATIVE_HANDOFF_RESULTS.md)。
接口、磁碟格式與 CLI timeout 未變。

原始記錄與來源封存見 [shared-reader evidence](evidence/2026-10-21/shared-reader/README.md)。

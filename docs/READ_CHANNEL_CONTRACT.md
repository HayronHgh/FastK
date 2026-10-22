# 持續讀取通道：接口與資源契約草案

2026-10-22。這是 [設計計劃](ADAPTIVE_READ_CHANNEL_PLAN.md) 的候選契約，供實作前評估；以下接口與狀態均尚未交付，不表示 P0–P3 已完成。

## 範圍與接口

第一版綁定一個經完整驗證的 sealed snapshot，支援 Kline／Scalar range、一個 Rust 背景 IO worker。既有 Reader／Writer、CLI、格式、file-cache 預設 64 與同步 native 的 timeout 語意不變。Snapshot 驗證算入初始化，不把驗證後的暖資料標為磁碟冷讀。

Rust 使用 ReadChannel、ReadRequest、ReadTicket、ReadBatch；Python 使用 ReadChannel 的 submit_kline_range／submit_scalar_range，回傳 ReadTicket。Channel 提供 close、context manager 與資源 stats；Ticket 提供 result(timeout=None)、cancel、state。新功能的錯誤保留明確 code，不改舊 FastKError code。

Ticket 為一次性交付：第一次成功 result 轉移結果，後續 result 回 CONSUMED，不重播、不重新讀取、不再把整份資料留在 Ticket。Timeout 僅停止等待，不更動原請求；cancel 是另一個明確操作。多個呼叫同時 result，只有一個能取得交付。已交付結果仍可建立多個 readonly view，各 view 保留同一 allocation owner。

## 狀態與資源

- Queued → Loading → Ready → Delivered，另有 Failed／Cancelled。只有尚未交付的 queued/loading/ready 占 pending 名額。Fail／cancel／deliver 各釋放一次名額。
- 佇列滿立即回 QUEUE_FULL；超過單請求可容納的預算回 MEMORY_LIMIT，並列出保守估計所需空間；現有配置／預留暫時佔用則回 BUDGET_FULL。不可排入永遠不可能完成的工作，拒絕也不在內部自動重試。
- Result 要求本身會把該請求提升為 demand；不把預讀的等待放在 demand 前面。IO 與資料銷毀不持有排程鎖。
- 一個 Loading 區間只執行一次。單一 worker 自然不會同時讀同一 span；後續需求以 Ready／Loading 覆蓋扣除缺口，不因 cancel 一個訂閱而重播仍有其他需求的資料。
- close 停止 submit，取消未交付需求、喚醒等待者，等待活動 IO 收尾與 Reader／lease 關閉。重複及並發 close 都必須等到清理完成；交付出去的資料不受影響。
- 不在持有 Reader／store lease 時等 queue、等消費或等 budget。處理一個有限工作組後關 Reader，連續 submit 也不能變成永久讀鎖。
- Drop 未交付 Ticket 視為取消自己的需求；不因此取消其他訂閱者。Channel drop 執行 close；禁止留下 detached worker。

## 記憶體與 ownership

max_bytes 管理通道可控制且已知的 backing allocations、在途預留、組裝輸出、scratch 與有界內部 metadata；不是整個 Python／allocator／OS RSS 上限。Pinned buffer 直到最後 owner drop 才歸還，淘汰索引或丟棄 Ticket 不代表實體記憶體已釋放。

任何會依資料大小配置的工作先取得預留；IO 與組裝完成時，將預留轉為 actual capacity 計帳，多餘部分立即歸還。同一 allocation 多個 view 只計一次。部分 view 保留原大 allocation 的成本完整計入，不做隱藏 shrink／搬移。單一連續輸出可共享 owner；跨多個 buffers 組裝另預留一次輸出，不宣稱全程零複製。

第一版建議 submit 接受請求時即保留保守資料／組裝額度，防止 speculative Ready 結果耗盡後續 demand 的空間。代價是定位前可能高估，造成提早背壓；定位與共用後立即退還剩餘。這項取捨須在同預算實驗中揭露，不能將預留、實際 backing bytes 與 RSS 混成一個數字。

P0 的初始化與 metadata 預留必須有可檢查的上界，不能先全檔驗證／解析後才假裝從零開始計帳。若現有 core helper 的暫存配置無法由外層上界安全涵蓋，先補受控路徑或明確拒絕不足預算；不得默默排除以宣稱符合計劃。計帳測試須同時覆蓋 IO 暫存、Ready、consumer pinned 與 close 後存活的 owner。

## 驗收證據

P0：上述契約與固定實驗 trace。P1：有界通道、一次性交付、cancel/close/timeout 與 ownership 測試。P2：部分重疊只讀缺口、預讀窗口自適應、預算及版本隔離。P3：同預算的同步／固定預讀／自適應完整 wall time，另行 RW 歸因；不得用 sleep 代替確定性運算，未加探針主量測與探針結果分開。

硬預算、失敗不重播、close 完成與資料正確性任何一項未過，不能以局部加速視為整體完成。沒有核心瓶頸證據的候選優化不實作，保留負結果與所有限制。

# 持續讀取通道與自適應資料保留計劃

日期：2026-10-22。狀態：設計提案，待評估；本文件不代表功能已實作或效能已改善。

目標是讓實驗 A 運算時，背景準備 B、C、D 的資料，重用重疊範圍，縮短整批實驗等待資料的時間。持續存在的是接收需求與排程的通道，不是永遠打開的 Reader、讀鎖或無限增長的 cache。

「預先寫入快取」指讀入記憶體，不是背景 append／修改持久化資料；真正 write、sync 與 commit 的成本另由 RW 歸因處理。

這次只制定計劃。產品預設 file cache=64、既有 Rust／Python／CLI 接口、int64 精度、NumPy 格式、timeout 與失敗不重播行為維持。新的通道屬於後續新增能力，不把 cache policy、provider trait、可替換 backend 等擴充框架預先放入核心。

評估重點是三個決策：持續需求通道與短生命週期 Reader 分開；以已知未來需求的唯一資料量決定保留；先拆清 RW 探針成本再改核心。候選狀態、取消、關閉與 ownership 的逐項規則見 [接口與資源契約草案](READ_CHANNEL_CONTRACT.md)。這些名稱和規則是待評估設計，還不是可使用的產品接口。

## 1. 現有證據與可以沿用的部分

| 已確認事項 | 設計上的意義 |
| --- | --- |
| 同 240 月檔重播，FastK 64／128／256 的 p50 為 69.8／11.6／7.1 µs，p95 為 89.955／78.285／10.4 µs | Handle 保留策略能影響熱讀尾部，但不是整批研究流程的成功指標 |
| 對應重新開檔 1,056／671／0 次；header／sparse index 都已暖 | 新系統應減少重複資料準備，不能把重開檔歸因為底層格式必然較慢 |
| Fresh Reader 每 query 的完整週期約 0.364／0.356／0.370 ms | 只擴大 file cache 不會改善沒有跨請求重用的流程 |
| Reader 關閉後 NumPy 結果仍有效，file handle 回到 baseline | 資料 ownership 可以和檔案／讀取 lease 的壽命分開 |
| 大範圍已有 find_chunks_for_range → plan_range → ReadSpan → read_spans | 沿用既有定位、邊界查找與連續輸出，不重做一套讀取引擎 |
| IO 已使用明確 offset；cache 淘汰的 drop 已放在 cache lock 外 | 不把既有能力重新包裝成優化，也不先換成 lock-free 結構 |

來源：[容量實驗](CACHE_CAPACITY_RESULTS.md)、[完整開讀關實驗](LAZY_READ_RESULTS.md)、[原生接口](NATIVE_ENGINE.md)。容量實驗是 70 個未加探針的 Python 程序與 36 個獨立歸因 Rust 程序，不是實際回測或實體磁碟 cold-cache 證據。

第一版資料範圍限現有 native Kline／Scalar range。交易撮合、策略狀態、跨月帶倉與實驗依賴由上層管理。只有依賴已確定的讀取需求才能提前提交，不能假設未來時間窗口一定獨立或一定不變。

## 2. 通道的責任與候選使用方式

上層提供標的、series、timeframe、範圍與需求順序，通道負責排隊、合併缺少的區間、背景讀取及交付。引擎不推測策略，也無法從 A 的範圍自行知道 B 的需求。

候選語意如下，名稱是待評估草案，不是現有可呼叫 API：

```text
channel = ReadChannel(snapshot_root, max_bytes, max_pending)
a = channel.submit(range_A)
b = channel.submit(range_B)
c = channel.submit(range_C)

bars_a = a.result()          # readonly NumPy，格式沿用現有 range 結果
run_experiment_A(bars_a)    # 此時背景可以準備 B／C
release(bars_a)             # 表示上層不再持有資料；實際生命週期由 ownership 決定
bars_b = b.result()
...
channel.close()
```

`release` 只是示意不再持有結果，不擬新增一個能強制釋放其他引用的函式。Rust／Python 以各自 ownership／引用生命週期釋放。既有 Reader 呼叫繼續可用，不透過它偷偷建立全域排程器。

第一版只需要三個公開配置概念：綁定資料來源、記憶體上限、未交付請求上限。不要先公開分數權重、cache 算法、worker provider 或大量調參選項。

### 通道是持續可用，不是無限接受

- `submit` 在通道有效期間可持續呼叫；佇列滿時明確拒絕並回報 queue full，不默默丟棄、不無限堆積，也不讓 producer 無期限卡住。
- 請求狀態：Queued → Loading → Ready → Delivered；另有 Failed／Cancelled。Ticket 本身已被上層保留的輕量物件由上層負責，不能因此允許通道內部無限存放 terminal history。
- `max_pending` 涵蓋 queued、loading、ready 未交付的需求。結果交付後不再占 pending 名額，但其記憶體仍依實際 owner 計帳。
- 使用者實際等待某 Ticket 時，它成為 demand，優先於尚未需要的預讀。IO 可改排程，回傳內容、順序與範圍語意不能被改寫。
- 新 Ticket 的 `result(timeout=None)` 僅限制等待結果的時間；不宣稱能強制中止同步 Rust IO，也不默默取消、重開 Reader 或重播請求。timeout 後 Ticket 仍可能 Loading／Ready，狀態必須可判斷。既有同步 Reader 不因此取得 hard timeout；候選型別命名在實作前凍結。
- 建議 Ticket 一次性交付：成功取走後即為 Delivered，重取回報 CONSUMED；不讓輕量 Ticket 永久保留整份結果。timeout 不消耗這次交付，並發取用只能一個成功。
- Queue／cache／in-flight 狀態在短臨界區內更新，磁碟 IO、NumPy 建立、結果釋放與等待均不持有排程鎖。

## 3. 不讓通道壽命變成讀鎖壽命

目前 `Reader` 透過 `RwLock<Option<FastKStore>>` 保護 close 與活動讀取；read-only store 在存活期間持有 shared OS lease，writer／recovery 與它互斥。不能把同一個 Reader 從通道建立一直留到所有 backtest 結束。

背景只在一個有界的讀取工作組內建立 Reader／store：

```mermaid
flowchart LR
  Q[持續需求佇列] --> P[找出重疊與缺少區間]
  P --> R[預留記憶體與當批資源]
  R --> I[開 Reader 並讀取必要 chunks]
  I --> C[關 Reader 與釋放讀取 lease]
  C --> B[Ready 的不可變 buffer]
  B --> A[上層運算並最終釋放引用]
  A --> R
```

不要因佇列持續有資料就無限延長工作組。可將接下來已決定、能在目前資源預算完成的需求合併，完成當批 IO 即關閉；等待新需求、等待消費者或等待記憶體時不持有 store lease。

FD 預算和資料 cache 的 byte 預算分開管理。`ReadSpan` 的 Arc<File> 可以使檔案在 cache 淘汰後仍存活，所以單看 cache.len 不能當成程序 FD 上限。大規劃按有界 chunk 窗口開檔／執行／釋放；沿用輸出 offsets，不預先持有所有未執行月份的 handles。

第一版從一個背景 IO worker 開始，核心 read_threads=1。既有 4 MiB 並行門檻、1 MiB task 粒度保留，不同時疊加多個外層 worker 與每個 Reader 的內層 pool。是否擴到兩個 IO worker，依端到端與記憶體帶寬實測決定。

## 4. 快取內容、重疊與 ownership

### 儲存可運算的資料，而非只增加 file handles

第一版以「chunk 內已完整讀出的連續 record 區間」對應一份不可變 backing buffer。每個 chunk 維護有界的區間索引；cache 項目不是完整 experiment result，也不把理論月份筆數視為實際列數。

已知 range 先透過 metadata／sparse index 決定實際 record 邊界。公有接口目前是 inclusive end，內部可用半開 record-index 區間，不用 `end_ts + 1`，避免 i64 邊界與時間缺口錯誤。不得以時間差除 timeframe 直接當作檔案 row offset。

對新需求先從要求區間扣除 Ready 與 Loading 覆蓋：

1. Ready 的交集直接引用既有資料。
2. Loading 的交集訂閱同一工作，不再發第二次讀取。
3. 剩餘缺口建立唯一 loading 記錄，才開始 IO。
4. 同 chunk 相鄰缺口可以合併一次連續讀取，但必須先預留實際配置量，並記錄讀取放大量；不為少量短查詢一律讀整月。
5. 已完整讀取且查無資料的區間可保留有界的空覆蓋資訊；不得把「沒在 cache」誤當成資料不存在。

例如 A 讀 1–6 月，B 讀 3–8 月：B 引用 3–6 月既有範圍，只讀 7–8 月缺口。若 A 只讀了 3 月的一部分，就只能重用該部分；不能因相同 chunk 就宣稱整個月份已載入。

### 回傳格式維持，但不要假裝 ownership 免費

現有 native 將一份 Vec allocation 轉交 NumPy；不能再把同一份 allocation 當作另一個 Rust Vec 擁有，否則是 double free。新增通道需要一個明確、不可變、可共用的 allocation owner，Python view 保留 owner，最後引用釋放後才扣回計帳。

單一連續範圍可在 layout 允許時交付 readonly view；跨多份 backing buffers 的結果，若仍要求現有 C-contiguous structured array，通常需要一次組裝複製。該輸出空間要先預留、計入峰值；不能宣稱所有重疊或所有 NumPy 回傳都是 zero-copy。既有 Reader 不因此更換回傳格式或 ownership 實作。

部分 view 即使只剩一小段，仍可能保留整份 backing allocation。第一版不加入背景壓縮／搬移框架，以實際 allocation 作淘汰及計帳單位。先測是否因大 allocation 的小切片造成明顯保留放大，再決定是否值得額外複製。

## 5. 動態計算保留空間的算法

算法先利用已知需求，不先建立通用 LRU／LFU／學習模型組合。它同時決定「提前準備多遠」與「哪些已讀資料值得繼續保留」。

### 唯一配置量與硬預算

記帳對象是通道建立、仍存活的實際 allocation，包含上層拿走後仍持有的資料：

```text
accounted_bytes = unique_live_allocations
                + reserved_inflight_allocations
                + reserved_result_assembly
                + bounded_scratch_and_metadata

accounted_bytes <= max_bytes
```

同一 backing buffer 的多個 view 只計一次；淘汰索引項目後若 consumer 還持有 owner，仍屬 live bytes，不能假裝已回收。預留與實際配置以同一 accounting owner 轉移，避免重複計入或短暫漏計。這是已知配置量的上限，不保證 allocator／runtime／OS 的整個 RSS 都低於該數值。

依 manifest counts、邊界上限與 record size 估計 IO／輸出大小，必要時保守預留；定位後可以退還多餘預留。若單個結果連必要的組裝與 scratch 都超出 max_bytes，明確回報 request too large，不偷偷超額或改成分頁結果。先規劃出錯誤與必要 byte 數，不能排進一個永遠不會滿足的等待。

### 先保證能交付，再提高空間利用率

要防止的死結是：B／C 已 Ready 並佔滿空間，上層卻在等 A；若 A 尚無輸出額度，就可能誰也無法前進。第一版建議在接受請求時，一併保留該請求最壞情況的資料與組裝額度；已承諾給 A 的空間不能被預讀拿走。定位出實際 record 邊界、共用範圍及缺口後，再將多餘額度歸還，保留目標仍依唯一 allocation 計算。

這有明確代價：若暫以相交 chunks 的完整 counts 估上界，短範圍也可能被高估，多個高度重疊請求在尚未定位前會較早遭遇背壓。這是第一版需要測的成本，不把預留額度誤報成實際配置或快取命中。單個請求本身超過上限回 MEMORY_LIMIT；只是現有結果／預留佔用空間則回 BUDGET_FULL；pending 名額不足才回 QUEUE_FULL。拒絕即未建立工作，不在內部排隊重試或重播。

初始化驗證、metadata 解碼與 sparse-index 暫存也須有上界。若現有 helper 無法安全涵蓋，P1 先補受控路徑或拒絕不足預算，不能先完成全檔配置後才開始記帳。max_bytes 是受控配置預算，包含保守預留，並非程序 RSS 的精確上限。

### 每次排程決策

1. 保護目前 demand、活動 IO 與上層仍持有的 allocations。先清掉取消、已過期或不再有未來用途的可回收資料。
2. 從已提交需求的近期前綴選一個 lookahead 窗口。初始僅準備下一個需求；沒有後續需求就不猜資料。
3. 在相同版本下，計算此窗口與 Ready／Loading 的區間聯集。保留目標為這個「唯一工作集＋結果組裝及 scratch」，不能逐 request 加總重複區間，也不是月份數乘固定檔案數。
4. 若超過預算，縮短窗口、移除不急迫的預讀，必要時停止背景 admission；不能淘汰 consumer 正在使用的資料。保留 demand 所需空間與工作名額，避免 speculative Ready 結果佔滿後 demand 無法進行。
5. 可回收 allocation 先按「下一次已知使用位置」排序：較遠者先釋放。同距離再比較每 resident byte 可避免的重讀成本；沒有已知後續用途時預設釋放，不單憑近期命中就長期保留。
6. 同一 allocation 覆蓋多個區間時，以整份 allocation 的下一次用途與實際可釋放 bytes 決策，不能只淘汰一個 view 就扣掉整塊記憶體。

```text
target_bytes = active_and_pinned_bytes
             + unique_future_bytes_excluding_active_and_pinned(selected_future_window)
             + required_assembly_and_scratch
```

target 是願意保留的工作集量，不是預先 allocate 的空白大 buffer；只在需要 IO／組裝前配置。當未來窗口縮小、實驗取消或資料用完，target 下降，可回收項目立即釋放。被 consumer pin 的資料只能等引用真正結束，不能因 target 下調而失效。

上述是互斥分組後的 bytes 計算；Ready／Loading 與活動結果若共享 allocation，只能歸入其中一組，不能重複相加。

### 用實際回饋調整窗口，避免一直擴張

收集 IO 準備時間、結果等候時間、Ready 到被取用的閒置時間、未使用即丟棄的預讀 bytes、記憶體及 IO 利用率。第一版採串行消費的實驗序列；可觀察相鄰結果取用的間隔作估計，但它含運算、使用者等待與其他工作，不稱為引擎直接知道 backtest 執行時間。測試 harness 另外直接計時真正的運算。

- 需求因資料尚未 ready 而等待，且 IO 未持續飽和、記憶體仍有餘量、預讀浪費低：逐步增加近期窗口。
- Ready 長時間未取用、預讀多次未用、consumer 放慢或記憶體壓力增加：縮小窗口，釋放沒有近期待用的資料。
- IO 已持續滿載且資料消耗速度更快：不要把擴大窗口當解法。先減少重複讀取／組裝，或接受可測的等待；任何有限 cache 都不能無限補足吞吐不足。
- 使用平滑的近期觀察與增減遲滯，每次以一個需求或一個有界工作組調整，避免每筆 record 重新算分與容量抖動。初始常數在 P0 固定並公開，正式比較前凍結，不跑到想要的結果才改。

OS available-memory 取樣只能使 admission 更保守，不能默默突破使用者 max_bytes；第一版不加入系統級記憶體自動擴張器。多個獨立 channel 的額度由上層合計管理，不建立隱藏全域資料 cache。

## 6. 資料版本、取消與失敗規則

- 跨讀取工作組保留資料，第一版綁定經驗證的 sealed snapshot digest。Key 至少含 source identity／snapshot digest、series、schema、chunk identity／generation 與 record 區間，不能只用可變 DatasetRef 名稱。
- 現有 `verify_snapshot` 會讀取完整 series inventory 並驗證每個檔案。若初始化使用它，耗時必須列入 channel 建立與整批 wall time；驗證也會暖 OS cache，後面的 read 不能稱為 cold disk。
- 在合作的引擎寫入模型下，sealed root 拒絕寫入；正常使用不得原地替換已綁定的 snapshot。任意外部程式直接修改原始檔不是 cache 能自動保證安全的模型，不能省略原有 corruption／truncation 檢查。
- Mutable／latest root 不在第一版跨工作組保留範圍內：明確拒絕此新通道模式，既有 Reader／Writer 仍可使用。未來若支援，需要先定義 epoch／失效與一致性，不自動弱化成可能混版的快取。
- 一份 Loading span 只有一次 IO；不同需求可共同訂閱。取消一個需求只移除自己的訂閱，不能取消仍被其他需求使用的工作。
- 取消 queued 工作可立即釋放預留；同步 IO 已開始則完成安全收尾，無使用者時丟棄結果，不宣稱即時中斷。cancel 與 ready／deliver 同時發生時只能有一個明確終態，不發布部分結果。
- IO／corruption／STORE_BUSY 等失敗傳給受影響的 Tickets，不自動重試、不重播整個請求、不在背景改寫或修復 store。其他不相依需求可以繼續。
- close 停止接受新需求、取消未開始項目、喚醒等待者，等待活動 IO 安全結束與 Reader 關閉，再清掉通道自持資料。上層已取得的陣列仍有效。close 的等待與 resource release 必須可驗證；不得留下背景 worker 或永久讀鎖。

## 7. 現有 RW 探針與路徑的檢查結果

這裡分開看 read/write IO、Rust RwLock／Mutex 與 OS read/write lease，避免把名稱相似的成本混成同一個數字。

| 現有觀察／限制 | 窄優化候選 | 驗證方法 |
| --- | --- | --- |
| native 設 Metrics Off，但 cache miss、chunk lookup、manifest 等呼叫點仍有先建立 Instant 再由記錄函式檢查 level 的路徑 | 在需要 Detailed 時才啟用計時，或在工作組入口決定一次測量模式；不先改 counter 契約 | 同一 binary 的 Off／Basic／Detailed 對照與現有輸出契約，分小查詢／bulk；不能只測空迴圈 |
| 探針使用全域原子與 Instant，歸因程序和主計時不同 | 歸因時以 worker-local 累計後合併，減少共享 cache-line；先測 probe 自身成本，不加進產品熱路徑 | 保留未加探針主量測，counter 總數與 byte 數精確對照，增加 worker 後再看競爭 |
| cache-path span 包含開檔與淘汰 drop；現有 64 重播其餘 cache 管理約 0.35 µs／query | 先維持短鎖，不能把小 residual 當作主要瓶頸，也不先換 lock-free cache | 歸因另分 lock acquisition／hold time，保留嵌套關係，不重複加總 |
| 既有 miss 在鎖外 File::open 後才取得寫鎖重查，多個 concurrent miss 可能重複開同檔 | 新通道的 in-flight span 合併先解決重複資料讀取；是否也要修改舊 handle cache，另由多執行緒證據決定 | 同 chunk 競爭、不同 chunk 競爭，計 actual open 次數、鎖等待與 p99；單執行緒數據不能代答 |
| header cache 為 Mutex<HashMap>；file cache 為 RwLock＋second chance；IO cursor 已局部化 | 有競爭證據才改 header cache 或分片；沿用明確 offset 與既有 chunk 計畫 | 不同 worker／同系列與跨系列對照，確認不是 file-open 影響 |
| native close 需取得 store write guard；drop 的時機會影響 close 成本 | 先確認 destructor 是否在 guard 範圍、實際等待多少。若移出鎖，必須保留並發 close 都等到清理完成的語意，不能只讓第二個 close 提早返回 | close／read race、雙 close、錯誤後 close、handles 與 lease 全部釋放；不能為降低 lock hold 破壞關閉完成保證 |
| 目前容量 probe 量的是讀取，沒有持久化 write／flush／sync／rename／manifest commit 拆解 | 只有要談「寫入比 backtest 快」時，另設 write 歸因；不能以 memcpy／write_all 等同耐久寫入 | 完整寫入公開接口計時，另拆 encode、write、flush、sync、publish／parent sync、鎖與 recovery marker；不省略 durability 換數字 |
| snapshot 完整驗證讀全檔、read-only open 做 root invalidation 與 recovery 檢查 | 合併有界工作組可攤提 open，保留正確性檢查；不要直接移除驗證、鎖或 pending marker 判斷 | 初始化、每工作組 open 與完整 wall time 都列出，任何新的可信驗證快取另立設計 |

probe 目前的 open／IO／eviction-drop ns 是 Rust 診斷時間，不是純 syscall profiler，也不能直接從 Python p50 相減。需要補的主要是 queue wait、budget wait、ready wait、allocation／組裝、lock wait／hold 及 write durability。新增測量先放 external harness；只在控制器實際需要時加入少量產品計數，不為測試暴露公開方法。

若考慮在工作組入口只讀一次 metrics level，要先確認動態切換 level 的既有契約；不能為少一次 atomic load，默默改成整批延後生效。優先評估語意不變的條件計時，再決定是否值得進一步修改。

## 8. 實作階段與完成條件

### P0：凍結需求、計帳與量測契約

交付：候選通道 API／狀態機定稿、snapshot 規則、每類 allocation 的 ownership 表，以及固定實驗 trace。選定一個可重現的確定性運算；先量出 compute 與資料準備速度，不能預設 IO 永遠較快。比對既有 Off／Detailed 探針成本，凍結控制器初始參數與量測邊界。

完成條件：能逐項說明 submit、queue full、result timeout、cancel、failure、close、pinned memory 與 oversized request 的語意；沒有未計入的組裝配置或未定義版本混用。

### P1：持續通道與有界預讀

先完成單背景 worker、bounded queue／bytes reservation、Tickets、close／cancel、短生命週期 Reader。用既有 range／ReadSpan 讀取，不加入自適應淘汰或多層抽象。需求未來已知時以固定小窗口預讀，先證明運算與 IO 可以重疊。

完成條件：長時間持續提交不累積 FD、執行緒、pending 記錄或未計帳 allocation；close 後等待者不掛住，資料格式與失敗規則成立。序列模式的正確結果與新通道一致。

### P2：區間重用與動態保留

加入同版本 Ready／Loading 區間扣除、唯一 in-flight、已知下一次用途排序及窗口調整。先處理包含、部分重疊、相鄰、完全不重疊、空區間與跨月；多 buffer 組裝只做必要的一次複製，實際 copies／bytes 可觀察。

完成條件：重疊資料沒有重複 IO、同一 in-flight 不重複開讀；cancel 不誤傷其他訂閱；實際 backing bytes 永不超過已宣告配置預算。縮小 target 不會使已回傳資料失效。

### P3：只做有證據的核心／探針優化

依 P0–P2 的殘餘成本，選擇 metrics 時鐘 gate、重複開檔合併、metadata／鎖或組裝路徑中的窄修正。每次只有一個可解釋變更，不把 mmap、無鎖 queue、新格式與並行度一起改。維持既有產品 file-cache 預設，任何預設調整另有資源證據與決策。

完成條件：未加探針的整批時間改善，且等待、浪費、記憶體、釋放或 correctness 沒有被移出計時來美化結果；沒有新證據的項目不實作。

## 9. 驗證設計：目標是整批完成時間

三個比較組：既有同步逐批載入、同一通道固定小窗口預讀、自適應重疊保留。三組使用相同資料版本、需求 trace、輸出 layout、確定性運算與 IO 執行緒預算；固定窗口與自適應共享同一 max_bytes／max_pending。不能再用可保留更多資源的組別直接宣稱算法較好。

三組也遵守相同 snapshot 驗證規則，初始化成本列入各自整批時間；不得只向某一組收取全檔驗證成本，或讓它先把其他組即將使用的 OS cache 暖好卻標成冷讀。若另做已驗證來源的 resident-process 實驗，單獨標示，不替代完整初始化結果。

主要指標：從通道／Reader 初始化到所有運算、結果釋放及通道關閉的整批 wall time；另列資料首次可運算時間、compute-ready 等待比例、實際計算時間、最後清理時間與 p95／p99 request wait。每個計時輸出仍做完整 canonical／packed-byte 檢查，驗證放在約定的 timer 外但不能改變對照的工作順序。

資源與效率：實際 bytes／IO 次數、重複讀取 bytes、未用預讀 bytes、組裝 copy bytes、allocation 高水位、RSS／private、active FD、IO worker 數。OS page cache 與資料 cache 分別標記；buffer 被淘汰不代表 OS 回收檔案頁，allocator 保留 RSS 也不必然是活物件洩漏。

必要 workloads：

- 原本 1,430 短查詢與四個 bulk cases，作為接口與局部回歸；不得代替 pipeline 主要結果。
- 已知按月／跨標的需求：完全重疊、部分重疊、無重疊、工作集超過預算；同一資料 trace 以實際唯一 bytes 描述，不捏造命中比例。
- Compute 比 IO 慢、接近與更快的三種情境。使用同一真實確定性運算的不同工作量，兩組執行相同工作，不用 sleep 假裝取得回測加速。真實 strategy backtest 若尚未接入，就明確稱為研究計算管線。
- Consumer 暫停、持續快速 submit、取用順序改變、反覆 cancel、共享 span 某訂閱失敗、result timeout、close race、上層長期持有 output、單請求超額。
- 缺口、跨月邊界、inclusive end、i64 邊界、不同 snapshot／generation、截斷／損壞與 STORE_BUSY。版本不同即不可共享，不因地址／檔名相同而命中。
- 長時間多輪運行後，所有 channel-owned allocations、FD、讀鎖與 worker 均釋放；pinned outputs 有意保留的 bytes 另列，最後 owner drop 後才要求歸零。

每設定至少五個獨立程序，順序隨機，保留全部樣本與失敗。主計時禁用詳細探針，歸因另跑。初始化的 snapshot 驗證、metadata、開檔、組裝、運算、關閉不能藏進免費 warmup。若執行 warm-cache 版本就明確標示；真正 cold-disk 實驗要另有 OS cache／實際 IO 證據，不能只換 Python process 就宣稱冷讀。

通過標準首先是語意與資源正確。效能只有在相同配置下整批 wall time 及等待有穩定收益，且沒有靠更多浪費讀取／更大記憶體換取未揭露成本，才視為成功。不預先承諾固定倍數或零等待；部分重疊、無重疊與吞吐不足的負結果同樣保留。

## 10. 檔案與邊界

沿用定位與輸出：`src/store_core/engine/store.rs`、`engine/parallel.rs`、`chunk/kline_reader.rs`、`chunk/scalar_reader.rs`。檔案／metadata cache 仍在 `chunk/cache.rs`，不把資料保留混入同一個 handle capacity。

後續新增通道實作應是一個直接服務這個需求的模組，內部持有 queue、區間與預算狀態，使用現有資料讀取。Native 綁定只處理同一型別與 ownership，不設 Python 專用查詢引擎。測量與故障 harness 留在 external-tests；確定需要跨語言通道 API 後才定公開名稱與最小方法集合。

本計劃不改網站內容、不啟動新 benchmark、不改源碼、鎖策略、檔案格式、CLI JSON 或產品預設。需要評估的決策已收斂為：先採單 worker／sealed snapshot／相同 NumPy 結果，以一個持續有界的需求通道配合可計帳的資料保留，按 P0→P1→P2→P3 驗證。

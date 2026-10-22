# FastK bulk 與 Linux 並行成本拆解（量測與方案，未實作優化）

2026-10-21。這次只新增隔離的診斷工具、量測資料與本報告；引擎、正式 native binding、CLI、舊 Client 與根目錄依賴檔均保持原樣，143 個既有來源檔案的 SHA256 與上一版來源封存一致。

主要發現是：bulk 交付多了一次 Record → i64 Vec 的配置與逐元素搬運；NumPy 接收 Vec 的所有權時已經沒有整批複製。Linux 多執行緒退步則有兩個來源：首次檔案描述符表擴充，以及小查詢反覆釋放／取回 GIL。兩者需要分開處理。

## 量測邊界與可信度

沿用原本四筆 bulk 查詢（127、4,077、437,115 筆 Kline，以及 437,115 筆 Scalar）與相同 1,430 筆短查詢。Windows / CPython 3.12.14 與 Linux WSL2 6.18.33.2 / glibc 2.36 / CPython 3.12.14，NumPy 2.3.5、PyArrow 26.0.0。Linux 資料複製到容器自身檔案系統；來源 manifest 與查詢內容保持一致。容器 CPU 沒有配額節流。

Windows 18 組、Linux 132 組正式診斷工作依序執行，編譯與其他效能工作不併跑。bulk 每組先做一筆 canonical 校驗與 32 次暖機，再量 30 次；三個獨立程序輪次合計每個案例 90 次。90 筆 bulk 不發布 p95/p99。短查詢每程序 1,430 筆、三輪；資料驗證與上一筆結果釋放在計時外。原始值、分布、每輪中位數與程序記憶體都有保留。

正式 wheel 是整體耗時的對照。隔離的 PyO3 診斷模組以相同 Rust 編譯設定及 86 個完全相同版本／checksum 的 registry 依賴，重現讀取、原本的 flat_map/collect、取回 GIL 與 NumPy 包裝步驟。計時與回傳診斷數字本身有開銷，不能把診斷鏡像當成新的產品速度，也不能把各段中位數相加當成另一個測試的總耗時。

## bulk：資料實際花在哪裡

以下分段在同一次診斷呼叫內量出，單位 ms。NumPy 包裝合計包含 dtype 查找、接收 Vec 所有權、readonly 與 structured view；核心包含查詢規劃、查檔與讀入 Record Vec。

| 環境 | 案例 | Rust 核心 | Record→word Vec | 取回 GIL | NumPy 包裝合計 | 診斷呼叫總計 |
|---|---|---|---|---|---|---|
| windows | bulk_kline | 4.4625 | 3.3971 | 0.0051 | 0.0572 | 7.8998 |
| windows | bulk_scalar | 1.0754 | 1.0106 | 0.0009 | 0.0286 | 2.1253 |
| linux | bulk_kline | 3.8364 | 7.0561 | 0.0021 | 0.0406 | 11.0363 |
| linux | bulk_scalar | 2.4858 | 2.1491 | 0.0008 | 0.0254 | 4.7191 |

正式 wheel 與未加分段計時的 Arrow adapter，均回傳相同 packed、readonly、具名 NumPy int64。以下是新一輪對照中位數，並未改寫先前報告的歷史數字。

| 環境 | 案例 | 正式 FastK ms | Arrow IPC ms |
|---|---|---|---|
| windows | small_kline | 0.1053 | 0.1301 |
| windows | medium_kline | 0.1429 | 0.1197 |
| windows | bulk_kline | 8.1924 | 3.9029 |
| windows | bulk_scalar | 2.1962 | 1.2929 |
| linux | small_kline | 0.0535 | 0.1137 |
| linux | medium_kline | 0.0990 | 0.1122 |
| linux | bulk_kline | 11.2351 | 2.1665 |
| linux | bulk_scalar | 4.6766 | 0.6866 |

獨立指標檢查在兩個平台確認，Record Vec 與產出的 word Vec 是不同 allocation；word Vec → NumPy base → structured view 則沿用同一塊記憶體。兩個 bulk word Vec 的最終容量正好等於長度，未觀察到需要優先解決的最終過度保留空間。這不是 allocator 呼叫次數追蹤，不能由容量反推每一個中間配置。

437,115 筆 Kline 每份 payload 約 20.01 MiB，Scalar 約 6.67 MiB。建立 word Vec 時，原 Record allocation 仍需存活到搬運完成；這是可以整段移除的額外工作與邏輯尖峰空間，並非 NumPy 在收到 Vec 後又複製一次。Linux Kline 分段呼叫的 minor faults 中位數為 6,830，說明配置／首次觸碰頁面也在成本內；不能全部稱為純記憶體頻寬成本。

另外用已在記憶體中的同內容 batch，分別量單純 Rust i64 slice → owned Vec，以及 NumPy structured-array copy，兩者都包含自己的配置。這些是隔離控制，輸入生命週期與快取狀態不同，不能拿來直接相減推算生產流程。

| 環境 | 案例 | Rust 配置＋直接複製 ms | NumPy structured copy 呼叫 ms |
|---|---|---|---|
| windows | bulk_kline | 2.2256 | 5.8007 |
| windows | bulk_scalar | 0.7435 | 1.9440 |
| linux | bulk_kline | 1.0901 | 3.9482 |
| linux | bulk_scalar | 0.3507 | 1.1955 |

Linux 的原 flat_map/collect 比直接 i64 複製控制昂貴得多。因此除了第二份 allocation，也有逐元素重組與釋放來源的成本；目前證據不足以把這一段細分成每種 allocator／CPU 指令的精確占比。優先移除整段搬運，比換另一個 Python 解析器或只調 reserve 更有依據。

## Arrow 為什麼仍然快

Arrow IPC adapter 保留 mmap 的 column views，用 timestamp 二分搜尋切片，最後只建立一次共同格式的 packed 輸出。資料頁的讀取／缺頁可能直到最後複製才發生，因此「map/search」不是完整資料讀取成本。下表把實際打包複製列在一起，沒有把 mmap 當成免費 I/O。

| 環境 | 案例 | 分區／map／搜尋 ms | 輸出配置 ms | columns→packed 複製 ms | 診斷呼叫 ms |
|---|---|---|---|---|---|
| windows | bulk_kline | 0.2130 | 0.0159 | 3.3969 | 3.6553 |
| windows | bulk_scalar | 0.2051 | 0.0193 | 0.9816 | 1.2226 |
| linux | bulk_kline | 0.1758 | 0.0132 | 1.8298 | 2.0946 |
| linux | bulk_scalar | 0.1614 | 0.0174 | 0.4118 | 0.6394 |

FastK 目前先由 pread 把資料放入 Record Vec，再由 binding 重組成 word Vec；Arrow 的暖讀路徑直接從映射欄位產出最後一份 packed buffer。兩者交付相同輸出，記憶體路徑的工作量卻不同。這輪結果無法支持「換成固定偏移或加更多執行緒就一定勝過 Arrow」；也沒有證據要求先換掉時間索引或磁碟格式。

程序記憶體是外部對實際 Python PID 每約 5 ms 採樣，包含匯入、暖機與驗證；短暫尖峰可能漏掉。Windows private 是 private commit，Linux private 是 USS，不能跨 OS 直接比較。診斷模組另有自身程式碼映射成本。

| 環境 | 完整 bulk 工作程序 | 峰值 RSS MiB | 峰值 private MiB |
|---|---|---|---|
| windows | native | 77.0 | 64.6 |
| windows | probe | 76.9 | 64.4 |
| windows | arrow | 104.3 | 57.6 |
| linux | native | 79.5 | 69.3 |
| linux | probe | 80.3 | 69.9 |
| linux | arrow | 154.1 | 144.0 |

## Linux：首次停頓與持續的 GIL 交接

原本五組各 286 筆的並行測試，每個 worker 新建自己的 reader，只預讀一筆。這同時量到新 handle 的檔案／metadata bootstrap 與暖讀。獨立 Rust 程序在同一批 handles 上重播完整 1,430 筆，第二遍的吞吐可隨執行緒上升；第一遍的彙總數字卻往下走。每遍的所有輸出均做 canonical 校驗。

| Rust workers | 第一遍 queries/s | 同 handles 第二遍 queries/s |
|---|---|---|
| 1 | 35,262 | 118,151 |
| 2 | 27,524 | 221,852 |
| 4 | 20,072 | 358,600 |
| 8 | 16,460 | 305,833 |

系統呼叫追蹤找到更具體的線索：openat 回傳 fd=64、128、256 時，出現約 26.2、27.7、25.5 ms 的停頓，相鄰其他開檔也一起等待。這與 [Linux 6.18 fdtable 擴充](https://github.com/torvalds/linux/blob/v6.18/fs/file.c)在共享描述符表擴充時等待 RCU／resize 的機制吻合。這個 kernel 內部原因是由原始碼、系統呼叫與控制實驗推論，沒有用 kernel probe 直接計時 synchronize_rcu。strace 會顯著干擾速度，追蹤下的吞吐沒有用於效能結論。

因果控制只在診斷程序的主執行緒預先擴充 FD 表，隨即關閉所有 dummy descriptors，再建立 reader 與 worker；資料／引擎快取仍沿用原本單筆預讀規則。下面是三輪配對程序的第一遍中位吞吐。預先擴表的工作在計時外，時間另有記錄；這項控制不代表正式實作，也不隱藏初始化成本。

| 路徑 | workers | 一般 FD 表 queries/s | 預擴 FD 表 queries/s | 一般首組 ms | 預擴首組 ms |
|---|---|---|---|---|---|
| rust | 1 | 32,533 | 94,171 | 33.6010 | 5.2326 |
| rust | 4 | 18,588 | 237,736 | 59.0038 | 1.8134 |
| rust | 8 | 13,341 | 232,632 | 78.7795 | 1.7006 |
| native | 1 | 23,126 | 55,648 | 44.3700 | 7.2464 |
| native | 4 | 9,930 | 19,086 | 69.6050 | 15.0998 |
| native | 8 | 9,093 | 18,186 | 77.5477 | 15.5876 |

每個獨立 handle 最多快取 64 個檔案，worker 越多就越容易在同一程序碰到多次 FD 表擴充。共用同一 Reader 可減少重複開檔；現有 positional IO 與共享讀鎖已支援這種用法。這是本次 WSL2 kernel 的明顯現象，不能外推成每一套 Linux 都有相同等待時間。

FD 暫態移開後，Python 的小查詢並行仍然退步。再對每個 handle 預先跑完所有 1,430 筆，保持正式 wheel 不變，結果如下。純 Rust 與 Python 為不同控制工作，不用兩者總時間相減來估算 FFI。

| workers | 正式 Python queries/s | 診斷核心 us | 診斷 flatten us | 診斷取回 GIL us | Reader 讀鎖 us | 診斷 voluntary switches |
|---|---|---|---|---|---|---|
| 1 | 56,912 | 10.856 | 0.141 | 0.040 | 0.020 | 27 |
| 2 | 30,084 | 14.914 | 0.180 | 0.360 | 0.020 | 692 |
| 4 | 19,458 | 24.137 | 0.220 | 131.763 | 0.020 | 1990 |
| 8 | 17,333 | 24.297 | 0.240 | 329.375 | 0.020 | 2392 |

四／八 worker 的 GIL 返回等待已超過核心讀取時間，讀鎖取得約 0.02 us。這個讀鎖數字不涵蓋核心內部每個 cache mutex，但純 Rust 暖讀與共享 handle 對照沒有支持先重設整個鎖架構。兩 worker 的 GIL 等待中位數很小、尾部與排程成本仍存在，不能把所有 workers 的退步都用同一個中位等待值解釋。

[PyO3 的 detach](https://pyo3.rs/main/parallelism.html)允許 Rust 期間其他 Python 執行緒執行，建立 NumPy 物件前仍要重新進入 Python。短讀每次釋放與取回 GIL，會增加排程與競爭；這與「Rust 沒有並行能力」是不同結論。診斷回傳數字也增加 GIL 內工作，因此這些分段是定位依據，正式 wheel 的整體對照才是產品結果。

另外在各自獨立的診斷程序把 [Python switch interval](https://docs.python.org/3.12/library/sys.html#sys.setswitchinterval)改為 1 ms／0.1 ms 做控制。它是全程序排程參數，結果沒有一致消除退步；不建議由函式庫偷偷修改。

| workers | 5 ms queries/s | 1 ms queries/s | 0.1 ms queries/s |
|---|---|---|---|
| 2 | 12,955 | 14,389 | 14,270 |
| 4 | 8,304 | 9,791 | 8,421 |
| 8 | 7,247 | 8,943 | 8,146 |

USS 採樣控制亦保留：Rust 暖讀 1 worker 無採樣／有採樣分別約 118,151／110,428 queries/s；8 worker 約 305,833／319,437。採樣有擾動，並非首次數十 ms 停頓的唯一來源。

大型單筆讀取的內部 pool 另有實際收益，但後面的 word Vec 搬運仍是序列工作，限制整體改善。

| 內部 workers | 診斷核心 ms | 診斷 flatten ms | 正式 wheel 總耗時 ms |
|---|---|---|---|
| 1 | 3.8364 | 7.0561 | 11.2351 |
| 2 | 2.8556 | 7.4799 | 11.4828 |
| 4 | 2.5199 | 7.4272 | 9.5413 |
| 8 | 2.2055 | 7.6934 | 9.4408 |

## 建議方案，待評估後才實作

**第一步：移除 Record → word Vec 的整段搬運。** 保持 Rust 公開讀取仍回傳 Vec<Record>，在 native adapter 的一個私有函式中，把已成功初始化的 allocation 交接成 Vec<i64>，沿用現在已無整批複製的 IntoPyArray 與 dtype view。可評估 [bytemuck 的 try_cast_vec](https://docs.rs/bytemuck/1.25.2/bytemuck/allocation/fn.try_cast_vec.html)：它檢查 alignment、長度與容量的位元組相容性，使用受約束的 NoUninit／AnyBitPattern 型別條件，而不是手寫一組公開 raw-pointer 接口。

只處理 KlineRecord（48 bytes、align 8）的 range／latest，以及 ScalarRecord（16 bytes、align 8）的 range，明確驗證欄位偏移、無 padding 與所有 i64 位元模式合法。核心不依賴 PyO3／NumPy；若選 bytemuck，核心只增加兩種固定 record 的表示保證，所有陣列轉接仍在 binding。predicate match 含 Option 值，維持既有轉換，不做任意型別 reinterpret。失敗只映射既有錯誤，不重讀資料，不重播任何寫入。

預期可移除的是 Windows Kline 約 3.40 ms、Linux 約 7.06 ms 所在的工作段，以及約一份 payload 的邏輯尖峰配置；這是可處理範圍，不是速度保證。移除後 allocator 行為會改變，不能直接用減法保證新延遲；Rust 核心仍有實際成本，未保證就會勝過 Arrow。現有 Reader／Writer 方法、參數、具名 readonly ndarray、int64 精度、結果順序與 close 後陣列生命週期均應保持。

**第二步：沿用同一 Reader 的生命週期，區分初始化與穩態。** 先使用現有共享 Reader 能力，減少每個 Python worker 各自複製一套 64 檔快取與 FD。外部測試分開列 open／首次覆蓋資料／同 handle 重播；Linux FD 預擴只留在診斷控制，正式函式庫不開一批 dummy files，也不新增全域 FD pool、預留未使用的 backend trait 或測試開關。共享 Reader 仍要保留 close 協調與 OS shared lease。

**第三步：按工作量使用既有並行界面。** 小查詢預設一個 Python 呼叫執行緒；研究程式可用既有 read_range 先取一個月，再對 ndarray 欄位運算，減少大量微小 FFI 往返。大型讀取則評估既有 threads=2/4/8 的有界內部 pool，先量第一步移除序列搬運後的收益。不新增 read_many、異步 scheduler、回測執行器或全域 GIL 調整。月份預讀可並行，策略消費仍依時間順序並攜帶跨月持倉。

NumPy dtype 查找／view 微調先延後：這輪 bulk 只占數十微秒，優先度低於額外配置／搬運。mmap 引擎化、縮窄 i64、再切更多工作執行緒，以及移除讀寫鎖，也不列入第一輪。

驗收應沿用四筆 bulk 與 1,430 短查詢、三輪配對程序、完整數值 checksum、RSS/private，以及 Windows/Linux CPython 3.11/3.12。確認 dtype/offset、空資料、非連續輸入、超過 2^53 與 i64 邊界、readonly、close/read 競爭與陣列獨立生命週期；既有 CLI pretty JSON、timeout、失敗不重播、故障恢復及讀寫排他測試均須維持。Rust 核心另外保留 5% 退步門檻；Python bulk 的收益不拿來掩蓋核心退步。

## 已執行的檢查與限制

| Check | Command / boundary | Exit/Status | Evidence | Result |
|---|---|---|---|---|
| 診斷輸出與所有權 | attribution/verify_probe.py；兩平台、四查詢、budgets 1/4 | 0 | verify-*-final logs；各 5,720 canonical Rust reads | PASS |
| 正式分段對照 | attribution/run.py；Windows 18、Linux 132 jobs | 0 | primary summaries + raw | PASS；12 組 GIL-only 為負控制，不是讀取驗證 |
| 暖 handle／採樣控制 | attribution/controls.py；42 jobs | 0 | controls-linux | PASS |
| FD 因果控制 | attribution/fd_controls.py；54 paired jobs | 0 | fd-controls-linux | PASS |
| 型別及 allocation 指標 | core_control layout；Windows/Linux | 0 | layout JSON | PASS |
| 143 既有來源 SHA | 與上一版 after-source manifest 逐檔比較 | 0 | verification.json | PASS |
| 樣式與建置 | 隔離 Cargo --release --locked --offline；Ruff / rustfmt | 0 | build/style logs | PASS |
| 正式產品測試重跑 | 本次沒有正式產品來源變更 | NOT RUN | 沿用上一版報告，不把它列為本次重跑 | NOT RUN |

第一輪診斷曾把上一組結果驗證混入純 Rust 批次 wall time，已修正 ready barrier 與最後 worker 完成時間，並將 Windows/Linux 正式分段工作全部重跑。初版來源與範例原始資料以診斷失敗資料保留，沒有採用其純 Rust wall time。後續獨立 Rust executable 再以同 handles 第二遍及 FD 控制驗證結論。

OS cold cache 未控制；新程序／新 handle 不等於冷磁碟。這些是同一實體主機上的 Windows 與 WSL2 環境，不能解讀為兩個 OS 的硬體性能排名。GIL 返回計時還包含很少的閉包返回與執行緒重新排程開銷；全域缺頁和 cache mutex 的各別細分不在本輪精確分段範圍。所有優化都仍是方案，沒有新增產品功能或發布新套件。

[完整原始資料、來源與校驗碼](evidence/2026-10-21/native-cost/README.md)。之前的 [v0.2 驗證報告](NATIVE_VALIDATION.md)與所有舊證據維持原樣。

# Kabaka 專案重構與功能增強說明 (v1.0.0)

本次更新將 Kabaka 從一個簡單的記憶體任務工具，轉型為具備**高可靠性**、**分散式擴展能力**及**深度觀測性**的工業級背景任務框架。

## 核心架構改進

### 1. 引入 Broker 抽象層
- **解耦儲存與邏輯**：新增 `Broker` 介面，支援多種後端實作。
- **MemoryBroker**：用於開發與單機測試，維持輕量化。
- **RedisBroker**：支援 Redis/Valkey，提供分散式環境下的任務持久化與共享。

### 2. 分散式可靠隊列模式 (Reliable Queue Pattern)
- **防止訊息遺失**：在 Redis 實作中導入 `BLMove` 指令。訊息在被 Worker 取出時，會原子性地移動到 `:processing` 備份隊列。
- **Ack 機制**：新增 `Ack` API。只有當任務成功執行或確認失敗後，才會從備份隊列移除訊息，確保任務在 Worker Crash 時不會消失。

### 3. 全域指標監控 (Global Observability)
- **跨實例統計**：當使用 Redis 時，所有實例共用同一套指標計數器（利用 Redis `HINCRBY`）。
- **新增指標維度**：
    - `TotalSuccess / TotalFailed / TotalRetried`: 歷史累計成功、失敗與重試次數。
    - `PendingJobs`: 目前隊列中堆積的任務數。
- **效能觀測 (P95/P99)**：
    - 採用 **滾動窗口採樣 (Rolling Window Sampling)**，紀錄最近 1000 筆任務耗時。
    - 即時計算 P95 與 P99 延遲指標（毫秒），協助發現長尾延遲問題。

### 4. 延遲任務系統 (Delayed Jobs) - **New**
- **支援延遲發布**：新增 `k.PublishDelayed(topic, message, delay)`，允許任務在指定時間後才開始執行。
- **BullMQ 風格高效實作**：
    - **事件驅動**：利用 Redis Pub/Sub 機制，當有新的更早期的任務加入時，立即喚醒背景 Poller。
    - **精確休眠**：Poller 根據 ZSET 中下一個任務的到期時間動態調整休眠長度，而非固定頻率輪詢。
    - **零負擔設計**：在沒有延遲任務時，背景 Goroutine 處於完全阻塞狀態，不消耗 CPU 與 Redis 資源。

### 5. 優雅關機與死鎖防護
- **任務保護**：關機時，Worker 會嘗試將未處理的任務推回 Broker。
- **逾時防護**：在 `dispatch` 與 `worker` 關閉路徑中加入 `context.WithTimeout`，防止因隊列滿載導致的關機死鎖。

---

## 新增 API 與工具

### Kabaka API
- `WithBroker(broker)`: 初始化時注入自定義 Broker。
- `GetMetrics()`: 獲取包含 P95/P99 在內的全方位指標。
- `ResetMetrics(topic)`: 清空特定 Topic 的歷史統計數據。

### 開發者工具
- **Makefile**：
    - `make up`: 一鍵啟動 Valkey (Redis 相容) 容器。
    - `make test-verbose`: 自動啟動環境並執行包含分散式競爭測試在內的所有測試。
- **TODO.md**：規劃未來優化路徑（延遲任務、Protobuf 支援等）。

---

## 測試驗證
- **分散式競爭測試 (`TestRedisBroker_DistributedCompetition`)**：
    - 模擬 3 個獨立 Kabaka 實例同時處理 50 個任務。
    - 驗證結果：任務分發完全原子化，無重複處理，無訊息遺失，指標統計精準。

---
*本文件由 Gemini CLI 協助整理，記錄專案邁向分散式架構的關鍵里程碑。*

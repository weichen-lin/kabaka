# Kabaka Broker 優化藍圖 (Refactor Roadmap)

目前已完成從內部 Channel 到 `Broker` 介面的重構，並實作了 `Memory` 與 `Redis` 兩種 Backend。為了提升系統在生產環境的穩定性與可靠性，建議後續針對以下方向進行優化：

## 1. Redis Broker 可靠性強化 (Reliability)

### 訊息丟失風險 (At-Least-Once Delivery)
*   **問題**：目前使用 `BLPop` 是一次性取出。若訊息取出後、處理完成前發生 Crash，訊息將永久遺失。
*   **優化方案**：
    *   改用 **Reliable Queue Pattern**。
    *   使用 `BLMOVE` (Redis 6.2+) 或 `BRPOPLPUSH` 指令，在取出訊息的同時將其放入一個「處理中 (Processing)」備份隊列。
    *   **Ack 機制**：新增 `Broker.Ack(msg)` 介面，當 Worker 處理成功後才從備份隊列移除。
    *   **Recovery 機制**：系統啟動時檢查備份隊列，重新處理超時未完成的任務。

## 2. 錯誤處理與重試機制 (Error Handling)

### Broker 層級的重試
*   **問題**：當 Redis 發生短暫網路抖動時，目前的 `Pop` 會直接回傳錯誤並由 Topic 層 `Sleep` 1秒。
*   **優化方案**：
    *   在 `RedisBroker` 內部實作指數退避 (Exponential Backoff) 重試。
    *   區分「暫時性錯誤」(網路) 與「永久性錯誤」(序列化失敗)。

## 3. 效能優化 (Performance)

### 序列化開銷
*   **問題**：`json.Marshal/Unmarshal` 在高併發下對 CPU 壓力較大。
*   **優化方案**：
    *   考慮支援 `Protobuf` 或 `MessagePack` 等更高效的序列化格式。
    *   目前的 `Message` 結構簡單，可以考慮預留介面讓使用者自定義序列化方式。

## 4. 監控與觀測性 (Observability)

### 隊列深度監控
*   **優化方案**：
    *   在 `Broker` 介面新增 `Len(topic string) (int64, error)` 方法。
    *   將隊列長度整合進現有的 `GetMetrics()`，讓使用者能觀察是否有任務堆積。

## 5. 功能擴充 (Features)

### 延遲任務 (Delayed Jobs)
*   **優化方案**：
    *   利用 Redis 的 `ZSET` 實作延遲隊列，支援 `k.PublishDelayed(topic, data, delay)`。

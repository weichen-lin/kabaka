# 🚀 Kabaka Redis Broker 潛在問題與改進待辦清單 (TODO List)

這是針對 `kabaka` 專案進行深度分析後整理出的問題清單，涵蓋了資料正確性、可靠性與效能優化。

---

## 🔴 第一階段：嚴重 Bug (必須立即修復)

這些問題會導致訊息遺失、處理停滯或系統功能失效。

- [ ] **修復 `Finish` (Ack) 失效問題**
  - **問題**：目前的 `LRem` 依賴 JSON 內容完全匹配。Worker 修改 `Retry` 次數後，`json.Marshal` 結果改變，導致 `LRem` 找不到元素，訊息永遠卡在 `:processing` 佇列中。
  - **建議**：改用 `Message.Id` 作為刪除標識（建議配合將 `:processing` 改為 Redis Hash 或 Sorted Set）。
- [ ] **修正 `PublishDelayed` 的主題名稱對應**
  - **問題**：`init.go` 中的 `PublishDelayed` 使用了原始名稱 `name` 尋找 Topic，但系統其他地方都使用雜湊後的 `internalName`。這會導致延遲訊息發送失敗。
- [ ] **解決 `Kabaka.dispatch` 無法感應新 Topic 的問題**
  - **問題**：`dispatch` 在 `Start()` 時就固定了監聽的 Topic 列表。之後透過 `CreateTopic` 動態新增的主題將不會被監聽。
  - **建議**：當有新 Topic 註冊時，需通知或重啟 `Watch` 迴圈。

## 🟠 第二階段：可靠性與穩定性 (Reliability)

確保系統在分散式環境下（Worker 當機、網路閃斷）依然能保證訊息不遺失。

- [ ] **實現訊息「可見性超時」與「自動重新派發」(Redelivery)**
  - **問題**：訊息移入 `:processing` 後，若 Worker 當機，該訊息會永久消失。
  - **建議**：
    - 將 `:processing` 改為 **Sorted Set (ZSet)**，Score 設為「預計完成時間戳」。
    - 實作一個背景 Goroutine 定期掃描 ZSet，將超時未 Ack 的訊息移回主佇列。
- [ ] **提升 `PushDelayed` 的原子性 (Atomicity)**
  - **問題**：先 `ZRange` 再 `ZAdd` 是非原子操作，高併發下會導致 Poller 喚醒通知遺失。
  - **建議**：使用 **Lua Script** 封裝 `ZAdd` 與 `Publish` 邏輯。
- [ ] **解決 Redis PubSub 的不可靠性**
  - **問題**：Redis PubSub 是發後既忘 (At-most-once)。若 Broker 重啟期間錯過通知，訊息會延遲 5~10 分鐘才被輪詢掃描到。
  - **建議**：增加更頻繁的「安全輪詢」機制或改用 Redis Streams。

## 🟡 第三階段：架構與效能優化 (Scalability)

提升系統在大規模訊息堆積時的反應速度與資源利用率。

- [ ] **消除 `LRem` 的 O(N) 效能瓶頸**
  - **問題**：當 `:processing` 佇列很長時，`LRem` 的線性掃描會拖慢 Redis。
  - **建議**：將訊息儲存在 **Redis Hash** 中，佇列僅儲存 `ID`。Ack 時透過 `HDel` 與 `ZRem` (O(log N)) 處理。
- [ ] **重構全域 `listenMover` 瓶頸**
  - **問題**：單一 Goroutine 輪詢所有 Topic。Topic 越多，反應越慢。
  - **建議**：改為基於 Topic 的分散式觸發，或為每個 Topic 提供獨立的掃描器。
- [ ] **完善序列化錯誤處理**
  - **問題**：目前的 `Unmarshal` 失敗僅會跳過訊息。
  - **建議**：實作 **死信佇列 (DLQ)**，將格式錯誤或重複失敗的訊息移出主流程進行人工排查。

## 🟢 第四階段：測試與可觀測性

- [ ] **修復並啟用 `TestRedisBroker_DelayedMessageProcessing`**
- [ ] **增加「Worker 當機」類型的集成測試**
- [ ] **加入訊息處理狀態的 Metrics (如：佇列長度、延遲時間、重試次數)**

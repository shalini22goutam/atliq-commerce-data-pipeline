# A Half-Page Write-Up: How the Speed Lane Relates to Phase 1's Batch Lane — When Does a Business Need Each?

### 1. HOW the speed lane relates to Phase 1's batch lane — when does a business need each?

The batch lane is needed for reporting and analysis based on accumulated historical data, where insights do not need to be available immediately. However, businesses may also need to respond to events and changes happening in real time that can directly impact operations or decision-making. In such cases, the streaming (speed) lane is required to process and analyze data continuously as it arrives.

### 2. WHY Gold windows appear late?

A window groups events within a specific time range. Events may arrive late, so Spark cannot immediately consider the window complete.

- 2.1 **Follow-up:** Windows only appear after they close — the watermark must pass the window's end first. So rows show up 10–15 minutes behind real time. That is correctness (waiting for late events), not a bug. Explain WHY?

Once the watermark passes the window's end time, Spark finalizes the window. This is why results can appear 10–15 minutes behind real time—it is a trade-off for correctness, not a bug.

- 2.2 **Follow-up:** WHY a window appears after the watermark passes its end?

The 10-minute watermark allows Spark to accept late-arriving events and include them in the correct window.

> #### Conclusion
> 
> Gold windows may appear late because a 10-minute watermark is applied to handle late-arriving data. Spark keeps the window open while data within the allowed lateness period may still arrive. As the watermark advances beyond the end of a window, Spark considers that window complete and finalizes its result. After the window is closed, data that arrives too late for that window is generally not included in further updates.

### 3. WHY events are keyed by `order_id` OR Why key by `order_id`?

Kafka guarantees ordering within a partition, and equal keys always land in the same partition — so one order's lifecycle events stay in order.

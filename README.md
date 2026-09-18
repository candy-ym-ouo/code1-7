# 录音入库与媒体任务投递（可恢复事务）

录音入库与媒体任务投递采用 **Transactional Outbox（事务发件箱）** 模式：
录音记录与媒体任务在同一个本地事务里落库，随后由中继器投递到 Redis Stream。
目标是两件事：

1. **Redis 短暂不可用时不丢任务** —— 任务先安全落库，投递失败无限补发。
2. **重试不产生两份处理记录** —— 投递、消费、崩溃恢复全链路幂等，一条任务只有一行处理记录。

## 组件

| 文件 | 职责 |
| --- | --- |
| `src/db.js` | SQLite 表结构 + **入库事务**：`recordings` 与 `media_tasks` 原子提交；`processing_records` 处理记录表 |
| `src/relay.js` | Outbox 中继：扫描 `media_tasks(pending)` → Lua 原子去重入队 Redis Stream → 置 `delivered` |
| `src/worker.js` | 消费组消费者：幂等认领处理记录、执行 handler、ACK；`XAUTOCLAIM` 接管崩溃消费者的滞留消息 |
| `src/redis.js` | ioredis 客户端（关闭离线队列，断连快速失败交由 outbox 兜底） |
| `src/service.js` | 编排：入库后即时触发一次投递，后台 relay + worker 循环 |
| `test/` | 13 个测试，含内置 RESP 测试服务器，可精确模拟 Redis 宕机/重启/丢内存 |

## 数据流

```
ingestRecording()                 OutboxRelay                     MediaWorker
─────────────────                 ───────────                     ───────────
BEGIN                             loop:                           XREADGROUP GROUP
 INSERT recordings                  SELECT pending                 INSERT OR IGNORE
 INSERT media_tasks (pending) ──►   Lua: SET dedupe NX + XADD  ──►   processing_records (唯一行)
COMMIT  ◄── 不丢的根本               UPDATE delivered               handler
                                   (失败则保持 pending 重试)        成功 UPDATE done + XACK
```

## 不丢任务：为什么 Redis 宕机不影响受理

- 顺序固定为 **先提交本地事务，再发 Redis**。入库接口的成功只依赖本地数据库，
  Redis 宕机期间录音照常受理，任务行停留在 `pending`。
- relay 每轮扫描 pending 行；Redis 不可用时记录失败原因、行不变，下一轮继续。
  Redis 恢复后自动补发，任务只是延迟，不会丢失。
- relay 只在连接 `ready` 后投递，避开「TCP 已连、握手未完」的半完成窗口
  （该窗口里服务端可能执行成功而客户端按失败处理）。

## 不产生重复：三层幂等防线

1. **入库幂等**：`recordings.id`（业务 UUID）、`media_tasks (recording_id, task_type)`
   均为唯一约束 + `INSERT OR IGNORE`。客户端/网关重试同一个入库请求，
   不会产生第二份录音或任务，返回 `repeated: true`。
2. **投递幂等**：入队用一段 Lua 把 `SET dedupe NX PX` 与 `XADD` 合成一个原子操作。
   - relay 若在 `XADD` 成功后、回写 `delivered` 前崩溃，重放时 dedupe 键命中，
     **只补本地状态，绝不第二次 XADD**，Stream 里仍只有一条消息。
   - Redis 重启清空脚本缓存（NOSCRIPT）时，relay 重新 `SCRIPT LOAD` 并以 EVAL 兜底，
     不会让后续任务在失效 SHA 上反复失败。
3. **消费幂等**：`processing_records.task_id` 主键 + `INSERT OR IGNORE`。
   同一条消息无论被投递、重投还是 `XAUTOCLAIM` 多少次，认领只成功一次，
   始终复用同一行（`attempts` 累加）。`done/failed` 是终态，重投只补 ACK，handler 不再执行。

## 崩溃恢复

- **relay 崩溃**：未投递的行仍 `pending`，重启后补发；已入队未回写的行由 dedupe 键挡住二次 XADD。
- **worker 崩溃**：handler 执行中崩溃则 ACK 未发，消息留在消费组 PEL；
  其他存活 worker 周期性 `XAUTOCLAIM` 接管超过 `visibilityMs` 的消息，
  命中同一行 `processing_records` 继续处理，结果只有一份。
- **瞬时失败**：handler 抛错时不 ACK，复用同一行指数退避重试；
  超过 `maxAttempts` 则置 `failed` 并写入 `dead_letters`（仍不丢，可补偿）。

## 测试

```bash
npm test
```

覆盖：入库事务与校验、入库重试幂等、正常链路、Redis 宕机期间受理与恢复补发、
relay 崩溃重放不二次入队、去重键丢失、消费端重复投递、瞬时失败重试、
超限死信、worker 崩溃后 XAUTOCLAIM 接手、宕机窗口批量入库的端到端收敛。

测试内置的 `test/helpers/fake-redis.js` 是真实 TCP 上的最小 RESP 服务器，
严格模拟单连接 FIFO 回复与阻塞命令语义，可 `crash()` / `restart({ flush })`
模拟宕机与无持久化重启。

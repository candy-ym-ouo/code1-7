'use strict';

const { openDb, createRepository } = require('./db');
const { createRedis } = require('./redis');
const { OutboxRelay, STREAM } = require('./relay');
const { MediaWorker } = require('./worker');

/**
 * 组装录音入库服务：
 * - ingest：本地事务（录音 + outbox 任务），提交成功即视为"已受理且不丢"，
 *   随后触发一次即时 relay（不等轮询周期）；Redis 挂了也没关系，后台 relay 补发。
 * - relay：outbox -> Redis Stream，幂等投递。
 * - worker：消费组处理，处理记录唯一、崩溃可恢复。
 */
async function createService({
  dbFile = ':memory:',
  redisOptions = {},
  stream = STREAM,
  handler = async (payload) => ({ ok: true, payload }),
  batchSize = 50,
  intervalMs = 200,
} = {}) {
  const repo = createRepository(openDb(dbFile));
  const redis = createRedis(redisOptions);
  await redis.connect().catch(() => {}); // 连不上不阻塞：服务照常受理，relay 负责补发

  const relay = new OutboxRelay({ repo, redis, stream, batchSize, intervalMs });
  const worker = new MediaWorker({ repo, redis, handler, stream });
  await worker.ensureGroup().catch(() => {});

  return {
    repo,
    redis,
    relay,
    worker,

    /**
     * 录音入库（可恢复事务入口）。
     * 事务提交后尽力立即投递；Redis 不可用时立即返回 accepted，后台补发。
     */
    async ingest(rec) {
      const result = repo.ingestRecording(rec);
      // 不 await 到失败：即时投递失败已由 outbox pending 行兜住
      relay.tick().catch(() => {});
      return result;
    },

    start() {
      relay.start();
      worker.start();
    },

    async stop() {
      relay.stop();
      worker.stop();
      await redis.quit().catch(() => redis.disconnect());
      repo.db.close();
    },
  };
}

module.exports = { createService };

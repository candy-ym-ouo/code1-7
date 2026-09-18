'use strict';

/**
 * 使用示例（需要一个可连接的 Redis）：
 *   REDIS_HOST=127.0.0.1 REDIS_PORT=6379 node examples/basic.js
 *
 * 演示：
 * 1. 录音入库（本地事务：recording + media task 原子提交）
 * 2. relay 把任务投递到 Redis Stream；Redis 宕机时任务保存在本地 outbox，恢复后补发
 * 3. worker 消费处理；重复投递/崩溃恢复都只产生一条处理记录
 */
const { createService } = require('../src');

async function main() {
  const service = await createService({
    dbFile: process.env.DB_FILE || ':memory:',
    redisOptions: {
      host: process.env.REDIS_HOST || '127.0.0.1',
      port: Number(process.env.REDIS_PORT || 6379),
    },
    // 真实的媒体处理逻辑（转写、分离等）
    handler: async (payload, ctx) => {
      console.log(`[worker] handling ${ctx.taskId} attempt=${ctx.attempt}`, payload);
      await new Promise((r) => setTimeout(r, 50));
      return { transcriptUri: `s3://out/${payload.recordingId}.txt` };
    },
  });

  service.start();

  const recordingId = `rec-${Date.now()}`;
  const result = await service.ingest({
    id: recordingId, // 业务 UUID：客户端重试同一请求的幂等键
    callId: 'call-123',
    fileUri: `s3://recordings/${recordingId}.wav`,
    durationMs: 12_000,
    metadata: { agent: 'agent-7' },
    tasks: [
      { type: 'transcribe' },
      { type: 'diarize', payload: { recordingId, sensitivity: 0.6 } },
    ],
  });
  console.log('ingested tasks:', result.tasks.map((t) => `${t.id} (${t.status})`));

  // 给后台 relay/worker 一点时间，然后退出
  await new Promise((r) => setTimeout(r, 1500));
  await service.stop();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

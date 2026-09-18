'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { openDb, createRepository } = require('../src/db');
const { createRedis } = require('../src/redis');
const { OutboxRelay, STREAM } = require('../src/relay');
const { MediaWorker } = require('../src/worker');
const { FakeRedis } = require('./helpers/fake-redis');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function newRedis(fake) {
  const r = createRedis({ port: fake.port, lazyConnect: true });
  await r.connect().catch(() => {});
  await new Promise((res) => {
    if (r.status === 'ready') return res();
    r.once('ready', res);
  });
  return r;
}

function makeRepo() {
  return createRepository(openDb(':memory:'));
}

async function setup({ handler } = {}) {
  const fake = new FakeRedis();
  await fake.listen();
  const repo = makeRepo();
  const redis = await newRedis(fake);
  const relay = new OutboxRelay({ repo, redis, intervalMs: 50 });
  const worker = new MediaWorker({
    repo, redis, handler: handler || (async (p) => ({ handled: p })),
    blockMs: 300, visibilityMs: 10000,
  });
  await worker.ensureGroup();
  return { fake, repo, redis, relay, worker };
}

const rec = (id, extra = {}) => ({
  id, callId: `call-${id}`, fileUri: `s3://bucket/${id}.wav`,
  durationMs: 3000, metadata: { agent: 'a1' }, ...extra,
});

test('入库：录音与任务在同一事务，默认生成转写任务', () => {
  const repo = makeRepo();
  const out = repo.ingestRecording(rec('r1'));
  assert.equal(out.repeated, false);
  assert.equal(out.tasks.length, 1);
  assert.equal(out.tasks[0].id, 'transcribe:r1');
  assert.equal(out.tasks[0].status, 'pending');
  assert.equal(out.tasks[0].payload.fileUri, 's3://bucket/r1.wav');

  assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM recordings').get().c, 1);
  assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM media_tasks').get().c, 1);
});

test('入库：一个录音可投递多种媒体任务', () => {
  const repo = makeRepo();
  const out = repo.ingestRecording(rec('r2', {
    tasks: [
      { type: 'transcribe' },
      { type: 'diarize', payload: { sensitivity: 0.5 } },
    ],
  }));
  assert.equal(out.tasks.length, 2);
  assert.deepEqual(out.tasks.map((t) => t.taskType).sort(), ['diarize', 'transcribe']);
});

test('入库重试幂等：同一 recordingId 重复提交不产生第二份记录', () => {
  const repo = makeRepo();
  const first = repo.ingestRecording(rec('r3'));
  const second = repo.ingestRecording(rec('r3'));
  assert.equal(first.repeated, false);
  assert.equal(second.repeated, true);
  assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM recordings').get().c, 1);
  assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM media_tasks').get().c, 1);
  // 重试时返回的仍是同一组任务
  assert.deepEqual(second.tasks.map((t) => t.id), ['transcribe:r3']);
});

test('入库校验：缺幂等键/必填字段直接拒绝（不会写半条数据）', () => {
  const repo = makeRepo();
  assert.throws(() => repo.ingestRecording({ callId: 'c', fileUri: 'u' }), /idempotency/);
  assert.throws(() => repo.ingestRecording(rec('x', { callId: '' })), /callId/);
  assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM recordings').get().c, 0);
});

test('正常路径：入库 → relay 投递 → worker 处理，只有一条处理记录', async () => {
  const { fake, repo, redis, relay, worker } = await setup();
  try {
    repo.ingestRecording(rec('n1'));
    await relay.tick();
    assert.equal(repo.db.prepare(
      "SELECT status FROM media_tasks WHERE id='transcribe:n1'").get().status, 'delivered');

    await worker.consumeOnce();
    const row = repo.db.prepare(
      "SELECT * FROM processing_records WHERE task_id='transcribe:n1'").get();
    assert.equal(row.status, 'done');
    assert.equal(row.attempts, 1); // handler 实际执行次数：成功执行一次
    assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM processing_records').get().c, 1);
    // Stream 已 ACK：PEL 为空
    assert.equal(fake.groups.get(STREAM).get('media-workers').pending.size, 0);
  } finally {
    worker.stop(); redis.disconnect(); fake.close();
  }
});

test('Redis 短暂不可用：入库不失败，任务保持 pending，恢复后自动补发', async () => {
  const { fake, repo, redis, relay, worker } = await setup();
  try {
    // Redis 宕机期间受理录音
    const port = fake.port;
    fake.crash();
    await sleep(150); // 等 ioredis 感知断连

    const accepted = repo.ingestRecording(rec('o1'));
    assert.ok(accepted.tasks[0]);

    // relay 多次尝试，全部失败但不抛、任务不丢
    const r1 = await relay.tick();
    assert.equal(r1, 0);
    const r2 = await relay.tick();
    assert.equal(r2, 0);
    assert.equal(repo.db.prepare(
      "SELECT status, attempts FROM media_tasks WHERE id='transcribe:o1'").get().status, 'pending');
    assert.ok(repo.db.prepare(
      "SELECT attempts FROM media_tasks WHERE id='transcribe:o1'").get().attempts >= 2);

    // Redis 恢复（同端口回来）
    await fake.restart({ port });
    await sleep(400); // ioredis 自动重连
    assert.equal(redis.status, 'ready');
    await worker.ensureGroup(); // 新服务端需要重建组
    const moved = await relay.tick();
    assert.equal(moved, 1);
    assert.equal(repo.db.prepare(
      "SELECT status FROM media_tasks WHERE id='transcribe:o1'").get().status, 'delivered');

    await worker.consumeOnce();
    const row = repo.db.prepare(
      "SELECT status FROM processing_records WHERE task_id='transcribe:o1'").get();
    assert.equal(row.status, 'done');
    assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM processing_records').get().c, 1);
  } finally {
    worker.stop(); redis.disconnect(); fake.close();
  }
});

test('relay 崩溃重放：XADD 已成功但状态未回写，重放不产生第二条 Stream 消息', async () => {
  const { fake, repo, redis, relay, worker } = await setup();
  try {
    repo.ingestRecording(rec('p1'));
    const task = repo.listPendingTasks(1)[0];

    // 模拟 relay 第一次：Lua 执行成功（消息入队 + dedupe 键写入），
    // 但在 markDelivered 之前进程被杀 —— 行仍为 pending。
    // （因此此处不能走 relay.tick，那会立刻回写 delivered。）
    const messageId = await redis.eval(
      require('../src/relay').ENQUEUE_LUA, 2,
      'media-task:enqueued:' + task.id, STREAM,
      task.id, task.payload, 30 * 24 * 3600 * 1000);
    assert.ok(messageId);
    assert.equal(repo.db.prepare(
      "SELECT status FROM media_tasks WHERE id=?").get(task.id).status, 'pending');
    const streamLenBefore = fake.streams.get(STREAM).length;

    // relay 重启后的重放：dedupe 命中，返回 replayed，绝不二次 XADD
    const result = await relay.deliverOne(task);
    assert.equal(result, 'replayed');
    assert.equal(fake.streams.get(STREAM).length, streamLenBefore);
    assert.equal(repo.db.prepare(
      "SELECT status FROM media_tasks WHERE id=?").get(task.id).status, 'delivered');

    // 消费端照常只处理一次
    await worker.consumeOnce();
    assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM processing_records').get().c, 1);
  } finally {
    worker.stop(); redis.disconnect(); fake.close();
  }
});

test('Redis 无持久化重启导致 dedupe 丢失：outbox 行已 delivered，不会再投，仍无重复', async () => {
  // 这是对"去重键只是优化、不是唯一安全网"的验证：
  // 即使 Redis 把 dedupe 键冲掉，已 delivered 的 outbox 行不会被 relay 再扫描，
  // Stream 里不会因 relay 产生第二条。
  const { fake, repo, redis, relay } = await setup();
  try {
    repo.ingestRecording(rec('p2'));
    await relay.tick();
    const before = fake.streams.get(STREAM).length;
    assert.equal(before, 1);
    await fake.restart({ port: fake.port, flush: true });
    await sleep(300);
    await relay.tick(); // 没有 pending 行
    assert.equal(fake.streams.get(STREAM).length, before);
    redis.disconnect();
  } finally {
    fake.close();
  }
});

test('消费者重投：同一条消息重复投递，只产生一行 processing_records 且 handler 只在首次执行', async () => {
  const { fake, repo, redis, worker } = await setup();
  try {
    let handlerRuns = 0;
    worker.handler = async () => { handlerRuns++; return { v: 1 }; };
    repo.claimProcessing('transcribe:d1'); // 模拟第一次投递时建行
    const fields = ['taskId', 'transcribe:d1', 'payload', JSON.stringify({ fileUri: 'u' })];

    await worker.handleMessage('1-0', fields);
    // 模拟 Stream 把同一条消息又投了一遍（重平衡/重放）
    await worker.handleMessage('1-0', fields);
    await worker.handleMessage('9-9', fields);

    assert.equal(handlerRuns, 1);
    assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM processing_records').get().c, 1);
    const row = repo.db.prepare('SELECT * FROM processing_records WHERE task_id=?').get('transcribe:d1');
    assert.equal(row.status, 'done');
    assert.equal(JSON.parse(row.result).v, 1);
  } finally {
    worker.stop(); redis.disconnect(); fake.close();
  }
});

test('瞬时失败重试：复用同一行，最终成功；attempts 反映重试次数', async () => {
  const { fake, repo, redis, worker } = await setup();
  try {
    let n = 0;
    worker.handler = async () => {
      n++;
      if (n < 3) throw new Error('transient upstream 503');
      return { ok: true };
    };
    // 关闭本地退避定时器的影响不做要求（它与手动重放共用幂等路径），直接连续重放消息
    const fields = ['taskId', 'transcribe:r', 'payload', '{}'];
    await worker.handleMessage('2-0', fields);
    await worker.handleMessage('2-0', fields);
    await worker.handleMessage('2-0', fields);
    assert.equal(n, 3);
    const row = repo.db.prepare('SELECT * FROM processing_records WHERE task_id=?').get('transcribe:r');
    assert.equal(row.status, 'done');
    assert.equal(row.attempts, 3); // 三次 handler 执行（两次失败 + 一次成功）
    assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM processing_records').get().c, 1);
  } finally {
    worker.stop(); redis.disconnect(); fake.close();
  }
});

test('超过最大重试：唯一一行置 failed 并入死信，消息被 ACK', async () => {
  const { fake, repo, redis, worker } = await setup({ handler: async () => {
    throw new Error('permanent decode failure');
  } });
  worker.maxAttempts = 2;
  try {
    const fields = ['taskId', 'transcribe:dl', 'payload', JSON.stringify({ x: 1 })];
    await worker.handleMessage('3-0', fields);
    await worker.handleMessage('3-0', fields);
    const row = repo.db.prepare('SELECT * FROM processing_records WHERE task_id=?').get('transcribe:dl');
    assert.equal(row.status, 'failed');
    assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM processing_records').get().c, 1);
    assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM dead_letters').get().c, 1);
    // 再来一条投递只 ACK 不执行 handler（终态保护）
    const runsBefore = row.attempts;
    await worker.handleMessage('3-0', fields);
    const row2 = repo.db.prepare('SELECT attempts FROM processing_records WHERE task_id=?').get('transcribe:dl');
    assert.equal(row2.attempts, runsBefore);
  } finally {
    worker.stop(); redis.disconnect(); fake.close();
  }
});

test('崩溃恢复：消费者 A 处理中崩溃（未 ACK），消费者 B 经 XAUTOCLAIM 接手，仍是同一行', async () => {
  const fake = new FakeRedis();
  await fake.listen();
  const repo = makeRepo();
  const redisA = await newRedis(fake);
  const redisB = await newRedis(fake);

  let runs = 0;
  const workerA = new MediaWorker({
    repo, redis: redisA, consumer: 'A',
    handler: async () => { runs++; throw new Error('A crashed mid-processing'); },
    blockMs: 200, visibilityMs: 200, retryBaseMs: 9_999_999, // 关掉 A 的本地重试
  });
  const workerB = new MediaWorker({
    repo, redis: redisB, consumer: 'B',
    handler: async () => { runs++; return { recovered: true }; },
    blockMs: 200, visibilityMs: 200, retryBaseMs: 9_999_999,
  });
  await workerA.ensureGroup();
  try {
    // 任务入队并投递给 A
    const relay = new OutboxRelay({ repo, redis: redisA });
    repo.ingestRecording(rec('cr1'));
    await relay.tick();
    await workerA.consumeOnce(); // A 收到并处理失败，未 ACK → 进入 PEL

    // 模拟 A 崩溃：彻底停止，其 PEL 消息闲置
    workerA.stop();
    const pel = fake.groups.get(STREAM).get('media-workers').pending;
    assert.equal(pel.size, 1);
    const [msgId, info] = [...pel.entries()][0];
    assert.equal(info.consumer, 'A');

    // 超过 visibility 后 B 接管
    info.idleSince = Date.now() - 10_000;
    await workerB.reclaimStale();

    const row = repo.db.prepare('SELECT * FROM processing_records WHERE task_id=?').get('transcribe:cr1');
    assert.equal(row.status, 'done');
    assert.equal(JSON.parse(row.result).recovered, true);
    assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM processing_records').get().c, 1);
    assert.equal(pel.size, 0); // B 成功后 ACK
    assert.ok(runs >= 2, 'handler 在 A、B 上各跑过');
  } finally {
    workerA.stop(); workerB.stop();
    redisA.disconnect(); redisB.disconnect(); fake.close();
  }
});

test('后台模式端到端：Redis 宕机窗口内连续入库，恢复后全部处理且无重复', async () => {
  const fake = new FakeRedis();
  await fake.listen();
  const repo = makeRepo();
  const redis = await newRedis(fake);
  const processed = [];
  const relay = new OutboxRelay({ repo, redis, intervalMs: 30 });
  const worker = new MediaWorker({
    repo, redis,
    handler: async (p, ctx) => { processed.push(ctx.taskId); return {}; },
    blockMs: 100, visibilityMs: 5000,
  });
  await worker.ensureGroup();
  relay.start(); worker.start();
  const port = fake.port;
  try {
    // 宕机窗口内入库 5 条 + 对第 1 条重复入库 2 次
    fake.crash();
    await sleep(100);
    for (let i = 0; i < 5; i++) repo.ingestRecording(rec(`e2e${i}`));
    repo.ingestRecording(rec('e2e0'));
    repo.ingestRecording(rec('e2e0'));
    await sleep(500);
    assert.equal(repo.db.prepare("SELECT COUNT(*) c FROM media_tasks WHERE status='pending'").get().c, 5);

    await fake.restart({ port });
    await sleep(300);
    await worker.ensureGroup().catch(() => {});
    // 等待后台 relay + worker 收敛
    await sleep(2000);

    assert.equal(repo.db.prepare("SELECT COUNT(*) c FROM media_tasks WHERE status='pending'").get().c, 0);
    assert.equal(repo.db.prepare('SELECT COUNT(*) c FROM processing_records').get().c, 5);
    assert.equal(repo.db.prepare("SELECT COUNT(*) c FROM processing_records WHERE status='done'").get().c, 5);
    assert.deepEqual(processed.slice().sort(), [0,1,2,3,4].map((i) => `transcribe:e2e${i}`));
    // Stream 中恰好 5 条消息（重复入库 + relay 重放都没有制造额外消息）
    assert.equal(fake.streams.get(STREAM).length, 5);
  } finally {
    relay.stop(); worker.stop(); redis.disconnect(); fake.close();
  }
});

'use strict';

/**
 * 媒体任务消费者（Redis Stream Consumer Group + 本地处理记录）。
 *
 * "重试不得生成两份处理记录"在这里由三道防线保证：
 * 1. relay 侧 Lua 去重：同一 taskId 在 Stream 里最多一条消息（见 relay.js）。
 * 2. processing_records.task_id 主键 + INSERT OR IGNORE：
 *    无论同一条消息被投递/重投/XAUTOCLAIM 多少次，认领只会成功一次，
 *    后续重投复用同一行（attempts 累加），绝不插入第二行。
 * 3. 状态机 done/failed 是终态：已经完成的任务即使再次收到也只补 XACK，
 *    handler 不会再执行第二次。
 *
 * 崩溃恢复：handler 执行中进程崩溃 → XACK 未发，消息留在该消费者 PEL；
 * 其余活着的消费者周期性 XAUTOCLAIM 拿走超时（min-idle-time）的消息，
 * 命中同一行 processing_records 继续执行。
 */
const { STREAM } = require('./relay');
const GROUP = 'media-workers';
const CONSUMER_DEFAULT = `consumer-${process.pid}`;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

class MediaWorker {
  /**
   * @param {object} opts
   * @param {import('./db').createRepository} opts.repo
   * @param {import('ioredis').Redis} opts.redis
   * @param {(payload:object, ctx:{taskId:string, attempt:number}) => Promise<object>} opts.handler
   * @param {string} [opts.stream]
   * @param {string} [opts.group]
   * @param {string} [opts.consumer]
   * @param {number} [opts.maxAttempts]  超过后落死信并 ACK
   * @param {number} [opts.blockMs]      XREADGROUP 阻塞时长
   * @param {number} [opts.visibilityMs] XAUTOCLAIM 的 min-idle-time
   */
  constructor({
    repo,
    redis,
    handler,
    stream = STREAM,
    group = GROUP,
    consumer = CONSUMER_DEFAULT,
    maxAttempts = 8,
    blockMs = 2000,
    visibilityMs = 15000,
    retryBaseMs = 100,
    retryMaxMs = 5000,
  }) {
    this.repo = repo;
    this.redis = redis;
    this.handler = handler;
    this.stream = stream;
    this.group = group;
    this.consumer = consumer;
    this.maxAttempts = maxAttempts;
    this.blockMs = blockMs;
    this.visibilityMs = visibilityMs;
    this.retryBaseMs = retryBaseMs;
    this.retryMaxMs = retryMaxMs;
    this.stopped = false;
    this.inFlight = new Set(); // 进程内正在执行 handler 的 taskId（XAUTOCLAIM 与本地重试去重）
    this.stats = {
      received: 0,
      processed: 0,     // handler 首次执行成功
      recovered: 0,     // 崩溃/超时后续跑成功
      resumed: 0,       // 中途瞬时失败后重试
      skippedDone: 0,   // 重投到已完成任务，仅 ACK
      transientErrors: 0,
      deadLettered: 0,
    };
  }

  /** 幂等建组：已存在则忽略 BUSYGROUP。从 0 开始：保证组创建早/晚于首批消息都不漏读。 */
  async ensureGroup() {
    try {
      await this.redis.xgroup('CREATE', this.stream, this.group, '0', 'MKSTREAM');
    } catch (err) {
      if (!/BUSYGROUP/.test(String(err.message))) throw err;
    }
  }

  /**
   * 处理一条 Stream 消息。返回 true 表示已 ACK（消息完成或已转死信）。
   * @param {string} id  Stream 消息 ID
   * @param {string[]|object} fields 消息字段
   */
  async handleMessage(id, fields) {
    this.stats.received++;
    const kv = Array.isArray(fields) ? fields : Object.entries(fields).flat();
    let taskId;
    let payloadRaw;
    for (let i = 0; i < kv.length; i += 2) {
      if (kv[i] === 'taskId') taskId = kv[i + 1];
      if (kv[i] === 'payload') payloadRaw = kv[i + 1];
    }
    if (!taskId) {
      // 畸形消息直接 ACK，避免无限毒消息
      await this.redis.xack(this.stream, this.group, id);
      return true;
    }

    // 幂等认领：同一 taskId 全局只有一行处理记录
    const claim = this.repo.claimProcessing(taskId);
    const row = claim.row;
    let payload;
    try {
      payload = JSON.parse(payloadRaw || '{}');
    } catch {
      payload = {};
    }

    if (!claim.inserted && (row.status === 'done' || row.status === 'failed')) {
      // 重投到已终态任务：handler 不再执行，不新增记录，仅补 ACK
      this.stats.skippedDone++;
      await this.redis.xack(this.stream, this.group, id);
      return true;
    }
    if (!claim.inserted && row.status === 'processing') {
      // 同一行被再次投递：上次的消费者崩溃或正在重试 → 恢复执行
      this.stats.recovered++;
    }

    // 进程内并发保护：本地退避重试与 XAUTOCLAIM 可能同时拿到同一条消息，
    // 第二个调用直接跳过（消息不 ACK，稍后还会再来），避免 handler 并发双跑。
    if (this.inFlight.has(taskId)) {
      return false;
    }
    this.inFlight.add(taskId);
    try {
      return await this._runHandler(id, taskId, payload, row);
    } finally {
      this.inFlight.delete(taskId);
    }
  }

  async _runHandler(id, taskId, payload, row) {
    let result;
    try {
      result = await this.handler(payload, { taskId, attempt: row.attempts + 1 });
    } catch (err) {
      return this._handleHandlerError(id, taskId, payload, err);
    }

    // 成功：终态 done（SQL 自身带 status != 'done' 守卫，重复完成不改行）
    this.repo.finishProcessing(taskId, result);
    this.stats.processed++;
    await this.redis.xack(this.stream, this.group, id);
    return true;
  }

  async _handleHandlerError(id, taskId, payload, err) {
    const row = this.repo.selectProcessing(taskId);
    const attempt = row ? row.attempts + 1 : 1;

    if (attempt >= this.maxAttempts) {
      // 终态失败：记录唯一一行的失败结果 + 落死信 + ACK，消息不再重投
      this.repo.db.transaction(() => {
        this.repo.failProcessing(taskId, err);
        this.repo.deadLetter(taskId, payload, err);
      })();
      this.stats.deadLettered++;
      await this.redis.xack(this.stream, this.group, id);
      return true;
    }

    // 瞬时错误：仍复用同一行（attempts+1），不 ACK。
    // 消息留在 PEL，超过 visibilityMs 后被 XAUTOCLAIM 重新认领执行。
    this.repo.requeueProcessing(taskId, err);
    this.stats.transientErrors++;
    if (!this._resumeTimers) this._resumeTimers = new Map();
    // 本地也安排一次提前重试（指数退避）；与 XAUTOCLAIM 谁先触发都安全——
    // processing 行会再次被同一套幂等逻辑兜住。
    const delay = Math.min(this.retryBaseMs * 2 ** (attempt - 1), this.retryMaxMs);
    if (!this._resumeTimers.has(id)) {
      this._resumeTimers.set(
        id,
        setTimeout(() => {
          this._resumeTimers.delete(id);
          this.handleMessage(id, ['taskId', taskId, 'payload', JSON.stringify(payload)]).catch(() => {});
        }, delay)
      );
    }
    this.stats.resumed++;
    return false;
  }

  /** 接管别的消费者 PEL 里滞留超过 visibilityMs 的消息（崩溃恢复）。 */
  async reclaimStale() {
    let res;
    try {
      res = await this.redis.xautoclaim(
        this.stream,
        this.group,
        this.consumer,
        String(this.visibilityMs),
        '0',
        'COUNT',
        50
      );
    } catch (err) {
      if (/NOGROUP/.test(String(err.message))) return;
      throw err;
    }
    // ioredis 可能返回数组（RESP2）或对象（RESP3），兼容取值
    const entries = Array.isArray(res) ? res[1] : res.messages;
    if (!entries) return;
    for (const entry of entries) {
      const [id, fields] = entry;
      await this.handleMessage(id, fields).catch(() => {});
    }
  }

  async consumeOnce() {
    let reply;
    try {
      reply = await this.redis.xreadgroup(
        'GROUP', this.group, this.consumer,
        'COUNT', 16,
        'BLOCK', this.blockMs,
        'STREAMS', this.stream, '>'
      );
    } catch (err) {
      if (/NOGROUP/.test(String(err.message))) {
        await this.ensureGroup();
        return;
      }
      // Redis 短暂不可用：Stream 消息在服务端 PEL/队列里不丢，稍后重连继续
      await sleep(500);
      return;
    }
    if (!reply) return;
    // ioredis v6 形态为 [[[stream, [[id, fields], ...]]], null]；RESP3 为对象。
    // 逐层解包，统一收集成 [streamName, [[id, fields], ...]] 列表。
    const groups = [];
    const collect = (node) => {
      if (!Array.isArray(node)) return;
      if (typeof node[0] === 'string' && Array.isArray(node[1]) &&
          node[1].every((e) => Array.isArray(e))) {
        groups.push(node);
        return;
      }
      for (const child of node) collect(child);
    };
    if (Array.isArray(reply)) collect(reply);
    else if (reply.messages) groups.push([reply.name || this.stream, reply.messages]);

    for (const group of groups) {
      const messages = group[1];
      for (const [id, fields] of messages) {
        await this.handleMessage(id, fields).catch(() => {});
      }
    }
  }

  start() {
    this.stopped = false;
    const loop = async () => {
      if (this.stopped) return;
      try {
        await this.consumeOnce();
        await this.reclaimStale().catch(() => {});
      } catch {
        // 消费循环不能因单轮异常退出
      }
      if (!this.stopped) setImmediate(loop);
    };
    setImmediate(loop);
  }

  stop() {
    this.stopped = true;
    if (this._resumeTimers) {
      for (const t of this._resumeTimers.values()) clearTimeout(t);
      this._resumeTimers.clear();
    }
  }
}

module.exports = { MediaWorker, GROUP };

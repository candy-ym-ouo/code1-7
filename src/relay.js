'use strict';

/**
 * Outbox 中继：把 media_tasks(pending) 投递到 Redis Stream。
 *
 * 可恢复语义：
 * - 顺序固定为"先落库（事务已提交）→ 再发 Redis"。Redis 宕机时行保持 pending，
 *   下一轮 relay 继续尝试，任务不可能丢；最坏只是延迟。
 * - 只有在 Redis 确认入队后才把行置为 delivered。
 *
 * 重试不产生两份队列记录：
 *   入队用 Lua 脚本原子完成 "SET dedupe NX PX ttl" + "XADD"。
 *   relay 崩溃在 XADD 之后、状态回写之前时，重放会被 dedupe 键挡住，
 *   只更新本地状态、绝不第二次 XADD（Stream 里仍然只有一条消息）。
 *
 * Lua 脚本返回 1 = 本次新入队；0 = 之前已入队，本次为重放。
 */
const ENQUEUE_LUA = `
if redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3], 'NX') then
  return redis.call('XADD', KEYS[2], '*', 'taskId', ARGV[1], 'payload', ARGV[2])
else
  return false
end
`;

const STREAM = 'stream:media-tasks';
const DEDUPE_PREFIX = 'media-task:enqueued:';
const DEDUPE_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 天，覆盖最长的滞留/恢复窗口

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

class OutboxRelay {
  /**
   * @param {object} opts
   * @param {import('./db').createRepository} opts.repo
   * @param {import('ioredis').Redis} opts.redis
   * @param {string} [opts.stream]
   * @param {number} [opts.batchSize]
   * @param {number} [opts.intervalMs]
   */
  constructor({ repo, redis, stream = STREAM, batchSize = 50, intervalMs = 200 }) {
    this.repo = repo;
    this.redis = redis;
    this.stream = stream;
    this.batchSize = batchSize;
    this.intervalMs = intervalMs;
    this.sha = null;
    this.stopped = false;
    this.timer = null;
    this.stats = { attempts: 0, delivered: 0, replayed: 0, errors: 0 };
  }

  async ensureScript() {
    if (!this.sha) this.sha = await this.redis.script('LOAD', ENQUEUE_LUA);
    return this.sha;
  }

  /**
   * 投递单条任务。
   * @returns {Promise<'delivered'|'replayed'|'retry'>}
   *   delivered 新入队；replayed 队列里已有（幂等重放，仅补状态）；retry Redis 不可用。
   */
  async deliverOne(task) {
    this.stats.attempts++;
    // 只在连接 ready 后投递。TCP 已连通但握手未完成（status=connect）时，
    // 命令可能"服务端执行成功、客户端却按失败处理"（响应在 ready 边界丢失）。
    // 这种半完成窗口下若跳过守卫，dedupe 键已写而本地状态未改，会造成假重放。
    if (this.redis.status !== 'ready') {
      this.stats.errors++;
      this.repo.recordRelayFailure(
        task.id,
        new Error(`redis not ready (status=${this.redis.status})`)
      );
      return 'retry';
    }
    let messageId;
    try {
      const sha = await this.ensureScript();
      messageId = await this.redis.evalsha(
        sha,
        2,
        DEDUPE_PREFIX + task.id,
        this.stream,
        task.id,
        task.payload,
        DEDUPE_TTL_MS
      );
    } catch (err) {
      // NOSCRIPT：Redis 重启清空了脚本缓存。必须立刻重新 LOAD 恢复 sha，
      // 否则后续每条任务都会拿到失效 sha 反复 NOSCRIPT、反复走 EVAL 兜底，
      // 在批量投递的命令管道交错中可能造成错误回复错配。
      if (err && /NOSCRIPT/.test(String(err.message))) {
        this.sha = null;
        return this._reloadAndDeliver(task);
      }
      // Redis 短暂不可用：记录失败原因，行保持 pending，等下轮重放，不抛给上层
      this.stats.errors++;
      this.repo.recordRelayFailure(task.id, err);
      return 'retry';
    }

    return this._settle(task, messageId);
  }

  /** NOSCRIPT 恢复：EVAL 兜底本次投递，同时 SCRIPT LOAD 恢复后续任务用的 sha。 */
  async _reloadAndDeliver(task) {
    // 先重新 LOAD：成功后后续任务立即恢复 EVALSHA 快路径
    await this.ensureScript().catch(() => {});
    let messageId;
    try {
      messageId = await this.redis.eval(
        ENQUEUE_LUA,
        2,
        DEDUPE_PREFIX + task.id,
        this.stream,
        task.id,
        task.payload,
        DEDUPE_TTL_MS
      );
    } catch (err) {
      if (err && /NOSCRIPT/.test(String(err.message))) this.sha = null;
      this.stats.errors++;
      this.repo.recordRelayFailure(task.id, err);
      return 'retry';
    }
    // 若此前 LOAD 没成功，这里再补一次（不影响本次结果）
    if (!this.sha) this.ensureScript().catch(() => {});
    return this._settle(task, messageId);
  }

  /** 根据 Lua 返回值落本地状态。 */
  _settle(task, messageId) {
    if (messageId) {
      // 真正 XADD 成功才转 delivered
      this.repo.markDelivered(task.id);
      this.stats.delivered++;
      return 'delivered';
    }
    // dedupe 命中：之前已经 XADD 过（典型：上次崩溃在回写状态前），只补状态
    this.repo.markDelivered(task.id);
    this.stats.replayed++;
    return 'replayed';
  }

  /** 扫描一轮 outbox。Redis 完全不可用时返回 0，等待下一轮。 */
  async tick() {
    const pending = this.repo.listPendingTasks(this.batchSize);
    let moved = 0;
    for (const task of pending) {
      const result = await this.deliverOne(task);
      if (result === 'delivered' || result === 'replayed') moved++;
    }
    return moved;
  }

  /** 启动后台轮询；也可只用 tick() 手动驱动（测试/与事务同提交点场景）。 */
  start() {
    if (this.timer) return;
    const loop = async () => {
      if (this.stopped) return;
      try {
        await this.tick();
      } catch {
        // 单轮异常不能杀死循环（行仍是 pending，下轮自愈）
      }
      if (!this.stopped) this.timer = setTimeout(loop, this.intervalMs);
    };
    this.timer = setTimeout(loop, 0);
  }

  stop() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }
}

module.exports = { OutboxRelay, ENQUEUE_LUA, STREAM, DEDUPE_PREFIX };

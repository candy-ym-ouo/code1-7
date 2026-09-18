/**
 * 媒体任务事务性发件箱（API 侧）。
 *
 * 与 apps/worker/src/outbox.ts 保持同构：两个应用独立部署，各自持有投递/补偿能力。
 * 修改本文件时请同步修改 worker 侧对应文件。
 */
import { Queue, type JobsOptions } from 'bullmq';
import { Prisma } from '@prisma/client';

/** 与数据库枚举 MediaTaskStatus 保持一致。 */
export type MediaTaskStatusValue =
  | 'PENDING'
  | 'QUEUED'
  | 'PROCESSING'
  | 'COMPLETED'
  | 'FAILED';

export const MEDIA_QUEUE_NAME = 'media';
export const MEDIA_JOB_NAME = 'media.process';
export const MEDIA_MAX_ATTEMPTS = 3;

export const MEDIA_JOB_OPTIONS: JobsOptions = {
  attempts: MEDIA_MAX_ATTEMPTS,
  backoff: { type: 'exponential', delay: 1000 },
  removeOnComplete: { age: 3600, count: 1000 },
  removeOnFail: { age: 24 * 3600, count: 1000 },
};

export type OutboxLogger = {
  info?: (obj: unknown, msg?: string) => void;
  warn: (obj: unknown, msg?: string) => void;
  error: (obj: unknown, msg?: string) => void;
  debug?: (obj: unknown, msg?: string) => void;
};

/** 发件箱逻辑对数据库的最小依赖面，PrismaClient / 事务客户端均可直接满足。 */
export type OutboxDb = {
  mediaTask: Prisma.MediaTaskDelegate;
};

export interface ClaimResult {
  /** 成功完成状态迁移（认领）的任务。 */
  claimed: Array<{ recordingId: string }>;
  /** 候选但状态已被其他进程抢先迁移的任务，无需再投递。 */
  skipped: Array<{ recordingId: string }>;
}

/**
 * 原子认领一批 PENDING 任务并迁移到 QUEUED。
 * 条件更新（WHERE status = 'PENDING'）保证多个 API/worker 进程并发运行时
 * 每个任务只会被一个进程投递，不会出现两份队列任务。
 */
export async function claimPendingTasks(
  db: OutboxDb,
  options: { limit: number; lockedBy: string },
): Promise<ClaimResult> {
  const candidates = await db.mediaTask.findMany({
    where: { status: 'PENDING' },
    orderBy: [{ createdAt: 'asc' }, { updatedAt: 'asc' }],
    take: options.limit,
    select: { recordingId: true },
  });

  const claimed: Array<{ recordingId: string }> = [];
  const skipped: Array<{ recordingId: string }> = [];

  for (const candidate of candidates) {
    const result = await db.mediaTask.updateMany({
      where: { recordingId: candidate.recordingId, status: 'PENDING' },
      data: { status: 'QUEUED', lockedAt: null, lockedBy: null },
    });
    (result.count > 0 ? claimed : skipped).push({ recordingId: candidate.recordingId });
  }

  return { claimed, skipped };
}

/**
 * 将任务投递到 BullMQ。固定 jobId = recordingId：
 * 即使因 Redis 故障恢复/多实例补偿导致重复调用，BullMQ 也只会保留一份任务，
 * 重试不会产生两份处理记录。
 */
export async function ensureMediaJob(
  queue: Queue,
  recordingId: string,
  options?: JobsOptions,
): Promise<boolean> {
  const job = await queue.add(
    MEDIA_JOB_NAME,
    { recordingId },
    { jobId: recordingId, ...(options ?? MEDIA_JOB_OPTIONS) },
  );
  return Boolean(job);
}

export interface DispatchOptions {
  limit?: number;
  /** 每次 ensureMediaJob 的最长等待时间，避免 Redis 长时间不可用时拖垮调用方。 */
  timeoutMs?: number;
  jobOptions?: JobsOptions;
}

/**
 * 认领 PENDING 发件箱任务并尽力投递到 Redis。
 * - Redis 不可用：任务保持/回退为 PENDING，等待下一轮 sweeper 或 Redis 恢复后补偿，不丢任务。
 * - 投递成功：任务迁移到 QUEUED，worker 正常消费。
 */
export async function dispatchPendingTasks(
  db: OutboxDb,
  queue: Queue,
  options: DispatchOptions = {},
): Promise<{ dispatched: number; pending: number }> {
  const limit = options.limit ?? 10;
  const timeoutMs = options.timeoutMs ?? 3000;
  const { claimed } = await claimPendingTasks(db, { limit, lockedBy: 'dispatcher' });

  let dispatched = 0;
  for (const task of claimed) {
    try {
      await withTimeout(
        ensureMediaJob(queue, task.recordingId, options.jobOptions),
        timeoutMs,
        'enqueue media job timed out',
      );
      dispatched += 1;
    } catch {
      // Redis 仍不可用：回退为 PENDING，下一轮（或对端进程）重新认领投递
      await db.mediaTask.updateMany({
        where: { recordingId: task.recordingId, status: 'QUEUED' },
        data: { status: 'PENDING' },
      });
    }
  }

  return { dispatched, pending: claimed.length - dispatched };
}

/**
 * 回收“已认领但迟迟没有心跳/锁陈旧”的 PROCESSING 任务。
 * 只回收还有重试次数的任务；到达上限的任务留在终态/由失败逻辑处理。
 *
 * attempts 只在 worker 真正开始处理时递增，这里不计数：
 * BullMQ 退避重试本身会再投递同一行，sweeper 只负责把“worker 崩溃、
 * 连重试机会都没有”的任务退回队列，两者不重复计数。
 *
 * 条件更新同样保证同一时刻只有一个进程会真正退回任务。
 */
export async function recoverStaleProcessing(
  db: OutboxDb,
  options: { staleMs: number; limit: number; lockedBy: string },
): Promise<Array<{ recordingId: string }>> {
  const staleBefore = new Date(Date.now() - options.staleMs);
  const candidates = await db.mediaTask.findMany({
    where: {
      status: 'PROCESSING',
      attempts: { lt: MEDIA_MAX_ATTEMPTS },
      OR: [{ lockedAt: null }, { lockedAt: { lt: staleBefore } }],
    },
    orderBy: { updatedAt: 'asc' },
    take: options.limit,
    select: { recordingId: true },
  });

  const recovered: Array<{ recordingId: string }> = [];
  for (const candidate of candidates) {
    const result = await db.mediaTask.updateMany({
      where: {
        recordingId: candidate.recordingId,
        status: 'PROCESSING',
        attempts: { lt: MEDIA_MAX_ATTEMPTS },
        OR: [{ lockedAt: null }, { lockedAt: { lt: staleBefore } }],
      },
      data: {
        status: 'PENDING',
        lockedAt: null,
        lockedBy: null,
      },
    });
    if (result.count > 0) {
      recovered.push({ recordingId: candidate.recordingId });
    }
  }
  return recovered;
}

/**
 * 修复 QUEUED 状态但 Redis 队列中实际不存在的任务
 * （如 Redis 被清空、投递成功后未收到响应、队列数据丢失）。
 * 无法连接 Redis 时本函数会整体失败，等待下一轮。
 */
export async function reconcileQueuedTasks(
  db: OutboxDb,
  queue: Queue,
  options: { limit: number },
): Promise<number> {
  const candidates = await db.mediaTask.findMany({
    where: { status: 'QUEUED' },
    orderBy: { updatedAt: 'asc' },
    take: options.limit,
    select: { recordingId: true },
  });
  if (candidates.length === 0) return 0;

  const existing = await queue.getJobs(['waiting', 'delayed', 'active', 'paused'], 0, 1000);
  const queuedIds = new Set(existing.map((job) => String(job.id)));

  let repaired = 0;
  for (const candidate of candidates) {
    if (queuedIds.has(candidate.recordingId)) continue;
    try {
      await ensureMediaJob(queue, candidate.recordingId);
      repaired += 1;
    } catch {
      // Redis 刚恢复但仍不稳定时等下一轮；行状态保持 QUEUED 不丢任务
    }
  }
  return repaired;
}

export interface StartOutboxSweeperOptions {
  intervalMs?: number;
  staleProcessingMs?: number;
  claimLimit?: number;
}

export interface OutboxSweeper {
  /** 手动触发一轮补偿（内部已吞掉异常，不会 reject）。 */
  tick: () => Promise<void>;
  start: () => void;
  stop: () => Promise<void>;
}

/**
 * 周期性发件箱补偿器。API 与 worker 都应各启动一个：
 * 即使上传进程在 DB 提交后立即崩溃、Redis 短暂不可用，
 * 任意存活进程都会把 PENDING 任务补投，并回收/修复异常状态。
 */
export function startOutboxSweeper(
  db: OutboxDb,
  queue: Queue,
  logger: OutboxLogger,
  options: StartOutboxSweeperOptions = {},
): OutboxSweeper {
  const intervalMs = options.intervalMs ?? 5000;
  const staleProcessingMs = options.staleProcessingMs ?? 2 * 60_000;
  const claimLimit = options.claimLimit ?? 10;

  let timer: ReturnType<typeof setInterval> | null = null;
  let running = false;

  const tick = async () => {
    if (running) return;
    running = true;
    try {
      const { dispatched, pending } = await dispatchPendingTasks(db, queue, {
        limit: claimLimit,
      });
      const recovered = await recoverStaleProcessing(db, {
        staleMs: staleProcessingMs,
        limit: claimLimit,
        lockedBy: 'sweeper',
      });
      let repaired = 0;
      try {
        repaired = await reconcileQueuedTasks(db, queue, { limit: claimLimit });
      } catch {
        // Redis 不可用时跳过一致性修复；PENDING 补投已覆盖“任务不存在”的主要场景
      }

      if (dispatched || pending || recovered.length || repaired) {
        logger.info?.(
          { dispatched, pending, recovered: recovered.length, repaired },
          'media outbox sweep complete',
        );
      }
    } catch (error) {
      logger.warn({ err: error }, 'media outbox sweep failed');
    } finally {
      running = false;
    }
  };

  return {
    tick,
    start() {
      if (timer) return;
      // 启动后立刻跑一轮，缩短恢复窗口
      void tick();
      timer = setInterval(() => void tick(), intervalMs);
    },
    async stop() {
      if (timer) clearInterval(timer);
      timer = null;
      await tick().catch(() => undefined);
    },
  };
}

function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
  });
  return Promise.race([promise.finally(() => clearTimeout(timer)), timeout]);
}

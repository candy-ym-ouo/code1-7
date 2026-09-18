import { Queue, Worker, type Job } from 'bullmq';
import { Redis } from 'ioredis';
import { PrismaClient } from '@prisma/client';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createHash } from 'node:crypto';
import { hostname } from 'node:os';
import { parseFile } from 'music-metadata';
import {
  MEDIA_JOB_OPTIONS,
  MEDIA_MAX_ATTEMPTS,
  startOutboxSweeper,
  type OutboxSweeper,
} from './outbox.js';

const execFileAsync = promisify(execFile);
const prisma = new PrismaClient();

// Queue（发件箱投递/对账）与 Worker 各用一条连接，避免 worker 的阻塞命令互相干扰
const queueRedis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
  maxRetriesPerRequest: null,
});
const workerRedis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
  maxRetriesPerRequest: null,
});

const mediaQueue = new Queue('media', { connection: queueRedis });
const workerId = `${hostname()}-${process.pid}-${createHash('sha1').update(String(Date.now())).digest('hex').slice(0, 8)}`;

const HEARTBEAT_MS = 10_000;

async function probeWithFfprobe(filePath: string): Promise<number> {
  const { stdout } = await execFileAsync(
    'ffprobe',
    [
      '-v',
      'error',
      '-show_entries',
      'format=duration',
      '-of',
      'default=noprint_wrappers=1:nokey=1',
      filePath,
    ],
    { timeout: 60_000, maxBuffer: 1024 * 1024 },
  );
  const seconds = Number.parseFloat(stdout.trim());
  if (!Number.isFinite(seconds) || seconds < 0) {
    throw new Error('ffprobe 未返回有效时长');
  }
  return Math.round(seconds * 1000);
}

async function probeWithMusicMetadata(filePath: string): Promise<number> {
  const metadata = await parseFile(filePath, { duration: true });
  const seconds = metadata.format.duration;
  if (seconds === undefined || !Number.isFinite(seconds) || seconds < 0) {
    throw new Error('无法读取音频时长');
  }
  return Math.round(seconds * 1000);
}

async function probeDurationMs(filePath: string): Promise<number> {
  try {
    return await probeWithFfprobe(filePath);
  } catch (ffprobeError) {
    try {
      return await probeWithMusicMetadata(filePath);
    } catch (metadataError) {
      const ffprobeMessage =
        ffprobeError instanceof Error ? ffprobeError.message : String(ffprobeError);
      const metadataMessage =
        metadataError instanceof Error ? metadataError.message : String(metadataError);
      throw new Error(
        `无法解析音频元数据（ffprobe: ${ffprobeMessage}; fallback: ${metadataMessage}）`,
      );
    }
  }
}

/**
 * 媒体处理。幂等性保证：
 * 1. BullMQ 中 jobId 固定为 recordingId，队列层最多一份任务；
 * 2. 条件认领（status IN (QUEUED, PENDING) 才允许进入 PROCESSING），
 *    重复交付/并发消费时后到者直接确认，不会重复处理；
 * 3. 每条录音在 MediaTask 表中只有一行（主键即 recordingId），
 *    重试只更新同一行，绝不产生第二份处理记录。
 */
async function processMediaJob(job: Job) {
  const recordingId = String(job.data?.recordingId || '');
  if (!recordingId) throw new Error('任务缺少 recordingId');

  const task = await prisma.mediaTask.findUnique({
    where: { recordingId },
    include: { recording: true },
  });
  if (!task) throw new Error(`媒体任务不存在: ${recordingId}`);
  if (task.status === 'COMPLETED') {
    return { recordingId, duplicated: true };
  }
  if (task.status === 'FAILED') {
    // 已到终态的重复交付直接确认，不复活失败任务
    return { recordingId, alreadyFailed: true };
  }

  const now = new Date();
  const claimData = task.attempts === 0 ? { startedAt: now } : {};
  const claim = await prisma.mediaTask.updateMany({
    where: { recordingId, status: { in: ['QUEUED', 'PENDING'] } },
    data: {
      status: 'PROCESSING',
      attempts: { increment: 1 },
      lockedAt: now,
      lockedBy: workerId,
      ...claimData,
    },
  });
  if (claim.count === 0) {
    // 另一个 worker 正在处理：直接确认本次交付，由持有者完成；
    // 若持有者已死，发件箱 sweeper 会凭陈旧锁回收后重新投递
    console.warn(`media job ${recordingId} already claimed by another worker, skipping`);
    return { recordingId, deferred: true };
  }

  await prisma.recording.update({
    where: { id: recordingId },
    data: { status: 'PROCESSING', processingError: null },
  });

  // 心跳：长时间 ffprobe 期间持续续租，防止 sweeper 误回收存活中的任务
  const heartbeat = setInterval(() => {
    void prisma.mediaTask
      .updateMany({
        where: { recordingId, status: 'PROCESSING', lockedBy: workerId },
        data: { lockedAt: new Date() },
      })
      .catch(() => undefined);
  }, HEARTBEAT_MS);

  try {
    const durationMs = await probeDurationMs(task.recording.originalPath);
    if (durationMs <= 0) throw new Error('音频时长为 0，无法进入编辑');

    // 任务结果与录音状态同一事务提交，避免一边 READY 一边仍显示处理中
    await prisma.$transaction([
      prisma.mediaTask.update({
        where: { id: recordingId },
        data: {
          status: 'COMPLETED',
          lastError: null,
          lockedAt: null,
          lockedBy: null,
          finishedAt: new Date(),
        },
      }),
      prisma.recording.update({
        where: { id: recordingId },
        data: {
          status: 'READY',
          durationMs,
          playbackPath: task.recording.playbackPath || task.recording.originalPath,
          processingError: null,
        },
      }),
    ]);

    return { recordingId, durationMs };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    // BullMQ 在执行处理器时抛出错误后才会递增 attemptsMade，故 +1 判断是否最后一次
    const isLastAttempt = job.attemptsMade + 1 >= MEDIA_MAX_ATTEMPTS;

    await prisma.$transaction([
      prisma.mediaTask.update({
        where: { id: recordingId },
        data: {
          // 非最后一次保持 PROCESSING（带最新错误），BullMQ 退避后重跑同一行；
          // 最后一次落 FAILED 终态，重试不会再产生新记录
          status: isLastAttempt ? 'FAILED' : 'PROCESSING',
          lastError: message,
          lockedAt: isLastAttempt ? null : new Date(),
          lockedBy: isLastAttempt ? null : workerId,
          finishedAt: isLastAttempt ? new Date() : null,
        },
      }),
      prisma.recording.update({
        where: { id: recordingId },
        data: {
          status: isLastAttempt ? 'FAILED' : 'PROCESSING',
          processingError: message,
        },
      }),
    ]);

    throw error;
  } finally {
    clearInterval(heartbeat);
  }
}

const worker = new Worker('media', processMediaJob, {
  connection: workerRedis,
  concurrency: 2,
});

// 发件箱补偿器：Redis 恢复后补投 PENDING，修复 QUEUED 丢任务，回收崩溃遗留锁。
// 与 API 侧的 sweeper 并发安全（认领/回收全部走条件更新）。
const outboxSweeper: OutboxSweeper = startOutboxSweeper(
  prisma,
  mediaQueue,
  {
    info: (obj: unknown, msg?: string) => console.log(msg ?? 'outbox', obj),
    warn: (obj: unknown, msg?: string) => console.warn(msg ?? 'outbox', obj),
    error: (obj: unknown, msg?: string) => console.error(msg ?? 'outbox', obj),
  },
  { intervalMs: 5000, staleProcessingMs: 2 * 60_000 },
);
outboxSweeper.start();

worker.on('completed', (job, result) => {
  console.log(`media job ${job.id} completed`, result ?? '');
});

worker.on('failed', (job, error) => {
  // 状态落库已在处理器内事务完成，这里只保留可观测日志
  console.error(`media job ${job?.id ?? '(unknown)'} failed: ${error.message}`);
});

worker.on('error', (error) => {
  console.error('media worker error', error);
});

let shuttingDown = false;
async function shutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`received ${signal}, shutting down media worker`);
  await outboxSweeper.stop();
  await worker.close();
  await mediaQueue.close();
  if (queueRedis.status !== 'end') queueRedis.disconnect();
  if (workerRedis.status !== 'end') workerRedis.disconnect();
  await prisma.$disconnect();
}

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

console.log(`media worker listening (workerId=${workerId}, jobOptions=${JSON.stringify(MEDIA_JOB_OPTIONS.attempts)})`);

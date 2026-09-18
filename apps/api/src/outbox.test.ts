import { describe, expect, it, vi } from 'vitest';
import {
  MEDIA_MAX_ATTEMPTS,
  claimPendingTasks,
  dispatchPendingTasks,
  recoverStaleProcessing,
  type OutboxDb,
} from './outbox.js';

function makeDb(
  rows: Array<{ recordingId: string; status: string; attempts?: number; lockedAt?: Date | null }>,
) {
  const data = rows.map((row) => ({ attempts: 0, lockedAt: null, ...row }));
  const updateMany = vi.fn(
    async (args: { where: Record<string, unknown>; data: Record<string, unknown> }) => {
      const index = data.findIndex((row) => {
        const where = args.where as {
          recordingId?: string;
          status?: string | { in?: string[] };
          attempts?: number | { lt?: number };
          OR?: Array<{ lockedAt?: null } | { lockedAt?: { lt?: Date } }>;
        };
        if (row.recordingId !== where.recordingId) return false;
        if (where.status !== undefined) {
          if (typeof where.status === 'string' && row.status !== where.status) return false;
          if (typeof where.status === 'object' && !where.status.in?.includes(row.status)) {
            return false;
          }
        }
        if (where.attempts !== undefined) {
          if (typeof where.attempts === 'number' && row.attempts !== where.attempts) {
            return false;
          }
          if (typeof where.attempts === 'object' && !(row.attempts < where.attempts.lt!)) {
            return false;
          }
        }
        if (where.OR) {
          const orMatch = where.OR.some((clause) => {
            if ('lockedAt' in clause && clause.lockedAt === null) {
              return row.lockedAt === null;
            }
            if ('lockedAt' in clause && clause.lockedAt?.lt) {
              return row.lockedAt !== null && row.lockedAt < clause.lockedAt.lt;
            }
            return false;
          });
          if (!orMatch) return false;
        }
        return true;
      });
      if (index >= 0) {
        Object.assign(data[index], args.data);
        const attemptPatch = args.data.attempts as { increment?: number } | undefined;
        if (attemptPatch && typeof attemptPatch === 'object' && 'increment' in attemptPatch) {
          data[index].attempts += attemptPatch.increment ?? 0;
        }
        return { count: 1 };
      }
      return { count: 0 };
    },
  );
  const db: OutboxDb = {
    mediaTask: {
      findMany: vi.fn(
        async (args?: {
          where?: {
            status?: string;
            attempts?: { lt?: number };
            OR?: Array<{ lockedAt?: null } | { lockedAt?: { lt?: Date } }>;
          };
        }) => {
          let rows = data;
          if (args?.where?.status) {
            rows = rows.filter((row) => row.status === args.where!.status);
          }
          if (args?.where?.attempts?.lt !== undefined) {
            rows = rows.filter((row) => row.attempts < args.where!.attempts!.lt!);
          }
          if (args?.where?.OR) {
            rows = rows.filter((row) =>
              args.where!.OR!.some((clause) => {
                if ('lockedAt' in clause && clause.lockedAt === null) {
                  return row.lockedAt === null;
                }
                if ('lockedAt' in clause && clause.lockedAt?.lt) {
                  return row.lockedAt !== null && row.lockedAt < clause.lockedAt.lt;
                }
                return false;
              }),
            );
          }
          return rows.map((row) => ({ ...row }));
        },
      ),
      update: vi.fn(),
      updateMany,
    } as unknown as OutboxDb['mediaTask'],
  };
  return { db, data };
}

describe('claimPendingTasks', () => {
  it('认领 PENDING 任务并迁移到 QUEUED，竞争者已抢先时记为 skipped', async () => {
    const { db } = makeDb([
      { recordingId: 'a', status: 'PENDING' },
      { recordingId: 'b', status: 'PENDING' },
    ]);

    // 第二个任务在本进程认领前已被别的进程迁移
    db.mediaTask.updateMany = vi
      .fn()
      .mockResolvedValueOnce({ count: 1 })
      .mockResolvedValueOnce({ count: 0 });

    const result = await claimPendingTasks(db, { limit: 10, lockedBy: 'test' });
    expect(result.claimed).toEqual([{ recordingId: 'a' }]);
    expect(result.skipped).toEqual([{ recordingId: 'b' }]);
  });
});

describe('dispatchPendingTasks', () => {
  it('Redis 不可用时任务回退为 PENDING，下一轮可重新投递', async () => {
    const { db, data } = makeDb([{ recordingId: 'r1', status: 'PENDING' }]);
    const queue = {
      add: vi.fn(async () => {
        throw new Error('ECONNREFUSED');
      }),
    };

    const result = await dispatchPendingTasks(db, queue as never, { timeoutMs: 50 });
    expect(result).toEqual({ dispatched: 0, pending: 1 });
    expect(data[0].status).toBe('PENDING');
  });

  it('投递超时时任务回退为 PENDING，不丢失也不重复计数', async () => {
    const { db, data } = makeDb([{ recordingId: 'r2', status: 'PENDING' }]);
    const queue = {
      add: vi.fn(() => new Promise((resolve) => setTimeout(() => resolve({ id: 'r2' }), 200))),
    };

    const result = await dispatchPendingTasks(db, queue as never, { timeoutMs: 30 });
    expect(result.dispatched).toBe(0);
    expect(data[0].status).toBe('PENDING');
  });

  it('投递成功后任务停留在 QUEUED，且 jobId 固定为 recordingId', async () => {
    const { db, data } = makeDb([{ recordingId: 'r3', status: 'PENDING' }]);
    const queue = { add: vi.fn(async () => ({ id: 'r3' })) };

    const result = await dispatchPendingTasks(db, queue as never, { timeoutMs: 100 });
    expect(result.dispatched).toBe(1);
    expect(data[0].status).toBe('QUEUED');
    expect(queue.add).toHaveBeenCalledWith(
      'media.process',
      { recordingId: 'r3' },
      expect.objectContaining({ jobId: 'r3' }),
    );
  });
});

describe('recoverStaleProcessing', () => {
  it('只回收锁陈旧且仍有重试次数的任务，回收不递增 attempts', async () => {
    const fresh = new Date();
    const stale = new Date(Date.now() - 10 * 60_000);
    const { db, data } = makeDb([
      { recordingId: 'fresh', status: 'PROCESSING', attempts: 1, lockedAt: fresh },
      { recordingId: 'stale', status: 'PROCESSING', attempts: 1, lockedAt: stale },
      {
        recordingId: 'exhausted',
        status: 'PROCESSING',
        attempts: MEDIA_MAX_ATTEMPTS,
        lockedAt: stale,
      },
    ]);

    const recovered = await recoverStaleProcessing(db, {
      staleMs: 60_000,
      limit: 10,
      lockedBy: 'test',
    });
    expect(recovered).toEqual([{ recordingId: 'stale' }]);

    const staleRow = data.find((row) => row.recordingId === 'stale');
    expect(staleRow?.status).toBe('PENDING');
    expect(staleRow?.lockedAt).toBeNull();
    // sweeper 回收不递增 attempts，真正重试次数只由 worker 认领时增加
    expect(staleRow?.attempts).toBe(1);
  });

  it('并发回收时按状态条件乐观锁，只有一方成功', async () => {
    const stale = new Date(Date.now() - 10 * 60_000);
    const { db } = makeDb([
      { recordingId: 'race', status: 'PROCESSING', attempts: 0, lockedAt: stale },
    ]);
    db.mediaTask.updateMany = vi
      .fn()
      .mockResolvedValueOnce({ count: 1 })
      .mockResolvedValueOnce({ count: 0 });

    const [first, second] = await Promise.all([
      recoverStaleProcessing(db, { staleMs: 60_000, limit: 10, lockedBy: 'a' }),
      recoverStaleProcessing(db, { staleMs: 60_000, limit: 10, lockedBy: 'b' }),
    ]);
    expect(first.length + second.length).toBe(1);
  });
});

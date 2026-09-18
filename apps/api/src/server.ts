import Fastify, { type FastifyReply, type FastifyRequest } from 'fastify';
import cors from '@fastify/cors';
import cookie from '@fastify/cookie';
import jwt from '@fastify/jwt';
import multipart from '@fastify/multipart';
import websocket from '@fastify/websocket';
import { Prisma, PrismaClient, Role } from '@prisma/client';
import argon2 from 'argon2';
import { createReadStream } from 'node:fs';
import { createWriteStream } from 'node:fs';
import { mkdir, stat, unlink } from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { pipeline } from 'node:stream/promises';
import { Queue } from 'bullmq';
import { Redis } from 'ioredis';
import { z } from 'zod';
import {
  MEDIA_JOB_OPTIONS,
  dispatchPendingTasks,
  startOutboxSweeper,
  type OutboxSweeper,
} from './outbox.js';
import {
  chapterBlockCreateSchema,
  chapterCreateSchema,
  chapterUpdateSchema,
  clipSchema,
  clipUpdateSchema,
} from '@history/contracts';

const MAX_UPLOAD_BYTES = 5 * 1024 * 1024 * 1024;
const ALLOWED_AUDIO_EXTENSIONS = /\.(aac|aiff|flac|m4a|mp3|mp4|oga|ogg|opus|wav|webm)$/i;
const DEFAULT_JWT_SECRET = 'development-secret-change-me-development';

class HttpError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly code: string,
    message: string,
    public readonly details?: unknown,
  ) {
    super(message);
    this.name = 'HttpError';
  }
}

const registerSchema = z
  .object({
    email: z.string().trim().email().max(254),
    password: z.string().min(8).max(128),
    displayName: z.string().trim().min(1).max(80).optional(),
  })
  .strict();

const loginSchema = z
  .object({
    email: z.string().trim().email().max(254),
    password: z.string().min(1).max(128),
  })
  .strict();

const workspaceCreateSchema = z
  .object({
    name: z.string().trim().min(1).max(120),
    timezone: z.string().trim().min(1).max(80).default('Asia/Shanghai'),
  })
  .strict();

const sequenceSchema = z.coerce.number().int().nonnegative().default(0);

type AuthUser = { id: string; email: string };
type JwtRequest = FastifyRequest & { user: AuthUser };
type DbClient = PrismaClient | Prisma.TransactionClient;

const currentDir = path.dirname(fileURLToPath(import.meta.url));
const repositoryRoot = path.resolve(currentDir, '../../..');
const configuredStorage = process.env.STORAGE_DIR || './storage';
const storage = path.isAbsolute(configuredStorage)
  ? configuredStorage
  : path.resolve(repositoryRoot, configuredStorage);

await mkdir(storage, { recursive: true });

const prisma = new PrismaClient();
const app = Fastify({ logger: true });
const port = Number(process.env.PORT || 4000);
const webOrigins = (process.env.WEB_ORIGIN || process.env.APP_ORIGIN || 'http://localhost:5173')
  .split(',')
  .map((origin) => origin.trim())
  .filter(Boolean);

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
  maxRetriesPerRequest: null,
});
redis.on('error', (error) => app.log.warn({ err: error }, 'Redis connection error'));

const mediaQueue = new Queue('media', { connection: redis });

// 发件箱补偿器：Redis 短暂不可用导致即时投递失败的任务，会在恢复后自动补投；
// 同时回收 worker 崩溃遗留的陈旧任务。API 与 worker 各跑一个，互不冲突。
const outboxSweeper: OutboxSweeper = startOutboxSweeper(
  prisma,
  mediaQueue,
  {
    info: (obj: unknown, msg?: string) => app.log.info(obj, msg),
    warn: (obj: unknown, msg?: string) => app.log.warn(obj, msg),
    error: (obj: unknown, msg?: string) => app.log.error(obj, msg),
    debug: (obj: unknown, msg?: string) => app.log.debug(obj, msg),
  },
  { intervalMs: 5000 },
);
outboxSweeper.start();

await app.register(cors, {
  origin: webOrigins,
  credentials: true,
  methods: ['GET', 'HEAD', 'POST', 'PATCH', 'DELETE', 'OPTIONS'],
});
await app.register(cookie);
await app.register(jwt, {
  secret: process.env.JWT_SECRET || DEFAULT_JWT_SECRET,
});
await app.register(multipart, {
  limits: { fileSize: MAX_UPLOAD_BYTES, files: 1, fields: 20 },
});
await app.register(websocket);

function validationError<T>(schema: z.ZodType<T>, value: unknown): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) {
    throw new HttpError(400, 'INVALID_INPUT', '输入格式不正确', parsed.error.flatten());
  }
  return parsed.data;
}

async function authenticate(req: FastifyRequest, reply: FastifyReply): Promise<void> {
  try {
    await req.jwtVerify();
  } catch {
    await reply.code(401).send({
      error: { code: 'UNAUTHENTICATED', message: '请先登录' },
    });
  }
}

function authUser(req: FastifyRequest): AuthUser {
  return req.user as AuthUser;
}

async function findMembership(workspaceId: string, userId: string, roles?: Role[]) {
  const membership = await prisma.workspaceMember.findUnique({
    where: { workspaceId_userId: { workspaceId, userId } },
  });
  if (!membership || (roles && !roles.includes(membership.role))) {
    return null;
  }
  return membership;
}

async function requireMembership(
  req: FastifyRequest,
  workspaceId: string,
  roles?: Role[],
): Promise<void> {
  const membership = await findMembership(workspaceId, authUser(req).id, roles);
  if (!membership) {
    throw new HttpError(404, 'NOT_FOUND', '资源不存在');
  }
}

async function recordEvent(
  db: DbClient,
  workspaceId: string,
  actorId: string,
  resourceType: string,
  resourceId: string,
  operation: string,
  payload: unknown,
) {
  return db.collaborationEvent.create({
    data: {
      workspaceId,
      actorId,
      resourceType,
      resourceId,
      operation,
      payloadJson: payload as Prisma.InputJsonValue,
    },
  });
}

function recordingDto<T extends { sizeBytes: bigint }>(recording: T) {
  return { ...recording, sizeBytes: recording.sizeBytes.toString() };
}

function parseByteRange(header: string, size: number): { start: number; end: number } | null {
  if (!header.startsWith('bytes=')) return null;
  const value = header.slice(6).trim();
  if (!value || value.includes(',')) return null;

  const match = /^(\d*)-(\d*)$/.exec(value);
  if (!match) return null;

  const [, startText, endText] = match;
  if (!startText && !endText) return null;

  if (!startText) {
    const suffixLength = Number(endText);
    if (!Number.isSafeInteger(suffixLength) || suffixLength <= 0) return null;
    const start = Math.max(size - suffixLength, 0);
    return { start, end: size - 1 };
  }

  const start = Number(startText);
  const requestedEnd = endText ? Number(endText) : size - 1;
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(requestedEnd) ||
    start < 0 ||
    start >= size ||
    requestedEnd < start
  ) {
    return null;
  }

  return { start, end: Math.min(requestedEnd, size - 1) };
}

async function resolveRequestUser(req: FastifyRequest): Promise<AuthUser | null> {
  const header = req.headers.authorization;
  const queryToken = typeof (req.query as { token?: unknown } | undefined)?.token === 'string'
    ? (req.query as { token: string }).token
    : undefined;
  const token = header?.startsWith('Bearer ') ? header.slice(7) : queryToken;
  if (!token) return null;

  try {
    return await app.jwt.verify<AuthUser>(token);
  } catch {
    return null;
  }
}

app.setErrorHandler((error, req, reply) => {
  if (error instanceof HttpError) {
    return reply.code(error.statusCode).send({
      error: { code: error.code, message: error.message, details: error.details },
      requestId: req.id,
    });
  }

  if (error instanceof Prisma.PrismaClientKnownRequestError) {
    if (error.code === 'P2002') {
      return reply.code(409).send({
        error: { code: 'CONFLICT', message: '数据已存在' },
        requestId: req.id,
      });
    }
    if (error.code === 'P2025') {
      return reply.code(404).send({
        error: { code: 'NOT_FOUND', message: '资源不存在' },
        requestId: req.id,
      });
    }
  }

  const unknownError = error as {
    statusCode?: number;
    code?: string;
    message?: string;
  };
  const statusCode =
    unknownError.statusCode && unknownError.statusCode < 500
      ? unknownError.statusCode
      : 500;
  if (statusCode >= 500) {
    req.log.error({ err: error }, 'request failed');
  }

  return reply.code(statusCode).send({
    error: {
      code:
        statusCode >= 500
          ? 'INTERNAL_SERVER_ERROR'
          : unknownError.code || 'BAD_REQUEST',
      message:
        statusCode >= 500 ? '服务器内部错误' : unknownError.message || '请求失败',
    },
    requestId: req.id,
  });
});

app.setNotFoundHandler((req, reply) =>
  reply.code(404).send({
    error: { code: 'NOT_FOUND', message: '接口不存在' },
    requestId: req.id,
  }),
);

app.get('/health', async () => ({ ok: true }));

app.get('/ready', async (_req, reply) => {
  try {
    await prisma.$queryRaw`SELECT 1`;
    if (redis.status !== 'ready') await redis.ping();
    return { ok: true };
  } catch {
    return reply.code(503).send({ ok: false });
  }
});

app.post('/v1/auth/register', async (req, reply) => {
  const body = validationError(registerSchema, req.body);
  const passwordHash = await argon2.hash(body.password);

  try {
    const user = await prisma.user.create({
      data: {
        email: body.email.toLowerCase(),
        passwordHash,
        displayName: body.displayName || body.email.split('@')[0],
      },
      select: { id: true, email: true, displayName: true },
    });
    const token = await app.jwt.sign(
      { id: user.id, email: user.email },
      { expiresIn: '2h' },
    );
    return reply.code(201).send({ data: { token, user } });
  } catch (error) {
    if (
      error instanceof Prisma.PrismaClientKnownRequestError &&
      error.code === 'P2002'
    ) {
      return reply.code(409).send({
        error: { code: 'EMAIL_EXISTS', message: '邮箱已注册' },
      });
    }
    throw error;
  }
});

app.post('/v1/auth/login', async (req, reply) => {
  const body = validationError(loginSchema, req.body);
  const user = await prisma.user.findUnique({
    where: { email: body.email.toLowerCase() },
  });

  if (!user || !(await argon2.verify(user.passwordHash, body.password))) {
    return reply.code(401).send({
      error: { code: 'INVALID_CREDENTIALS', message: '邮箱或密码错误' },
    });
  }

  const token = await app.jwt.sign(
    { id: user.id, email: user.email },
    { expiresIn: '2h' },
  );
  return {
    data: {
      token,
      user: { id: user.id, email: user.email, displayName: user.displayName },
    },
  };
});

app.get('/v1/me', { preHandler: authenticate }, async (req) => ({
  data: await prisma.user.findUnique({
    where: { id: authUser(req).id },
    select: { id: true, email: true, displayName: true },
  }),
}));

app.post('/v1/workspaces', { preHandler: authenticate }, async (req, reply) => {
  const body = validationError(workspaceCreateSchema, req.body);
  const user = authUser(req);
  const workspace = await prisma.workspace.create({
    data: {
      name: body.name,
      timezone: body.timezone,
      ownerId: user.id,
      members: { create: { userId: user.id, role: Role.OWNER } },
    },
  });
  return reply.code(201).send({ data: workspace });
});

app.get('/v1/workspaces', { preHandler: authenticate }, async (req) => {
  const user = authUser(req);
  return {
    data: await prisma.workspace.findMany({
      where: { members: { some: { userId: user.id } }, deletedAt: null },
      orderBy: { updatedAt: 'desc' },
    }),
  };
});

app.post(
  '/v1/workspaces/:id/recordings/uploads',
  { preHandler: authenticate },
  async (req, reply) => {
    const workspaceId = (req.params as { id: string }).id;
    await requireMembership(req, workspaceId, [Role.OWNER, Role.EDITOR]);

    // 客户端重试（网络抖动、超时重发）携带相同键时，返回第一份录音而不是再建一条
    const idempotencyKey = parseIdempotencyKey(req.headers['idempotency-key']);
    if (idempotencyKey) {
      const existing = await prisma.recording.findUnique({
        where: { uploadKey: `${workspaceId}:${idempotencyKey}` },
      });
      if (existing) {
        return reply.send({ data: recordingDto(existing) });
      }
    }

    let upload;
    try {
      upload = await req.file();
    } catch (error) {
      if (error instanceof app.multipartErrors.RequestFileTooLargeError) {
        throw new HttpError(413, 'FILE_TOO_LARGE', '文件超过 5 GB 限制');
      }
      throw error;
    }

    if (!upload) {
      throw new HttpError(400, 'FILE_REQUIRED', '请选择录音文件');
    }

    const originalName = path.basename(upload.filename || 'recording');
    const hasAudioMime =
      upload.mimetype.startsWith('audio/') ||
      ['application/ogg', 'video/mp4', 'video/webm'].includes(upload.mimetype);
    if (!hasAudioMime || !ALLOWED_AUDIO_EXTENSIONS.test(originalName)) {
      upload.file.resume();
      throw new HttpError(
        415,
        'UNSUPPORTED_MEDIA_TYPE',
        '只支持常见音频文件（mp3、wav、m4a、aac、flac、ogg、opus、webm）',
      );
    }

    const id = crypto.randomUUID();
    const safeName = originalName.replace(/[^a-zA-Z0-9._-]/g, '_').slice(-180) || 'recording';
    const filePath = path.join(storage, `${id}-${safeName}`);
    let recordingPersisted = false;

    try {
      await pipeline(upload.file, createWriteStream(filePath, { flags: 'wx' }));
      if (upload.file.truncated) {
        throw new HttpError(413, 'FILE_TOO_LARGE', '文件超过 5 GB 限制');
      }

      const fileStat = await stat(filePath);
      if (fileStat.size <= 0) {
        throw new HttpError(400, 'EMPTY_FILE', '录音文件为空');
      }

      // 录音与媒体任务在同一事务落库：只要事务提交成功，任务就绝不会丢。
      // Redis 此刻是否可用不影响入库；投递失败由发件箱 sweeper 在 Redis 恢复后补偿。
      let recording;
      try {
        recording = await prisma.$transaction(async (tx) => {
          const created = await tx.recording.create({
            data: {
              id,
              workspaceId,
              title: originalName,
              originalPath: filePath,
              mimeType: upload.mimetype,
              sizeBytes: BigInt(fileStat.size),
              durationMs: 0,
              createdById: authUser(req).id,
              status: 'PROCESSING',
              uploadKey: idempotencyKey ? `${workspaceId}:${idempotencyKey}` : null,
            },
          });
          await tx.mediaTask.create({
            data: { recordingId: created.id, status: 'PENDING' },
          });
          return created;
        });
      } catch (error) {
        // 并发重放（两个请求带相同 Idempotency-Key 同时到达）：
        // 唯一索引挡住第二份，删除刚写入的文件并返回已存在的录音
        if (
          error instanceof Prisma.PrismaClientKnownRequestError &&
          error.code === 'P2002' &&
          (error.meta?.target as string[] | undefined)?.includes('uploadKey')
        ) {
          await unlink(filePath).catch(() => undefined);
          const winner = await prisma.recording.findUniqueOrThrow({
            where: { uploadKey: `${workspaceId}:${idempotencyKey}` },
          });
          return reply.send({ data: recordingDto(winner) });
        }
        throw error;
      }

      recordingPersisted = true;

      // 尽力即时投递，让常见路径保持“上传后立刻开始处理”。
      // 失败/超时只记日志：任务仍是 PENDING， sweeper 会补投，接口仍返回 201。
      try {
        await dispatchPendingTasks(prisma, mediaQueue, {
          limit: 1,
          timeoutMs: 2500,
          jobOptions: MEDIA_JOB_OPTIONS,
        });
      } catch (error) {
        req.log.warn({ err: error, recordingId: recording.id }, 'media outbox dispatch deferred');
      }

      return reply.code(201).send({ data: recordingDto(recording) });
    } catch (error) {
      if (!recordingPersisted) {
        await unlink(filePath).catch(() => undefined);
      }
      throw error;
    }
  },
);

function parseIdempotencyKey(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const key = value.trim();
  // 可打印 ASCII、长度 8~128，防止滥用超长键或注入控制字符
  if (key.length < 8 || key.length > 128) return null;
  return /^[\x21-\x7e]+$/.test(key) ? key : null;
}

app.get('/v1/workspaces/:id/recordings', { preHandler: authenticate }, async (req) => {
  const workspaceId = (req.params as { id: string }).id;
  await requireMembership(req, workspaceId);
  const rows = await prisma.recording.findMany({
    where: { workspaceId },
    include: {
      _count: { select: { clips: { where: { deletedAt: null } } } },
    },
    orderBy: { createdAt: 'desc' },
  });
  return { data: rows.map(recordingDto) };
});

app.get('/v1/recordings/:id/file', async (req, reply) => {
  const recordingId = (req.params as { id: string }).id;
  const recording = await prisma.recording.findUnique({ where: { id: recordingId } });
  if (!recording) {
    throw new HttpError(404, 'NOT_FOUND', '录音不存在');
  }

  const user = await resolveRequestUser(req);
  if (!user || !(await findMembership(recording.workspaceId, user.id))) {
    throw new HttpError(404, 'NOT_FOUND', '录音不存在');
  }

  const filePath = recording.playbackPath || recording.originalPath;
  let fileStat;
  try {
    fileStat = await stat(filePath);
  } catch {
    throw new HttpError(404, 'FILE_NOT_FOUND', '录音文件不存在');
  }

  const rangeHeader = req.headers.range;
  const range = rangeHeader ? parseByteRange(rangeHeader, fileStat.size) : null;
  if (rangeHeader && !range) {
    return reply
      .code(416)
      .header('Content-Range', `bytes */${fileStat.size}`)
      .send();
  }

  reply
    .type(recording.mimeType || 'application/octet-stream')
    .header('Accept-Ranges', 'bytes')
    .header('Cache-Control', 'private, no-store')
    .header('X-Content-Type-Options', 'nosniff');

  if (!range) {
    return reply
      .header('Content-Length', fileStat.size)
      .send(createReadStream(filePath));
  }

  return reply
    .code(206)
    .header('Content-Length', range.end - range.start + 1)
    .header('Content-Range', `bytes ${range.start}-${range.end}/${fileStat.size}`)
    .send(createReadStream(filePath, { start: range.start, end: range.end }));
});

app.get('/v1/recordings/:id/clips', { preHandler: authenticate }, async (req) => {
  const recordingId = (req.params as { id: string }).id;
  const recording = await prisma.recording.findUnique({ where: { id: recordingId } });
  if (!recording) {
    throw new HttpError(404, 'NOT_FOUND', '录音不存在');
  }
  await requireMembership(req, recording.workspaceId);

  return {
    data: await prisma.clip.findMany({
      where: { recordingId, deletedAt: null },
      orderBy: { startMs: 'asc' },
    }),
  };
});

app.post('/v1/recordings/:id/clips', { preHandler: authenticate }, async (req, reply) => {
  const recordingId = (req.params as { id: string }).id;
  const recording = await prisma.recording.findUnique({ where: { id: recordingId } });
  if (!recording) {
    throw new HttpError(404, 'NOT_FOUND', '录音不存在');
  }
  await requireMembership(req, recording.workspaceId, [Role.OWNER, Role.EDITOR]);

  if (recording.status !== 'READY' || recording.durationMs <= 0) {
    throw new HttpError(409, 'RECORDING_NOT_READY', '录音仍在处理中');
  }

  const parsed = clipSchema.safeParse(req.body);
  if (!parsed.success) {
    throw new HttpError(400, 'INVALID_CLIP', '片段时间范围无效', parsed.error.flatten());
  }
  if (parsed.data.endMs > recording.durationMs) {
    throw new HttpError(400, 'INVALID_CLIP', '片段超出录音时长');
  }
  if (parsed.data.speakerPersonId) {
    const person = await prisma.person.findFirst({
      where: {
        id: parsed.data.speakerPersonId,
        workspaceId: recording.workspaceId,
        deletedAt: null,
      },
    });
    if (!person) {
      throw new HttpError(400, 'INVALID_SPEAKER', '关联人物不存在');
    }
  }

  const { version: _version, ...clipData } = parsed.data;
  const user = authUser(req);
  const clip = await prisma.$transaction(async (tx) => {
    const created = await tx.clip.create({
      data: {
        ...clipData,
        recordingId: recording.id,
        workspaceId: recording.workspaceId,
        createdById: user.id,
        updatedById: user.id,
        speakerPersonId: clipData.speakerPersonId ?? null,
      },
    });
    await recordEvent(
      tx,
      recording.workspaceId,
      user.id,
      'clip',
      created.id,
      'created',
      created,
    );
    return created;
  });

  return reply.code(201).send({ data: clip });
});

app.patch('/v1/clips/:id', { preHandler: authenticate }, async (req, reply) => {
  const clipId = (req.params as { id: string }).id;
  const old = await prisma.clip.findUnique({ where: { id: clipId } });
  if (!old || old.deletedAt) {
    throw new HttpError(404, 'NOT_FOUND', '片段不存在');
  }
  await requireMembership(req, old.workspaceId, [Role.OWNER, Role.EDITOR]);

  const parsed = clipUpdateSchema.safeParse(req.body);
  if (!parsed.success) {
    throw new HttpError(400, 'INVALID_CLIP', '片段数据无效', parsed.error.flatten());
  }

  const recording = await prisma.recording.findUnique({ where: { id: old.recordingId } });
  if (!recording || recording.status !== 'READY') {
    throw new HttpError(409, 'RECORDING_NOT_READY', '录音尚未就绪');
  }

  const startMs = parsed.data.startMs ?? old.startMs;
  const endMs = parsed.data.endMs ?? old.endMs;
  if (startMs < 0 || endMs <= startMs || endMs > recording.durationMs) {
    throw new HttpError(400, 'INVALID_CLIP', '片段时间范围无效或超出录音时长');
  }

  const speakerPersonId =
    parsed.data.speakerPersonId === undefined
      ? old.speakerPersonId
      : parsed.data.speakerPersonId;
  if (speakerPersonId) {
    const person = await prisma.person.findFirst({
      where: { id: speakerPersonId, workspaceId: old.workspaceId, deletedAt: null },
    });
    if (!person) {
      throw new HttpError(400, 'INVALID_SPEAKER', '关联人物不存在');
    }
  }

  const user = authUser(req);
  const updated = await prisma.$transaction(async (tx) => {
    const result = await tx.clip.updateMany({
      where: { id: old.id, version: parsed.data.version, deletedAt: null },
      data: {
        title: parsed.data.title ?? old.title,
        startMs,
        endMs,
        summary: parsed.data.summary ?? old.summary,
        transcript: parsed.data.transcript ?? old.transcript,
        speakerPersonId,
        updatedById: user.id,
        version: { increment: 1 },
      },
    });

    if (result.count === 0) return null;
    const current = await tx.clip.findUniqueOrThrow({ where: { id: old.id } });
    await recordEvent(
      tx,
      old.workspaceId,
      user.id,
      'clip',
      current.id,
      'updated',
      current,
    );
    return current;
  });

  if (!updated) {
    const server = await prisma.clip.findUnique({ where: { id: old.id } });
    return reply.code(409).send({
      error: {
        code: 'CLIP_VERSION_CONFLICT',
        message: '片段已被其他成员修改',
        details: { server },
      },
    });
  }

  return { data: updated };
});

app.get('/v1/workspaces/:id/chapters', { preHandler: authenticate }, async (req) => {
  const workspaceId = (req.params as { id: string }).id;
  await requireMembership(req, workspaceId);
  return {
    data: await prisma.chapter.findMany({
      where: { workspaceId },
      include: {
        blocks: {
          orderBy: { position: 'asc' },
          include: { clip: { where: { deletedAt: null } } },
        },
      },
      orderBy: { updatedAt: 'desc' },
    }),
  };
});

app.post('/v1/workspaces/:id/chapters', { preHandler: authenticate }, async (req, reply) => {
  const workspaceId = (req.params as { id: string }).id;
  await requireMembership(req, workspaceId, [Role.OWNER, Role.EDITOR]);
  const body = validationError(chapterCreateSchema, req.body);
  const user = authUser(req);

  const chapter = await prisma.$transaction(async (tx) => {
    const created = await tx.chapter.create({
      data: {
        workspaceId,
        title: body.title,
        intro: body.intro,
        createdById: user.id,
      },
    });
    await recordEvent(tx, workspaceId, user.id, 'chapter', created.id, 'created', created);
    return created;
  });
  return reply.code(201).send({ data: chapter });
});

app.patch('/v1/chapters/:id', { preHandler: authenticate }, async (req, reply) => {
  const chapterId = (req.params as { id: string }).id;
  const old = await prisma.chapter.findUnique({ where: { id: chapterId } });
  if (!old) throw new HttpError(404, 'NOT_FOUND', '章节不存在');
  await requireMembership(req, old.workspaceId, [Role.OWNER, Role.EDITOR]);

  const body = validationError(chapterUpdateSchema, req.body);
  const user = authUser(req);
  const updated = await prisma.$transaction(async (tx) => {
    const result = await tx.chapter.updateMany({
      where: { id: old.id, version: body.version },
      data: {
        title: body.title ?? old.title,
        intro: body.intro ?? old.intro,
        version: { increment: 1 },
      },
    });
    if (result.count === 0) return null;

    const current = await tx.chapter.findUniqueOrThrow({ where: { id: old.id } });
    await recordEvent(
      tx,
      old.workspaceId,
      user.id,
      'chapter',
      current.id,
      'updated',
      current,
    );
    return current;
  });

  if (!updated) {
    const server = await prisma.chapter.findUnique({ where: { id: old.id } });
    return reply.code(409).send({
      error: {
        code: 'CHAPTER_VERSION_CONFLICT',
        message: '章节已被其他成员修改',
        details: { server },
      },
    });
  }
  return { data: updated };
});

app.post('/v1/chapters/:id/blocks', { preHandler: authenticate }, async (req, reply) => {
  const chapterId = (req.params as { id: string }).id;
  const chapter = await prisma.chapter.findUnique({ where: { id: chapterId } });
  if (!chapter) throw new HttpError(404, 'NOT_FOUND', '章节不存在');
  await requireMembership(req, chapter.workspaceId, [Role.OWNER, Role.EDITOR]);

  const body = validationError(chapterBlockCreateSchema, req.body);
  if (body.clipId) {
    const clip = await prisma.clip.findFirst({
      where: {
        id: body.clipId,
        workspaceId: chapter.workspaceId,
        deletedAt: null,
      },
    });
    if (!clip) throw new HttpError(400, 'INVALID_CLIP', '关联片段不存在');
  }

  const user = authUser(req);
  const block = await prisma.$transaction(async (tx) => {
    const created = await tx.chapterBlock.create({
      data: {
        chapterId: chapter.id,
        type: body.type ?? 'paragraph',
        position:
          body.position ||
          `${Date.now().toString(36)}-${crypto.randomUUID().slice(0, 8)}`,
        contentJson: (body.content ?? {}) as Prisma.InputJsonValue,
        clipId: body.clipId ?? null,
      },
    });
    await recordEvent(
      tx,
      chapter.workspaceId,
      user.id,
      'chapterBlock',
      created.id,
      'created',
      created,
    );
    return created;
  });
  return reply.code(201).send({ data: block });
});

app.post('/v1/chapters/:id/publish', { preHandler: authenticate }, async (req, reply) => {
  const chapterId = (req.params as { id: string }).id;
  const chapter = await prisma.chapter.findUnique({
    where: { id: chapterId },
    include: {
      blocks: {
        include: {
          clip: {
            include: { recording: { select: { status: true } } },
          },
        },
      },
    },
  });
  if (!chapter) throw new HttpError(404, 'NOT_FOUND', '章节不存在');
  await requireMembership(req, chapter.workspaceId, [Role.OWNER, Role.EDITOR]);

  if (chapter.blocks.length === 0) {
    throw new HttpError(
      400,
      'PUBLISH_VALIDATION_FAILED',
      '章节至少需要一个内容块',
    );
  }
  const invalidBlock = chapter.blocks.find(
    (block) =>
      block.clip &&
      (block.clip.deletedAt !== null || block.clip.recording.status !== 'READY'),
  );
  if (invalidBlock) {
    throw new HttpError(
      400,
      'PUBLISH_VALIDATION_FAILED',
      '章节包含不可播放或已归档的片段',
    );
  }

  const user = authUser(req);
  const published = await prisma.$transaction(async (tx) => {
    const updated = await tx.chapter.update({
      where: { id: chapter.id },
      data: { status: 'PUBLISHED', version: { increment: 1 } },
    });
    await recordEvent(
      tx,
      chapter.workspaceId,
      user.id,
      'chapter',
      updated.id,
      'published',
      updated,
    );
    return updated;
  });
  return { data: published };
});

app.get('/v1/workspaces/:id/events', { preHandler: authenticate }, async (req) => {
  const workspaceId = (req.params as { id: string }).id;
  await requireMembership(req, workspaceId);
  const query = validationError(
    z.object({ afterSequence: sequenceSchema }).strict(),
    req.query,
  );
  return {
    data: await prisma.collaborationEvent.findMany({
      where: {
        workspaceId,
        sequence: { gt: query.afterSequence },
      },
      orderBy: { sequence: 'asc' },
      take: 500,
    }),
  };
});

app.get('/v1/realtime', { websocket: true }, async (socket: any, req: any) => {
  const user = await resolveRequestUser(req);
  const workspaceId = String(req.query?.workspaceId || '');
  if (
    !user ||
    !workspaceId ||
    !(await findMembership(workspaceId, user.id))
  ) {
    socket.close(1008, 'forbidden');
    return;
  }

  socket.send(JSON.stringify({ type: 'connected', at: new Date().toISOString() }));
  socket.on('message', (raw: Buffer) => {
    try {
      const message = JSON.parse(raw.toString()) as { type?: string };
      if (message.type === 'ping') {
        socket.send(JSON.stringify({ type: 'pong', at: new Date().toISOString() }));
      }
    } catch {
      socket.send(JSON.stringify({ type: 'error', message: '消息格式错误' }));
    }
  });
});

app.addHook('onClose', async () => {
  await outboxSweeper.stop();
  await mediaQueue.close();
  if (redis.status !== 'end') redis.disconnect();
  await prisma.$disconnect();
});

try {
  await app.listen({ port, host: '0.0.0.0' });
} catch (error) {
  app.log.error(error);
  process.exit(1);
}

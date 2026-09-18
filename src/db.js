'use strict';

/**
 * 录音入库 + 媒体任务 outbox 的持久层。
 *
 * 关键设计（可恢复事务 / Transactional Outbox）：
 * 1. recordings 与 media_tasks 在同一个 SQLite 事务里提交，不存在"库写了任务没写"。
 * 2. media_tasks 是 outbox：事务提交后再由 relay 投递 Redis。Redis 不可用时
 *    行保持 pending，relay 会一直重试 —— 任务不会丢。
 * 3. 所有写操作都带幂等键：
 *    - recordings.id 由调用方传入（录音文件的业务 UUID）
 *    - media_tasks.id 由 (task_type, recording_id) 确定性派生
 *    客户端/网关重试同一个入库请求，INSERT OR IGNORE 命中，不会产生第二份数据。
 * 4. processing_records.task_id 唯一：同一条任务无论被投递/重投多少次，
 *    只会有一条处理记录（消费者侧的最终幂等防线）。
 */
const Database = require('better-sqlite3');

const SCHEMA = `
CREATE TABLE IF NOT EXISTS recordings (
  id          TEXT PRIMARY KEY,
  call_id     TEXT NOT NULL,
  file_uri    TEXT NOT NULL,
  duration_ms INTEGER,
  metadata    TEXT NOT NULL DEFAULT '{}',
  created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS media_tasks (
  id           TEXT PRIMARY KEY,                  -- <taskType>:<recordingId>
  recording_id TEXT NOT NULL REFERENCES recordings(id),
  task_type    TEXT NOT NULL,
  payload      TEXT NOT NULL,                     -- JSON
  status       TEXT NOT NULL DEFAULT 'pending',   -- pending | delivered
  attempts     INTEGER NOT NULL DEFAULT 0,        -- relay 尝试次数（含成功的那次）
  last_error   TEXT,
  created_at   TEXT NOT NULL,
  delivered_at TEXT,
  UNIQUE (recording_id, task_type)
);

-- 媒体处理记录：一条任务一行，重投/崩溃恢复都复用这一行
CREATE TABLE IF NOT EXISTS processing_records (
  task_id      TEXT PRIMARY KEY,
  status       TEXT NOT NULL DEFAULT 'processing', -- processing | done | failed
  attempts     INTEGER NOT NULL DEFAULT 0,         -- handler 实际执行次数
  result       TEXT,
  error        TEXT,
  claimed_at   TEXT NOT NULL,
  processed_at TEXT,
  created_at   TEXT NOT NULL
);

-- 超过最大重试次数的任务落死信表，仍然不丢，可人工/定时补偿
CREATE TABLE IF NOT EXISTS dead_letters (
  task_id    TEXT PRIMARY KEY,
  payload    TEXT NOT NULL,
  error      TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_media_tasks_status
  ON media_tasks(status, created_at);
`;

function openDb(filename) {
  const db = new Database(filename);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  db.exec(SCHEMA);
  return db;
}

function taskIdFor(taskType, recordingId) {
  return `${taskType}:${recordingId}`;
}

function nowIso() {
  return new Date().toISOString();
}

function createRepository(db) {
  const insertRecording = db.prepare(`
    INSERT OR IGNORE INTO recordings (id, call_id, file_uri, duration_ms, metadata, created_at)
    VALUES (@id, @call_id, @file_uri, @duration_ms, @metadata, @created_at)
  `);

  const insertTask = db.prepare(`
    INSERT OR IGNORE INTO media_tasks (id, recording_id, task_type, payload, created_at)
    VALUES (@id, @recording_id, @task_type, @payload, @created_at)
  `);

  const selectTasksByRecording = db.prepare(
    `SELECT id, recording_id AS recordingId, task_type AS taskType, payload, status, attempts
       FROM media_tasks WHERE recording_id = ? ORDER BY id`
  );

  const listPendingTasks = db.prepare(`
    SELECT id, recording_id AS recordingId, task_type AS taskType, payload, attempts
      FROM media_tasks
     WHERE status = 'pending'
     ORDER BY created_at
     LIMIT ?
  `);

  const markDelivered = db.prepare(`
    UPDATE media_tasks
       SET status = 'delivered',
           attempts = attempts + 1,
           last_error = NULL,
           delivered_at = @now
     WHERE id = @id AND status = 'pending'
  `);

  const recordRelayFailure = db.prepare(`
    UPDATE media_tasks
       SET attempts = attempts + 1,
           last_error = @error
     WHERE id = @id AND status = 'pending'
  `);

  const claimProcessing = db.prepare(`
    INSERT OR IGNORE INTO processing_records (task_id, status, attempts, claimed_at, created_at)
    VALUES (?, 'processing', 0, ?, ?)
  `);

  const selectProcessing = db.prepare(
    `SELECT task_id AS taskId, status, attempts, result, error
       FROM processing_records WHERE task_id = ?`
  );

  const finishProcessing = db.prepare(`
    UPDATE processing_records
       SET status = 'done', attempts = attempts + 1, result = @result,
           error = NULL, processed_at = @now
     WHERE task_id = @taskId AND status != 'done'
  `);

  const requeueProcessing = db.prepare(`
    UPDATE processing_records
       SET status = 'processing', attempts = attempts + 1, error = @error, claimed_at = @now
     WHERE task_id = @taskId AND status != 'done'
  `);

  const failProcessing = db.prepare(`
    UPDATE processing_records
       SET status = 'failed', attempts = attempts + 1, error = @error, processed_at = @now
     WHERE task_id = @taskId AND status != 'done'
  `);

  const insertDeadLetter = db.prepare(`
    INSERT OR IGNORE INTO dead_letters (task_id, payload, error, created_at)
    VALUES (@taskId, @payload, @error, @now)
  `);

  /**
   * 录音入库：recordings + media_tasks 在一个事务里提交。
   * 重复请求（同一 recordingId）整体幂等，返回 repeated=true，不产生第二份记录。
   *
   * @param {object} rec
   * @param {string} rec.id         录音业务 UUID（幂等键，必传）
   * @param {string} rec.callId
   * @param {string} rec.fileUri
   * @param {number} [rec.durationMs]
   * @param {object} [rec.metadata]
   * @param {Array<{type:string, payload?:object}>} [rec.tasks] 媒体任务，默认转写
   */
  function ingestRecording(rec) {
    if (!rec || !rec.id) throw new Error('recording.id is required (idempotency key)');
    if (!rec.callId) throw new Error('recording.callId is required');
    if (!rec.fileUri) throw new Error('recording.fileUri is required');

    const tasks = rec.tasks && rec.tasks.length
      ? rec.tasks
      : [{ type: 'transcribe', payload: { recordingId: rec.id, fileUri: rec.fileUri } }];

    const ts = nowIso();
    const recordingRow = {
      id: rec.id,
      call_id: rec.callId,
      file_uri: rec.fileUri,
      duration_ms: rec.durationMs ?? null,
      metadata: JSON.stringify(rec.metadata ?? {}),
      created_at: ts,
    };

    const taskRows = tasks.map((t) => {
      if (!t || !t.type) throw new Error('each task requires a type');
      return {
        id: taskIdFor(t.type, rec.id),
        recording_id: rec.id,
        task_type: t.type,
        payload: JSON.stringify(t.payload ?? { recordingId: rec.id, fileUri: rec.fileUri }),
        created_at: ts,
      };
    });

    const tx = db.transaction(() => {
      const recordingInserted = insertRecording.run(recordingRow).changes === 1;
      let insertedCount = 0;
      for (const row of taskRows) {
        insertedCount += insertTask.run(row).changes;
      }
      // 录音行已存在但任务行是新的（旧版本只写了部分任务的补偿场景），也视为首次写入这些任务
      return { recordingInserted, insertedCount };
    });

    const { insertedCount } = tx();
    const savedTasks = selectTasksByRecording.all(rec.id).map((t) => ({
      ...t,
      payload: JSON.parse(t.payload),
    }));

    return {
      recording: {
        id: rec.id,
        callId: rec.callId,
        fileUri: rec.fileUri,
        durationMs: rec.durationMs ?? null,
        metadata: rec.metadata ?? {},
        createdAt: ts,
      },
      tasks: savedTasks,
      // 整个请求此前已经处理过（录音行与全部任务行都不是新建）
      repeated: insertedCount === 0,
    };
  }

  return {
    db,
    ingestRecording,
    listPendingTasks: (limit = 100) => listPendingTasks.all(limit),
    markDelivered: (id) => markDelivered.run({ id, now: nowIso() }).changes,
    recordRelayFailure: (id, error) =>
      recordRelayFailure.run({ id, error: String(error && error.message || error) }).changes,
    claimProcessing: (taskId) => {
      const ts = nowIso();
      const inserted = claimProcessing.run(taskId, ts, ts).changes === 1;
      return { inserted, row: selectProcessing.get(taskId) };
    },
    selectProcessing: (taskId) => selectProcessing.get(taskId),
    finishProcessing: (taskId, result) =>
      finishProcessing.run({ taskId, result: JSON.stringify(result ?? null), now: nowIso() }).changes,
    requeueProcessing: (taskId, error) =>
      requeueProcessing.run({ taskId, error: String(error && error.message || error), now: nowIso() }).changes,
    failProcessing: (taskId, error) =>
      failProcessing.run({ taskId, error: String(error && error.message || error), now: nowIso() }).changes,
    deadLetter: (taskId, payload, error) =>
      insertDeadLetter.run({
        taskId,
        payload: typeof payload === 'string' ? payload : JSON.stringify(payload),
        error: String(error && error.message || error),
        now: nowIso(),
      }).changes,
  };
}

module.exports = { openDb, createRepository, taskIdFor };

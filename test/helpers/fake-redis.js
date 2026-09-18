'use strict';

/**
 * 最小 RESP2/3 兼容的 Redis 测试服务器（真实 TCP）。
 *
 * 只实现本项目用到的命令；ioredis 作为真实客户端连上来，
 * 因此握手、重连、阻塞命令超时等行为与生产一致。
 *
 * 额外能力（用于验证可恢复语义）：
 * - crash()：直接关闭 socket 模拟宕机
 * - restart(port)：重新监听；flush=true 时模拟丢内存（dedupe 键/脚本缓存消失）
 * - block XREADGROUP 支持（带超时、新 XADD 即时唤醒）
 */
const net = require('net');

// ---- RESP 编解码 ----
function encode(value) {
  if (value === null || value === undefined) return '$-1\r\n';
  if (typeof value === 'number' && Number.isInteger(value)) return `:${value}\r\n`;
  if (value instanceof Error) return `-${value.message}\r\n`;
  if (typeof value === 'string') return `$${Buffer.byteLength(value)}\r\n${value}\r\n`;
  if (Buffer.isBuffer(value)) return `$${value.length}\r\n` + value.toString('binary') + '\r\n';
  if (Array.isArray(value)) {
    return `*${value.length}\r\n` + value.map(encode).join('');
  }
  if (value instanceof Map) {
    // Hash: RESP2 下扁平成数组（ioredis xreadgroup 兼容）
    const entries = [...value.entries()].flat();
    return `*${entries.length}\r\n` + entries.map(encode).join('');
  }
  if (typeof value === 'object') {
    // 命名回复按 key=val 扁平数组编码
    const entries = [];
    for (const [k, v] of Object.entries(value)) entries.push(k, v);
    return `*${entries.length}\r\n` + entries.map(encode).join('');
  }
  return encode(String(value));
}

class FakeRedis {
  constructor() {
    this.keys = new Map();       // string -> Buffer|string
    this.streams = new Map();    // name -> [{id, fields:Map}]
    this.groups = new Map();     // stream -> Map(group -> {consumers:Map, lastDelivered, pending:Map<id,{consumer,idleSince,deliveryCount}>})
    this.scripts = new Map();    // sha -> lua
    this.server = null;
    this.port = null;
    this.waiters = [];           // 阻塞的 XREADGROUP
    this.connections = new Set();
  }

  listen(port = 0) {
    return new Promise((resolve) => {
      this.server = net.createServer((socket) => this._onConnection(socket));
      this.server.listen(port, '127.0.0.1', () => {
        this.port = this.server.address().port;
        resolve(this.port);
      });
    });
  }

  crash() {
    // 取消所有阻塞中的 XREADGROUP：既要清 timer，也要标记失效。
    // 否则旧 waiter 的超时回调仍会在重连后调用 gather()，把新消息标记为
    // pending 并推进消费组 cursor，却把结果写给已销毁的 socket（消息被吞）。
    for (const w of this.waiters) {
      w.cancelled = true;
      clearTimeout(w.timer);
      try { w.socket.destroy(); } catch {}
    }
    this.waiters = [];
    // 销毁所有活动连接，客户端必须重连 —— 与真实 Redis 宕机一致
    for (const socket of this.connections) {
      try { socket.destroy(); } catch {}
    }
    this.connections.clear();
    const server = this.server;
    this.server = null;
    if (!server) return Promise.resolve();
    return new Promise((resolve) => server.close(() => resolve()));
  }

  async restart({ port = 0, flush = false } = {}) {
    await this.crash();
    if (flush) {
      // 模拟 Redis 无持久化重启：普通键与脚本缓存丢失；
      // Stream 在生产中若开 AOF 会保留——这里也保留，以验证 outbox 才是不丢的根本
      this.keys.clear();
      this.scripts.clear();
    }
    await this.listen(port);
    return this.port;
  }

  close() {
    this.crash();
  }

  _onConnection(socket) {
    this.connections.add(socket);
    socket.on('close', () => this.connections.delete(socket));
    let buf = Buffer.alloc(0);

    // 严格按命令到达顺序回复（Redis 单连接 FIFO 语义）。
    // 每条命令占一个输出槽，槽内的 Promise resolve 后才 flush；
    // 前面的槽未就绪时，后面的回复在内存等待，绝不提前上线交错。
    // 阻塞命令（XREADGROUP BLOCK）等待期间占住队头——这与真实 Redis 一致：
    // 一个连接在阻塞命令返回前不会有别的在途命令。
    const slots = [];
    let flushing = false;
    const enqueueSlot = (replyOrPromise) => {
      const slot = Promise.resolve(replyOrPromise).catch(
        (e) => (e instanceof Error ? e : new Error(String(e)))
      );
      slots.push(slot);
      pump();
    };
    const pump = () => {
      if (flushing) return;
      flushing = true;
      void (async () => {
        while (slots.length) {
          const reply = await slots[0];
          slots.shift();
          if (!socket.destroyed) socket.write(encode(reply));
        }
        flushing = false;
      })();
    };

    socket.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      while (true) {
        const { value, rest } = parseRESP(buf);
        if (value === undefined) break;
        buf = rest;
        try {
          const reply = this._dispatch(value, socket);
          if (reply !== undefined) enqueueSlot(reply);
        } catch (e) {
          enqueueSlot(e instanceof Error ? e : new Error(String(e)));
        }
      }
    });
  }

  _streamEntries(name) {
    if (!this.streams.has(name)) this.streams.set(name, []);
    return this.streams.get(name);
  }

  _nextStreamId(name, explicit = '*') {
    const entries = this._streamEntries(name);
    if (explicit !== '*') return explicit;
    const ms = Date.now();
    let seq = 0;
    if (entries.length) {
      const [lastMs, lastSeq] = entries[entries.length - 1].id.split('-').map(Number);
      if (lastMs === ms) seq = lastSeq + 1;
    }
    return `${ms}-${seq}`;
  }

  _groupFor(streamName, groupName) {
    const groups = this.groups.get(streamName);
    return groups ? groups.get(groupName) : undefined;
  }

  _dispatch(cmd, socket) {
    const args = cmd.map((a) => (Buffer.isBuffer(a) ? a.toString() : a));
    const c = args[0].toUpperCase();

    switch (c) {
      case 'PING': return 'PONG';
      case 'CLIENT': return 'OK';   // CLIENT SETINFO etc.
      case 'COMMAND': return [];
      case 'INFO': return '# Server\r\nredis_version:7.0.0\r\n# Replication\r\nrole:master\r\n';
      case 'HELLO':
        // ioredis v6 探协议：HELLO [2|3]；始终按 RESP2 返回键值数组
        return ['server', 'redis', 'version', '7.0.0', 'proto', 2, 'id', 1, 'mode', 'standalone', 'role', 'master'];
      case 'SELECT': return 'OK';
      case 'AUTH': return 'OK';
      case 'CONFIG': return 'OK';
      case 'SET': return this._set(args.slice(1));
      case 'GET': return this.keys.has(args[1]) ? this.keys.get(args[1]) : null;
      case 'SCRIPT': return this._script(args.slice(1));
      case 'EVAL': case 'EVALSHA': return this._eval(args);
      case 'XADD': return this._xadd(args.slice(1));
      case 'XGROUP': return this._xgroup(args.slice(1));
      case 'XREADGROUP': return this._xreadgroup(args.slice(1), socket);
      case 'XACK': return this._xack(args.slice(1));
      case 'XAUTOCLAIM': return this._xautoclaim(args.slice(1));
      case 'XPENDING': return [0, null, null, null];
      case 'QUIT': return 'OK';
      default: throw new Error(`ERR unknown command '${c}'`);
    }
  }

  _set(args) {
    // SET key value [PX ms] [NX]
    const [key, value] = args;
    let px = null;
    let nx = false;
    for (let i = 2; i < args.length; i++) {
      const opt = args[i].toUpperCase();
      if (opt === 'NX') nx = true;
      if (opt === 'PX') px = Number(args[++i]);
    }
    if (nx && this.keys.has(key)) return null;
    this.keys.set(key, value);
    if (px !== null && px <= 2_147_483_647) {
      setTimeout(() => this.keys.delete(key), px).unref?.();
    }
    return 'OK';
  }

  _script(args) {
    if (args[0].toUpperCase() === 'LOAD') {
      const body = args[1];
      const sha = sha1(body);
      this.scripts.set(sha, body);
      return sha;
    }
    if (args[0].toUpperCase() === 'EXISTS') return args.slice(1).map((s) => (this.scripts.has(s) ? 1 : 0));
    if (args[0].toUpperCase() === 'FLUSH') { this.scripts.clear(); return 'OK'; }
    return 'OK';
  }

  _eval(args) {
    // EVAL/EVALSHA script numkeys k0 k1 a0 a1 a2
    let body;
    if (args[0].toUpperCase() === 'EVAL') body = args[1];
    else {
      body = this.scripts.get(args[1]);
      if (!body) throw new Error('NOSCRIPT No matching script. Please use EVAL.');
    }
    const numkeys = Number(args[2]);
    const keys = args.slice(3, 3 + numkeys);
    const argv = args.slice(3 + numkeys);

    // 精确匹配 relay 的入队脚本（用脚本全文比对，绝不能用宽泛正则，
    // 否则握手期的 HELLO/其它脚本会被误判，污染 dedupe 键空间）。
    if (body === require('../../src/relay').ENQUEUE_LUA) {
      const [dedupeKey, stream] = keys;
      const [taskId, payload, ttl] = argv;
      if (this.keys.has(dedupeKey)) return null;
      this.keys.set(dedupeKey, taskId);
      const ttlNum = Number(ttl);
      if (ttlNum <= 2_147_483_647) setTimeout(() => this.keys.delete(dedupeKey), ttlNum).unref?.();
      const id = this._xadd([stream, '*', 'taskId', taskId, 'payload', payload]);
      this._wakeWaiters(stream);
      return id;
    }
    throw new Error('ERR unsupported eval script in fake redis');
  }

  _xadd(args) {
    const [name, idSpec, ...fieldArgs] = args;
    // 支持 MAXLEN ~ n 前缀（本项目未用，但保持健壮）
    let fields = fieldArgs;
    let spec = idSpec;
    if (fieldArgs[0] && fieldArgs[0].toUpperCase() === 'MAXLEN') {
      fields = fieldArgs.slice(3);
    }
    const id = this._nextStreamId(name, spec);
    const fm = new Map();
    for (let i = 0; i < fields.length; i += 2) fm.set(fields[i], fields[i + 1]);
    this._streamEntries(name).push({ id, fields: fm });
    this._wakeWaiters(name);
    return id;
  }

  _xgroup(args) {
    const sub = args[0].toUpperCase();
    if (sub === 'CREATE') {
      const [, stream, group, start] = args;
      let mkstream = args.some((a) => a.toUpperCase() === 'MKSTREAM');
      if (!this.streams.has(stream)) {
        if (!mkstream) throw new Error('NOGROUP The XGROUP subgroup or key name does not exist');
        this.streams.set(stream, []);
      }
      if (!this.groups.has(stream)) this.groups.set(stream, new Map());
      const groups = this.groups.get(stream);
      if (groups.has(group)) throw new Error('BUSYGROUP Consumer Group name already exists');
      const entries = this._streamEntries(stream);
      const cursor = start === '$'
        ? (entries.length ? entries[entries.length - 1].id : '0-0')
        : (start === '0' ? '0-0' : start);
      groups.set(group, { start, consumers: new Map(), pending: new Map(), cursor });
      return 'OK';
    }
    if (sub === 'CREATECONSUMER') return 1;
    return 'OK';
  }

  _xreadgroup(args, socket) {
    // XREADGROUP GROUP g c [COUNT n] [BLOCK ms] STREAMS s1 ... id1 ...
    let i = 0;
    // args[0]=GROUP
    const group = args[1];
    const consumer = args[2];
    i = 3;
    let count = Infinity;
    let block = null;
    while (i < args.length && args[i].toUpperCase() !== 'STREAMS') {
      if (args[i].toUpperCase() === 'COUNT') count = Number(args[++i]);
      if (args[i].toUpperCase() === 'BLOCK') block = Number(args[++i]);
      i++;
    }
    i++; // STREAMS
    const rest = args.slice(i);
    const half = rest.length / 2;
    const streams = rest.slice(0, half);
    const ids = rest.slice(half);

    const gather = () => {
      const results = [];
      for (let s = 0; s < streams.length; s++) {
        const name = streams[s];
        const wantNew = ids[s] === '>';
        const g = this._groupFor(name, group);
        if (!g) throw new Error('NOGROUP No such consumer group');
        if (!g.consumers.has(consumer)) g.consumers.set(consumer, new Set());
        const entries = this._streamEntries(name);
        const picked = [];
        if (wantNew) {
          for (const e of entries) {
            if (picked.length >= count) break;
            if (cmpId(e.id, g.cursor) > 0 && !g.pending.has(e.id)) {
              picked.push(e);
              g.pending.set(e.id, {
                consumer,
                idleSince: Date.now(),
                deliveryCount: (g.pending.get(e.id)?.deliveryCount || 0) + 1,
              });
            }
          }
          if (picked.length) {
            g.cursor = picked[picked.length - 1].id;
          }
        }
        if (picked.length) {
          results.push([name, picked.map((e) => [e.id, [...e.fields.entries()].flat()])]);
        }
      }
      return results.length ? results : null;
    };

    const immediate = gather();
    if (immediate) return immediate;
    if (block === 0 || block > 0) {
      return new Promise((resolve) => {
        const waiter = {
          socket,
          streams: new Set(streams),
          cancelled: false,
          timer: setTimeout(() => {
            this.waiters = this.waiters.filter((w) => w !== waiter);
            // 连接已失效：绝不能再 gather（否则会吞掉新消息）
            if (waiter.cancelled || socket.destroyed) return resolve(null);
            resolve(null);
          }, block === 0 ? 5_000 : block).unref(),
          fire: () => {
            if (waiter.cancelled) return;
            clearTimeout(waiter.timer);
            this.waiters = this.waiters.filter((w) => w !== waiter);
            if (socket.destroyed) return resolve(null);
            resolve(gather());
          },
        };
        this.waiters.push(waiter);
      });
    }
    return null;
  }

  _wakeWaiters(stream) {
    for (const w of this.waiters) {
      if (w.streams.has(stream)) w.fire();
    }
  }

  _xack(args) {
    const [stream, group, ...ids] = args;
    const g = this._groupFor(stream, group);
    if (!g) return 0;
    let n = 0;
    for (const id of ids) {
      if (g.pending.delete(id)) n++;
    }
    return n;
  }

  _xautoclaim(args) {
    // XAUTOCLAIM key group consumer min-idle-time start [COUNT n]
    const [stream, group, consumer, minIdle, start] = args;
    let count = 100;
    for (let i = 5; i < args.length; i++) {
      if (args[i].toUpperCase() === 'COUNT') count = Number(args[++i]);
    }
    const g = this._groupFor(stream, group);
    if (!g) throw new Error('NOGROUP No such consumer group');
    if (!g.consumers.has(consumer)) g.consumers.set(consumer, new Set());
    const now = Date.now();
    const claimed = [];
    for (const [id, pel] of g.pending) {
      if (claimed.length >= count) break;
      if (cmpId(id, start) <= 0) continue;
      if (now - pel.idleSince >= Number(minIdle)) {
        pel.consumer = consumer;
        pel.idleSince = now;
        const entry = this._streamEntries(stream).find((e) => e.id === id);
        if (entry) claimed.push([id, [...entry.fields.entries()].flat()]);
      }
    }
    // RESP2 形态：[nextCursor, entries]（worker 兼容数组/对象两种）
    return ['0-0', claimed];
  }
}

function cmpId(a, b) {
  const [a0, a1] = a.split('-').map(Number);
  const [b0, b1] = b.split('-').map(Number);
  return a0 - b0 || a1 - b1;
}

// ---- RESP 解析（行数组/管道）----
function parseRESP(buf) {
  let offset = 0;
  function readLine() {
    const idx = buf.indexOf('\r\n', offset);
    if (idx === -1) return null;
    const line = buf.slice(offset, idx).toString();
    offset = idx + 2;
    return line;
  }
  function parseOne() {
    const line = readLine();
    if (line === null) return undefined;
    const type = line[0];
    const body = line.slice(1);
    if (type === '*') {
      const n = parseInt(body, 10);
      if (n < 0) return null;
      const arr = [];
      for (let i = 0; i < n; i++) {
        const v = parseOne();
        if (v === undefined) return undefined;
        arr.push(v);
      }
      return arr;
    }
    if (type === '$') {
      const len = parseInt(body, 10);
      if (len < 0) return null;
      if (buf.length < offset + len + 2) return undefined;
      const bulk = buf.slice(offset, offset + len);
      offset += len + 2;
      return bulk;
    }
    if (type === '+' || type === '-' || type === ':') return body;
    return body;
  }
  const value = parseOne();
  if (value === undefined) return { value: undefined, rest: buf };
  return { value, rest: buf.slice(offset) };
}

// 简易 SHA-1（足够 SCRIPT LOAD 的键用途）
function sha1(input) {
  const crypto = require('crypto');
  return crypto.createHash('sha1').update(input).digest('hex');
}

module.exports = { FakeRedis };

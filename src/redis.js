'use strict';

const Redis = require('ioredis');

/**
 * 构造 ioredis 客户端。
 * - enableOfflineQueue=false：命令必须立刻能发出去，relay 遇到断连会快速失败，
 *   由 outbox 重试兜底，而不是让命令在客户端内存里排队（进程崩溃就丢了）。
 * - maxRetriesPerRequest=null：避免单次请求在重连期间被 ioredis 放弃而吞掉错误。
 *   relay 自行控制重试节奏。
 * - 不设置 reconnectOnError：真实 socket 故障由 ioredis 自动重连；若配置成
 *   "对任何错误都重连"，会把 BUSYGROUP 这类普通命令错误也当成连接故障反复断连，
 *   反而造成连接状态异常。
 */
function createRedis({ host = '127.0.0.1', port = 6379, db = 0, lazyConnect = false } = {}) {
  return new Redis({
    host,
    port,
    db,
    lazyConnect,
    enableOfflineQueue: false,
    maxRetriesPerRequest: null,
    connectTimeout: 1000,
    retryStrategy: (times) => Math.min(100 * times, 2000),
  });
}

module.exports = { createRedis };

/**
 * Central configuration for the distributed job scheduler.
 * All values can be overridden via environment variables.
 */
module.exports = {
  // Worker identity
  workerId: process.env.WORKER_ID || `worker-${process.pid}-${Date.now()}`,

  // PostgreSQL
  postgres: {
    host: process.env.PG_HOST || 'localhost',
    port: parseInt(process.env.PG_PORT || '5432'),
    database: process.env.PG_DATABASE || 'job_scheduler',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASSWORD || 'postgres',
    max: parseInt(process.env.PG_POOL_MAX || '20'),
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  },

  // Redis
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379'),
    password: process.env.REDIS_PASSWORD || undefined,
    maxRetriesPerRequest: 3,
    retryStrategy: (times) => Math.min(times * 100, 3000),
  },

  // Leader Election
  leader: {
    lockKey: 'scheduler:leader:lock',
    lockTTLMs: 15000,         // Leader lock expires after 15s
    renewIntervalMs: 5000,    // Renew lock every 5s
    electionIntervalMs: 3000, // Try to become leader every 3s
  },

  // Job Execution
  execution: {
    lockPrefix: 'scheduler:job:lock:',
    lockTTLMs: 300000,                // Job lock TTL: 5 minutes (extended during execution)
    lockExtendIntervalMs: 60000,      // Extend lock every 60s during execution
    maxRetries: 5,
    baseRetryDelayMs: 1000,           // Exponential backoff base: 1s
    maxRetryDelayMs: 300000,          // Max backoff: 5 minutes
    batchSize: 10,                    // Jobs to claim per poll cycle
    pollIntervalMs: 1000,             // Worker poll interval
  },

  // Redis Streams
  streams: {
    jobStream: 'scheduler:jobs:pending',
    dlqStream: 'scheduler:jobs:dlq',
    consumerGroup: 'job-workers',
    blockTimeMs: 5000,                // Block read timeout
    maxPendingAgeMs: 600000,          // Reclaim jobs pending > 10 minutes
  },

  // Scheduler (Leader only)
  scheduler: {
    cronPollIntervalMs: 10000,        // Check for due cron jobs every 10s
    delayedPollIntervalMs: 5000,      // Check for due delayed jobs every 5s
  },
};

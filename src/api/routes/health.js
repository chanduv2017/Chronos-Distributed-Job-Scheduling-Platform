/**
 * Health check and system status routes.
 * 
 * GET /health          - Basic liveness check
 * GET /health/ready    - Readiness check (dependencies healthy)
 * GET /health/status   - Detailed system status
 */
const { Router } = require('express');
const { getPool } = require('../../db/pool');
const { getClient } = require('../../lib/redis');
const { registry } = require('../../services/handler-registry');
const config = require('../../config');

const router = Router();

/**
 * GET /health
 * Basic liveness — returns 200 if the process is running.
 */
router.get('/', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

/**
 * GET /health/ready
 * Readiness — checks PostgreSQL and Redis connectivity.
 */
router.get('/ready', async (req, res) => {
  const checks = { postgres: false, redis: false };

  try {
    const pool = getPool();
    await pool.query('SELECT 1');
    checks.postgres = true;
  } catch (e) {
    checks.postgres = false;
  }

  try {
    const redis = getClient();
    await redis.ping();
    checks.redis = true;
  } catch (e) {
    checks.redis = false;
  }

  const allHealthy = checks.postgres && checks.redis;
  res.status(allHealthy ? 200 : 503).json({
    status: allHealthy ? 'ready' : 'degraded',
    checks,
  });
});

/**
 * GET /health/status
 * Detailed system status including job counts and worker info.
 */
router.get('/status', async (req, res) => {
  try {
    const pool = getPool();
    const redis = getClient();

    // Job counts by status
    const jobCounts = await pool.query(
      `SELECT status, COUNT(*) as count FROM jobs GROUP BY status`
    );

    // DLQ count
    const dlqCount = await pool.query('SELECT COUNT(*) FROM dead_letter_queue');

    // Recent executions
    const recentExecs = await pool.query(
      `SELECT status, COUNT(*) as count 
       FROM job_executions 
       WHERE started_at > NOW() - INTERVAL '1 hour'
       GROUP BY status`
    );

    // Redis stream info
    let streamLength = 0;
    try {
      streamLength = await redis.xlen(config.streams.jobStream);
    } catch (e) { /* stream may not exist yet */ }

    let dlqStreamLength = 0;
    try {
      dlqStreamLength = await redis.xlen(config.streams.dlqStream);
    } catch (e) { /* stream may not exist yet */ }

    res.json({
      status: 'ok',
      workerId: config.workerId,
      uptime: process.uptime(),
      handlers: registry.list(),
      jobs: Object.fromEntries(jobCounts.rows.map((r) => [r.status, parseInt(r.count)])),
      dlq: { postgres: parseInt(dlqCount.rows[0].count), redis: dlqStreamLength },
      stream: { pending: streamLength },
      recentExecutions: Object.fromEntries(recentExecs.rows.map((r) => [r.status, parseInt(r.count)])),
    });
  } catch (error) {
    res.status(500).json({ status: 'error', error: error.message });
  }
});

module.exports = router;

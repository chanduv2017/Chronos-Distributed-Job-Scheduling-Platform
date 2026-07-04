/**
 * Job Service: CRUD operations for job management.
 * This is the public API for creating, querying, and managing jobs.
 */
const { v4: uuidv4 } = require('uuid');
const { getPool } = require('../db/pool');
const { isValid, getNextFireTime } = require('../lib/cron-parser');
const logger = require('../lib/logger');

class JobService {
  /**
   * Create a new cron job (recurring).
   * 
   * @param {object} params
   * @param {string} params.name - Human-readable job name
   * @param {string} params.cronExpression - 5-field cron expression
   * @param {string} params.handlerName - Registered handler function name
   * @param {object} params.payload - Data passed to handler
   * @param {string} params.timezone - IANA timezone (default: UTC)
   * @param {number} params.maxRetries - Max retry attempts (default: 5)
   * @param {string} params.idempotencyKey - Prevent duplicate creation
   * @returns {object} Created job
   */
  async createCronJob({ name, cronExpression, handlerName, payload = {}, timezone = 'UTC', maxRetries = 5, idempotencyKey }) {
    if (!isValid(cronExpression)) {
      throw new Error(`Invalid cron expression: ${cronExpression}`);
    }

    const nextRunAt = getNextFireTime(cronExpression);
    if (!nextRunAt) {
      throw new Error(`Cron expression will never fire: ${cronExpression}`);
    }

    const pool = getPool();
    const id = uuidv4();

    const result = await pool.query(
      `INSERT INTO jobs (id, name, type, cron_expression, handler_name, payload, timezone, max_retries, next_run_at, idempotency_key)
       VALUES ($1, $2, 'cron', $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (idempotency_key) DO NOTHING
       RETURNING *`,
      [id, name, cronExpression, handlerName, JSON.stringify(payload), timezone, maxRetries, nextRunAt, idempotencyKey || id]
    );

    if (result.rows.length === 0) {
      logger.warn({ idempotencyKey }, 'Job already exists (idempotency conflict)');
      const existing = await pool.query('SELECT * FROM jobs WHERE idempotency_key = $1', [idempotencyKey]);
      return existing.rows[0];
    }

    logger.info({ jobId: id, name, cronExpression, nextRunAt }, 'Cron job created');
    return result.rows[0];
  }

  /**
   * Create a delayed job (one-time, fires at a specific time).
   * 
   * @param {object} params
   * @param {string} params.name - Human-readable job name
   * @param {Date|string} params.scheduledAt - When to fire
   * @param {string} params.handlerName - Registered handler function name
   * @param {object} params.payload - Data passed to handler
   * @param {number} params.maxRetries - Max retry attempts
   * @param {string} params.idempotencyKey - Prevent duplicate creation
   * @returns {object} Created job
   */
  async createDelayedJob({ name, scheduledAt, handlerName, payload = {}, maxRetries = 5, idempotencyKey }) {
    const fireTime = new Date(scheduledAt);

    if (isNaN(fireTime.getTime())) {
      throw new Error(`Invalid scheduledAt: ${scheduledAt}`);
    }

    if (fireTime <= new Date()) {
      throw new Error('scheduledAt must be in the future');
    }

    const pool = getPool();
    const id = uuidv4();

    const result = await pool.query(
      `INSERT INTO jobs (id, name, type, scheduled_at, handler_name, payload, max_retries, next_run_at, idempotency_key)
       VALUES ($1, $2, 'delayed', $3, $4, $5, $6, $3, $7)
       ON CONFLICT (idempotency_key) DO NOTHING
       RETURNING *`,
      [id, name, fireTime, handlerName, JSON.stringify(payload), maxRetries, idempotencyKey || id]
    );

    if (result.rows.length === 0) {
      logger.warn({ idempotencyKey }, 'Delayed job already exists (idempotency conflict)');
      const existing = await pool.query('SELECT * FROM jobs WHERE idempotency_key = $1', [idempotencyKey]);
      return existing.rows[0];
    }

    logger.info({ jobId: id, name, scheduledAt: fireTime }, 'Delayed job created');
    return result.rows[0];
  }

  /**
   * Create an immediate job (fires ASAP).
   * 
   * @param {object} params
   * @returns {object} Created job
   */
  async createImmediateJob({ name, handlerName, payload = {}, maxRetries = 5, idempotencyKey }) {
    const pool = getPool();
    const id = uuidv4();
    const now = new Date();

    const result = await pool.query(
      `INSERT INTO jobs (id, name, type, handler_name, payload, max_retries, next_run_at, idempotency_key)
       VALUES ($1, $2, 'immediate', $3, $4, $5, $6, $7)
       ON CONFLICT (idempotency_key) DO NOTHING
       RETURNING *`,
      [id, name, handlerName, JSON.stringify(payload), maxRetries, now, idempotencyKey || id]
    );

    if (result.rows.length === 0) {
      const existing = await pool.query('SELECT * FROM jobs WHERE idempotency_key = $1', [idempotencyKey]);
      return existing.rows[0];
    }

    logger.info({ jobId: id, name }, 'Immediate job created');
    return result.rows[0];
  }

  /**
   * Get jobs that are due to run (next_run_at <= now, status = active, not locked).
   * Uses SELECT FOR UPDATE SKIP LOCKED for safe concurrent access.
   * 
   * @param {number} limit - Max jobs to fetch
   * @returns {Array} Due jobs
   */
  async getDueJobs(limit = 10) {
    const pool = getPool();

    const result = await pool.query(
      `SELECT * FROM jobs
       WHERE status = 'active'
         AND next_run_at <= NOW()
         AND (locked_by IS NULL OR locked_at < NOW() - INTERVAL '5 minutes')
       ORDER BY next_run_at ASC
       LIMIT $1
       FOR UPDATE SKIP LOCKED`,
      [limit]
    );

    return result.rows;
  }

  /**
   * Lock a job for execution (claim ownership).
   * @param {string} jobId - Job ID
   * @param {string} workerId - Worker claiming the job
   * @returns {boolean} true if locked successfully
   */
  async lockJob(jobId, workerId) {
    const pool = getPool();

    const result = await pool.query(
      `UPDATE jobs
       SET locked_by = $2, locked_at = NOW()
       WHERE id = $1
         AND status = 'active'
         AND (locked_by IS NULL OR locked_at < NOW() - INTERVAL '5 minutes')
       RETURNING id`,
      [jobId, workerId]
    );

    return result.rowCount > 0;
  }

  /**
   * Mark job execution as complete and compute next run time.
   * @param {string} jobId - Job ID
   */
  async completeExecution(jobId) {
    const pool = getPool();

    const jobResult = await pool.query('SELECT * FROM jobs WHERE id = $1', [jobId]);
    if (jobResult.rows.length === 0) return;

    const job = jobResult.rows[0];

    if (job.type === 'cron') {
      // Recurring: compute next fire time
      const nextRunAt = getNextFireTime(job.cron_expression);

      await pool.query(
        `UPDATE jobs
         SET locked_by = NULL, locked_at = NULL, last_run_at = NOW(),
             next_run_at = $2, retry_count = 0, last_error = NULL
         WHERE id = $1`,
        [jobId, nextRunAt]
      );
    } else {
      // One-time (delayed/immediate): mark as completed
      await pool.query(
        `UPDATE jobs
         SET locked_by = NULL, locked_at = NULL, last_run_at = NOW(),
             status = 'completed', next_run_at = NULL
         WHERE id = $1`,
        [jobId]
      );
    }
  }

  /**
   * Mark job as failed and schedule retry or send to DLQ.
   * @param {string} jobId - Job ID
   * @param {string} error - Error message
   * @param {Date} retryAt - When to retry (null = send to DLQ)
   */
  async failExecution(jobId, error, retryAt) {
    const pool = getPool();

    if (retryAt) {
      // Schedule retry
      await pool.query(
        `UPDATE jobs
         SET locked_by = NULL, locked_at = NULL,
             retry_count = retry_count + 1, last_error = $2,
             next_run_at = $3
         WHERE id = $1`,
        [jobId, error, retryAt]
      );
    } else {
      // Max retries exceeded — mark as permanently failed
      await pool.query(
        `UPDATE jobs
         SET locked_by = NULL, locked_at = NULL,
             retry_count = retry_count + 1, last_error = $2,
             status = 'failed_permanent', next_run_at = NULL
         WHERE id = $1`,
        [jobId, error]
      );
    }
  }

  /**
   * Unlock a job without changing status (e.g., on graceful shutdown).
   */
  async unlockJob(jobId) {
    const pool = getPool();
    await pool.query(
      'UPDATE jobs SET locked_by = NULL, locked_at = NULL WHERE id = $1',
      [jobId]
    );
  }

  /**
   * Pause a job.
   */
  async pauseJob(jobId) {
    const pool = getPool();
    await pool.query("UPDATE jobs SET status = 'paused' WHERE id = $1", [jobId]);
  }

  /**
   * Resume a paused job.
   */
  async resumeJob(jobId) {
    const pool = getPool();
    const job = (await pool.query('SELECT * FROM jobs WHERE id = $1', [jobId])).rows[0];

    if (!job) throw new Error(`Job not found: ${jobId}`);

    let nextRunAt = null;
    if (job.type === 'cron') {
      nextRunAt = getNextFireTime(job.cron_expression);
    } else if (job.type === 'delayed') {
      nextRunAt = job.scheduled_at > new Date() ? job.scheduled_at : null;
    }

    await pool.query(
      "UPDATE jobs SET status = 'active', next_run_at = $2 WHERE id = $1",
      [jobId, nextRunAt]
    );
  }

  /**
   * Trigger a job to run immediately (set next_run_at = now).
   */
  async triggerNow(jobId) {
    const pool = getPool();
    await pool.query(
      `UPDATE jobs SET next_run_at = NOW(), locked_by = NULL, locked_at = NULL
       WHERE id = $1 AND status IN ('active', 'paused')`,
      [jobId]
    );
    // Also ensure it's active
    await pool.query("UPDATE jobs SET status = 'active' WHERE id = $1", [jobId]);
  }

  /**
   * Update job properties.
   */
  async updateJob(jobId, updates) {
    const pool = getPool();
    const setClauses = [];
    const values = [jobId];
    let paramIndex = 2;

    if (updates.name) {
      setClauses.push(`name = $${paramIndex++}`);
      values.push(updates.name);
    }
    if (updates.cronExpression) {
      if (!isValid(updates.cronExpression)) {
        throw new Error(`Invalid cron expression: ${updates.cronExpression}`);
      }
      setClauses.push(`cron_expression = $${paramIndex++}`);
      values.push(updates.cronExpression);

      // Recompute next run time
      const nextRunAt = getNextFireTime(updates.cronExpression);
      setClauses.push(`next_run_at = $${paramIndex++}`);
      values.push(nextRunAt);
    }
    if (updates.payload) {
      setClauses.push(`payload = $${paramIndex++}`);
      values.push(JSON.stringify(updates.payload));
    }
    if (updates.maxRetries !== undefined) {
      setClauses.push(`max_retries = $${paramIndex++}`);
      values.push(updates.maxRetries);
    }
    if (updates.timezone) {
      setClauses.push(`timezone = $${paramIndex++}`);
      values.push(updates.timezone);
    }

    if (setClauses.length === 0) return this.getJob(jobId);

    const result = await pool.query(
      `UPDATE jobs SET ${setClauses.join(', ')} WHERE id = $1 RETURNING *`,
      values
    );

    return result.rows[0];
  }

  /**
   * Delete a job.
   */
  async deleteJob(jobId) {
    const pool = getPool();
    await pool.query('DELETE FROM jobs WHERE id = $1', [jobId]);
  }

  /**
   * Get job by ID.
   */
  async getJob(jobId) {
    const pool = getPool();
    const result = await pool.query('SELECT * FROM jobs WHERE id = $1', [jobId]);
    return result.rows[0] || null;
  }

  /**
   * List jobs with optional filters.
   */
  async listJobs({ status, type, limit = 50, offset = 0 } = {}) {
    const pool = getPool();
    let query = 'SELECT * FROM jobs WHERE 1=1';
    const params = [];

    if (status) {
      params.push(status);
      query += ` AND status = $${params.length}`;
    }
    if (type) {
      params.push(type);
      query += ` AND type = $${params.length}`;
    }

    params.push(limit, offset);
    query += ` ORDER BY created_at DESC LIMIT $${params.length - 1} OFFSET $${params.length}`;

    const result = await pool.query(query, params);
    return result.rows;
  }
}

module.exports = { JobService };

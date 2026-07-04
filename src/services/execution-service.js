/**
 * Execution Service: Records job execution history.
 * Provides audit trail and idempotency checks.
 */
const { v4: uuidv4 } = require('uuid');
const { getPool } = require('../db/pool');
const logger = require('../lib/logger');

class ExecutionService {
  /**
   * Record the start of a job execution.
   * Uses execution_key for idempotency (prevents duplicate execution records).
   * 
   * @param {string} jobId - Job being executed
   * @param {string} workerId - Worker executing it
   * @param {number} attempt - Attempt number
   * @returns {object|null} Execution record, or null if duplicate
   */
  async recordStart(jobId, workerId, attempt) {
    const pool = getPool();
    const id = uuidv4();
    const executionKey = `${jobId}:${attempt}:${Date.now()}`;

    try {
      const result = await pool.query(
        `INSERT INTO job_executions (id, job_id, worker_id, status, attempt, execution_key)
         VALUES ($1, $2, $3, 'running', $4, $5)
         ON CONFLICT (execution_key) DO NOTHING
         RETURNING *`,
        [id, jobId, workerId, attempt, executionKey]
      );

      if (result.rows.length === 0) {
        logger.warn({ jobId, attempt }, 'Duplicate execution detected (idempotency)');
        return null;
      }

      return result.rows[0];
    } catch (error) {
      logger.error({ jobId, error }, 'Failed to record execution start');
      throw error;
    }
  }

  /**
   * Record successful completion.
   */
  async recordSuccess(executionId) {
    const pool = getPool();

    await pool.query(
      `UPDATE job_executions
       SET status = 'success', completed_at = NOW(),
           duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000
       WHERE id = $1`,
      [executionId]
    );
  }

  /**
   * Record failure.
   */
  async recordFailure(executionId, errorMessage, errorStack) {
    const pool = getPool();

    await pool.query(
      `UPDATE job_executions
       SET status = 'failed', completed_at = NOW(),
           duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000,
           error_message = $2, error_stack = $3
       WHERE id = $1`,
      [executionId, errorMessage, errorStack]
    );
  }

  /**
   * Record that a job was sent to DLQ.
   */
  async recordDLQ(executionId) {
    const pool = getPool();

    await pool.query(
      `UPDATE job_executions
       SET status = 'dlq', completed_at = NOW(),
           duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000
       WHERE id = $1`,
      [executionId]
    );
  }

  /**
   * Write to the dead_letter_queue table (permanent storage).
   */
  async writeToDLQTable(job, errorMessage, errorStack) {
    const pool = getPool();

    await pool.query(
      `INSERT INTO dead_letter_queue (job_id, handler_name, payload, error_message, error_stack, retry_count, max_retries)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [
        job.id || job.jobId,
        job.handler_name || job.handlerName,
        JSON.stringify(job.payload),
        errorMessage,
        errorStack,
        job.retry_count || parseInt(job.attempt || '0'),
        job.max_retries || parseInt(job.maxRetries || '5'),
      ]
    );
  }

  /**
   * Get execution history for a job.
   */
  async getHistory(jobId, limit = 20) {
    const pool = getPool();
    const result = await pool.query(
      'SELECT * FROM job_executions WHERE job_id = $1 ORDER BY started_at DESC LIMIT $2',
      [jobId, limit]
    );
    return result.rows;
  }
}

module.exports = { ExecutionService };

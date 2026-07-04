/**
 * Dead Letter Queue routes.
 * 
 * GET    /api/dlq           - List DLQ entries
 * GET    /api/dlq/:id       - Get DLQ entry details
 * POST   /api/dlq/:id/retry - Requeue a DLQ entry for retry
 * DELETE /api/dlq/:id       - Delete a DLQ entry (acknowledge/discard)
 */
const { Router } = require('express');
const { param, query } = require('express-validator');
const { validate } = require('../middleware/validate');
const { getPool } = require('../../db/pool');
const { JobService } = require('../../services/job-service');
const createError = require('http-errors');
const logger = require('../../lib/logger');

const router = Router();
const jobService = new JobService();

/**
 * GET /api/dlq
 * List dead letter queue entries.
 */
router.get('/',
  query('limit').optional().isInt({ min: 1, max: 100 }),
  query('offset').optional().isInt({ min: 0 }),
  validate,
  async (req, res, next) => {
    try {
      const pool = getPool();
      const limit = parseInt(req.query.limit || '50');
      const offset = parseInt(req.query.offset || '0');

      const result = await pool.query(
        `SELECT dlq.*, j.name as job_name, j.type as job_type
         FROM dead_letter_queue dlq
         LEFT JOIN jobs j ON j.id = dlq.job_id
         ORDER BY dlq.failed_at DESC
         LIMIT $1 OFFSET $2`,
        [limit, offset]
      );

      const countResult = await pool.query('SELECT COUNT(*) FROM dead_letter_queue');

      res.json({
        data: result.rows,
        meta: { count: result.rows.length, total: parseInt(countResult.rows[0].count) },
      });
    } catch (error) {
      next(error);
    }
  }
);

/**
 * GET /api/dlq/:id
 * Get a single DLQ entry.
 */
router.get('/:id',
  param('id').isUUID(),
  validate,
  async (req, res, next) => {
    try {
      const pool = getPool();
      const result = await pool.query(
        `SELECT dlq.*, j.name as job_name, j.type as job_type, j.cron_expression
         FROM dead_letter_queue dlq
         LEFT JOIN jobs j ON j.id = dlq.job_id
         WHERE dlq.id = $1`,
        [req.params.id]
      );

      if (result.rows.length === 0) throw createError(404, 'DLQ entry not found');
      res.json({ data: result.rows[0] });
    } catch (error) {
      next(error);
    }
  }
);

/**
 * POST /api/dlq/:id/retry
 * Requeue a failed job from the DLQ for another attempt.
 * Resets retry count and sets status back to active.
 */
router.post('/:id/retry',
  param('id').isUUID(),
  validate,
  async (req, res, next) => {
    try {
      const pool = getPool();

      // Get the DLQ entry
      const dlqResult = await pool.query(
        'SELECT * FROM dead_letter_queue WHERE id = $1',
        [req.params.id]
      );

      if (dlqResult.rows.length === 0) throw createError(404, 'DLQ entry not found');

      const dlqEntry = dlqResult.rows[0];

      // Reset the job for retry
      await pool.query(
        `UPDATE jobs
         SET status = 'active', retry_count = 0, last_error = NULL,
             next_run_at = NOW(), locked_by = NULL, locked_at = NULL
         WHERE id = $1`,
        [dlqEntry.job_id]
      );

      // Mark DLQ entry as requeued
      await pool.query(
        'UPDATE dead_letter_queue SET requeued_at = NOW() WHERE id = $1',
        [req.params.id]
      );

      logger.info({ dlqId: req.params.id, jobId: dlqEntry.job_id }, 'DLQ entry requeued for retry');

      res.json({ data: { id: req.params.id, jobId: dlqEntry.job_id, status: 'requeued' } });
    } catch (error) {
      next(error);
    }
  }
);

/**
 * DELETE /api/dlq/:id
 * Remove a DLQ entry (acknowledge the failure, discard it).
 */
router.delete('/:id',
  param('id').isUUID(),
  validate,
  async (req, res, next) => {
    try {
      const pool = getPool();
      const result = await pool.query(
        'DELETE FROM dead_letter_queue WHERE id = $1 RETURNING id',
        [req.params.id]
      );

      if (result.rowCount === 0) throw createError(404, 'DLQ entry not found');
      res.status(204).send();
    } catch (error) {
      next(error);
    }
  }
);

module.exports = router;

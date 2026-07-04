/**
 * Job management routes.
 * 
 * POST   /api/jobs/cron        - Create a recurring cron job
 * POST   /api/jobs/delayed     - Create a one-time delayed job
 * POST   /api/jobs/immediate   - Create an immediate job
 * GET    /api/jobs             - List jobs (with filters)
 * GET    /api/jobs/:id         - Get job by ID
 * GET    /api/jobs/:id/history - Get execution history
 * PATCH  /api/jobs/:id         - Update a job
 * POST   /api/jobs/:id/pause   - Pause a job
 * POST   /api/jobs/:id/resume  - Resume a paused job
 * POST   /api/jobs/:id/trigger - Manually trigger a job now
 * DELETE /api/jobs/:id         - Delete a job
 */
const { Router } = require('express');
const { body, param, query } = require('express-validator');
const { validate } = require('../middleware/validate');
const { JobService } = require('../../services/job-service');
const { ExecutionService } = require('../../services/execution-service');
const { registry } = require('../../services/handler-registry');
const createError = require('http-errors');

const router = Router();
const jobService = new JobService();
const executionService = new ExecutionService();

// --- Validation schemas ---

const cronJobValidation = [
  body('name').isString().trim().notEmpty().withMessage('name is required'),
  body('cronExpression').isString().trim().notEmpty().withMessage('cronExpression is required'),
  body('handlerName').isString().trim().notEmpty().withMessage('handlerName is required')
    .custom((value) => {
      if (!registry.has(value)) throw new Error(`Unknown handler: ${value}. Available: ${registry.list().map(h => h.name).join(', ')}`);
      return true;
    }),
  body('payload').optional().isObject().withMessage('payload must be an object'),
  body('timezone').optional().isString(),
  body('maxRetries').optional().isInt({ min: 0, max: 20 }),
  body('idempotencyKey').optional().isString(),
];

const delayedJobValidation = [
  body('name').isString().trim().notEmpty().withMessage('name is required'),
  body('scheduledAt').isISO8601().withMessage('scheduledAt must be a valid ISO 8601 date'),
  body('handlerName').isString().trim().notEmpty().withMessage('handlerName is required')
    .custom((value) => {
      if (!registry.has(value)) throw new Error(`Unknown handler: ${value}`);
      return true;
    }),
  body('payload').optional().isObject(),
  body('maxRetries').optional().isInt({ min: 0, max: 20 }),
  body('idempotencyKey').optional().isString(),
];

const immediateJobValidation = [
  body('name').isString().trim().notEmpty().withMessage('name is required'),
  body('handlerName').isString().trim().notEmpty().withMessage('handlerName is required')
    .custom((value) => {
      if (!registry.has(value)) throw new Error(`Unknown handler: ${value}`);
      return true;
    }),
  body('payload').optional().isObject(),
  body('maxRetries').optional().isInt({ min: 0, max: 20 }),
  body('idempotencyKey').optional().isString(),
];

const idParamValidation = [
  param('id').isUUID().withMessage('id must be a valid UUID'),
];

// --- Routes ---

/**
 * POST /api/jobs/cron
 * Create a recurring cron job.
 */
router.post('/cron', cronJobValidation, validate, async (req, res, next) => {
  try {
    const job = await jobService.createCronJob(req.body);
    res.status(201).json({ data: job });
  } catch (error) {
    next(createError(400, error.message));
  }
});

/**
 * POST /api/jobs/delayed
 * Create a one-time delayed job.
 */
router.post('/delayed', delayedJobValidation, validate, async (req, res, next) => {
  try {
    const job = await jobService.createDelayedJob(req.body);
    res.status(201).json({ data: job });
  } catch (error) {
    next(createError(400, error.message));
  }
});

/**
 * POST /api/jobs/immediate
 * Create an immediate job (fires ASAP).
 */
router.post('/immediate', immediateJobValidation, validate, async (req, res, next) => {
  try {
    const job = await jobService.createImmediateJob(req.body);
    res.status(201).json({ data: job });
  } catch (error) {
    next(createError(400, error.message));
  }
});

/**
 * GET /api/jobs
 * List jobs with optional filters.
 */
router.get('/',
  query('status').optional().isIn(['active', 'paused', 'completed', 'failed_permanent']),
  query('type').optional().isIn(['cron', 'delayed', 'immediate']),
  query('limit').optional().isInt({ min: 1, max: 100 }),
  query('offset').optional().isInt({ min: 0 }),
  validate,
  async (req, res, next) => {
    try {
      const { status, type, limit, offset } = req.query;
      const jobs = await jobService.listJobs({
        status,
        type,
        limit: limit ? parseInt(limit) : 50,
        offset: offset ? parseInt(offset) : 0,
      });
      res.json({ data: jobs, meta: { count: jobs.length } });
    } catch (error) {
      next(error);
    }
  }
);

/**
 * GET /api/jobs/:id
 * Get a single job by ID.
 */
router.get('/:id', idParamValidation, validate, async (req, res, next) => {
  try {
    const job = await jobService.getJob(req.params.id);
    if (!job) throw createError(404, 'Job not found');
    res.json({ data: job });
  } catch (error) {
    next(error);
  }
});

/**
 * GET /api/jobs/:id/history
 * Get execution history for a job.
 */
router.get('/:id/history', idParamValidation, validate, async (req, res, next) => {
  try {
    const job = await jobService.getJob(req.params.id);
    if (!job) throw createError(404, 'Job not found');

    const history = await executionService.getHistory(req.params.id);
    res.json({ data: history, meta: { count: history.length } });
  } catch (error) {
    next(error);
  }
});

/**
 * PATCH /api/jobs/:id
 * Update job properties (payload, cron, maxRetries, etc.).
 */
router.patch('/:id', idParamValidation, validate, async (req, res, next) => {
  try {
    const job = await jobService.getJob(req.params.id);
    if (!job) throw createError(404, 'Job not found');

    const allowedFields = ['name', 'cronExpression', 'payload', 'maxRetries', 'timezone'];
    const updates = {};
    for (const field of allowedFields) {
      if (req.body[field] !== undefined) {
        updates[field] = req.body[field];
      }
    }

    if (Object.keys(updates).length === 0) {
      throw createError(400, 'No valid fields to update');
    }

    const updated = await jobService.updateJob(req.params.id, updates);
    res.json({ data: updated });
  } catch (error) {
    next(error);
  }
});

/**
 * POST /api/jobs/:id/pause
 * Pause a job (stops it from being scheduled).
 */
router.post('/:id/pause', idParamValidation, validate, async (req, res, next) => {
  try {
    const job = await jobService.getJob(req.params.id);
    if (!job) throw createError(404, 'Job not found');
    if (job.status === 'paused') throw createError(409, 'Job is already paused');

    await jobService.pauseJob(req.params.id);
    res.json({ data: { id: req.params.id, status: 'paused' } });
  } catch (error) {
    next(error);
  }
});

/**
 * POST /api/jobs/:id/resume
 * Resume a paused job.
 */
router.post('/:id/resume', idParamValidation, validate, async (req, res, next) => {
  try {
    const job = await jobService.getJob(req.params.id);
    if (!job) throw createError(404, 'Job not found');
    if (job.status !== 'paused') throw createError(409, 'Job is not paused');

    await jobService.resumeJob(req.params.id);
    res.json({ data: { id: req.params.id, status: 'active' } });
  } catch (error) {
    next(error);
  }
});

/**
 * POST /api/jobs/:id/trigger
 * Manually trigger a job to run immediately (regardless of schedule).
 */
router.post('/:id/trigger', idParamValidation, validate, async (req, res, next) => {
  try {
    const job = await jobService.getJob(req.params.id);
    if (!job) throw createError(404, 'Job not found');

    // Set next_run_at to now so the scheduler picks it up immediately
    await jobService.triggerNow(req.params.id);
    res.json({ data: { id: req.params.id, message: 'Job triggered, will execute shortly' } });
  } catch (error) {
    next(error);
  }
});

/**
 * DELETE /api/jobs/:id
 * Delete a job permanently.
 */
router.delete('/:id', idParamValidation, validate, async (req, res, next) => {
  try {
    const job = await jobService.getJob(req.params.id);
    if (!job) throw createError(404, 'Job not found');

    await jobService.deleteJob(req.params.id);
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

module.exports = router;

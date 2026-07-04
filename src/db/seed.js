/**
 * Seed script: creates demo jobs you can immediately run and observe.
 * 
 * Usage:
 *   npm run migrate   (first time only)
 *   npm run seed      (creates these jobs)
 *   npm start         (starts processing them)
 */
require('dotenv').config();
require('../db/pool');

const { JobService } = require('../services/job-service');
const logger = require('../lib/logger');

async function seed() {
  const jobService = new JobService();

  logger.info('=== Seeding demo jobs ===');

  // ─────────────────────────────────────────────────────────────
  // CRON JOBS (recurring)
  // ─────────────────────────────────────────────────────────────

  // 1. Heartbeat every minute — proves the system is alive
  await jobService.createCronJob({
    name: 'System Heartbeat',
    cronExpression: '* * * * *',          // Every minute
    handlerName: 'heartbeat',
    payload: { message: 'scheduler-alive' },
    idempotencyKey: 'heartbeat-every-minute',
  });
  logger.info('Created: System Heartbeat (every minute)');

  // 2. Compute stats every 5 minutes
  await jobService.createCronJob({
    name: 'Execution Stats (5min)',
    cronExpression: '*/5 * * * *',        // Every 5 minutes
    handlerName: 'compute-stats',
    payload: { period: '1 hour' },
    idempotencyKey: 'stats-every-5min',
  });
  logger.info('Created: Execution Stats (every 5 minutes)');

  // 3. Stale job detector every 10 minutes
  await jobService.createCronJob({
    name: 'Stale Job Detector',
    cronExpression: '*/10 * * * *',       // Every 10 minutes
    handlerName: 'detect-stale-jobs',
    payload: { staleThresholdMinutes: 15 },
    idempotencyKey: 'stale-detector-10min',
  });
  logger.info('Created: Stale Job Detector (every 10 minutes)');

  // 4. Log rotation every hour
  await jobService.createCronJob({
    name: 'Hourly Log Rotation',
    cronExpression: '0 * * * *',          // Top of every hour
    handlerName: 'log-rotation',
    payload: { logDir: './logs', maxFiles: 24 },
    idempotencyKey: 'log-rotation-hourly',
  });
  logger.info('Created: Hourly Log Rotation');

  // 5. Database cleanup daily at 3 AM
  await jobService.createCronJob({
    name: 'Daily DB Cleanup',
    cronExpression: '0 3 * * *',          // 3:00 AM daily
    handlerName: 'db-cleanup',
    payload: { retentionDays: 30 },
    idempotencyKey: 'db-cleanup-daily',
  });
  logger.info('Created: Daily DB Cleanup (3 AM)');

  // 6. Health ping every 2 minutes (checks a public endpoint)
  await jobService.createCronJob({
    name: 'External Health Check',
    cronExpression: '*/2 * * * *',        // Every 2 minutes
    handlerName: 'health-ping',
    payload: {
      url: 'https://httpstat.us/200',
      expectedStatus: 200,
      timeoutMs: 5000,
    },
    idempotencyKey: 'health-ping-2min',
  });
  logger.info('Created: External Health Check (every 2 minutes)');

  // 7. Chaos monkey every 3 minutes — demonstrates retry + DLQ
  await jobService.createCronJob({
    name: 'Chaos Monkey (Test Retries)',
    cronExpression: '*/3 * * * *',        // Every 3 minutes
    handlerName: 'chaos-monkey',
    payload: { failureRate: 0.6, minDelayMs: 200, maxDelayMs: 1000 },
    maxRetries: 3,
    idempotencyKey: 'chaos-monkey-3min',
  });
  logger.info('Created: Chaos Monkey (every 3 minutes, 60% failure rate)');

  // ─────────────────────────────────────────────────────────────
  // DELAYED JOBS (one-time, fire in the future)
  // ─────────────────────────────────────────────────────────────

  // 8. Webhook in 2 minutes
  const twoMinutes = new Date(Date.now() + 2 * 60 * 1000);
  await jobService.createDelayedJob({
    name: 'Delayed Webhook (2min)',
    scheduledAt: twoMinutes,
    handlerName: 'send-webhook',
    payload: {
      webhookUrl: 'https://httpstat.us/200',
      event: 'test.scheduled',
      data: { message: 'This was scheduled 2 minutes ago' },
    },
    idempotencyKey: 'delayed-webhook-demo-' + Date.now(),
  });
  logger.info(`Created: Delayed Webhook (fires at ${twoMinutes.toISOString()})`);

  // 9. Batch processor in 1 minute
  const oneMinute = new Date(Date.now() + 1 * 60 * 1000);
  await jobService.createDelayedJob({
    name: 'Delayed Batch Job (1min)',
    scheduledAt: oneMinute,
    handlerName: 'batch-processor',
    payload: {
      items: ['order-001', 'order-002', 'order-003', 'order-004', 'order-005',
              'order-006', 'order-007', 'order-008', 'order-009', 'order-010'],
      batchSize: 3,
      processingDelayMs: 100,
    },
    idempotencyKey: 'delayed-batch-demo-' + Date.now(),
  });
  logger.info(`Created: Delayed Batch Job (fires at ${oneMinute.toISOString()})`);

  // 10. File cleanup in 5 minutes
  const fiveMinutes = new Date(Date.now() + 5 * 60 * 1000);
  await jobService.createDelayedJob({
    name: 'Delayed File Cleanup (5min)',
    scheduledAt: fiveMinutes,
    handlerName: 'file-cleanup',
    payload: { directory: './logs', maxAgeDays: 1, pattern: '*.log' },
    idempotencyKey: 'delayed-cleanup-demo-' + Date.now(),
  });
  logger.info(`Created: Delayed File Cleanup (fires at ${fiveMinutes.toISOString()})`);

  // ─────────────────────────────────────────────────────────────
  // IMMEDIATE JOBS (fire ASAP)
  // ─────────────────────────────────────────────────────────────

  // 11. Immediate heartbeat
  await jobService.createImmediateJob({
    name: 'Immediate Heartbeat',
    handlerName: 'heartbeat',
    payload: { message: 'immediate-test' },
    idempotencyKey: 'immediate-heartbeat-' + Date.now(),
  });
  logger.info('Created: Immediate Heartbeat (fires ASAP)');

  // 12. Immediate stats computation
  await jobService.createImmediateJob({
    name: 'Immediate Stats',
    handlerName: 'compute-stats',
    payload: { period: '24 hours' },
    idempotencyKey: 'immediate-stats-' + Date.now(),
  });
  logger.info('Created: Immediate Stats (fires ASAP)');

  // 13. Immediate chaos monkey (will likely fail and retry)
  await jobService.createImmediateJob({
    name: 'Immediate Chaos Test',
    handlerName: 'chaos-monkey',
    payload: { failureRate: 0.8, minDelayMs: 50, maxDelayMs: 500 },
    maxRetries: 2,
    idempotencyKey: 'immediate-chaos-' + Date.now(),
  });
  logger.info('Created: Immediate Chaos Test (80% failure, 2 retries → likely DLQ)');

  // ─────────────────────────────────────────────────────────────
  logger.info('');
  logger.info('=== Seeding complete! ===');
  logger.info('');
  logger.info('Summary:');
  logger.info('  Cron jobs:      7 (recurring on schedule)');
  logger.info('  Delayed jobs:   3 (fire at specific future times)');
  logger.info('  Immediate jobs: 3 (fire ASAP)');
  logger.info('');
  logger.info('Run "npm start" to begin processing.');
  logger.info('Watch the logs to see jobs execute, retry, and (for chaos-monkey) hit the DLQ.');
  logger.info('');
  logger.info('API available at http://localhost:3000');
  logger.info('  GET  /health/status     — system dashboard');
  logger.info('  GET  /api/jobs          — list all jobs');
  logger.info('  GET  /api/dlq           — see failed jobs in dead letter queue');
  logger.info('');

  const { closePool } = require('../db/pool');
  await closePool();
}

if (require.main === module) {
  seed().catch((error) => {
    logger.error({ error }, 'Seed failed');
    process.exit(1);
  });
}

module.exports = { seed };

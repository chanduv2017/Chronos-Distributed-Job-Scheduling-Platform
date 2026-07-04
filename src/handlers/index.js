/**
 * Job Handler Registration.
 * 
 * All handlers are registered here. Each handler must be:
 *   - Idempotent: safe to call multiple times with the same input
 *   - Async: returns a Promise
 *   - Self-contained: doesn't rely on external state that could change between retries
 * 
 * Handler signature: async (payload, context) => result
 */
const { registry } = require('../services/handler-registry');
const { getPool } = require('../db/pool');
const logger = require('../lib/logger');
const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

// ============================================================
// 1. DATABASE CLEANUP — Removes old execution records
// ============================================================
registry.register('db-cleanup', async (payload, context) => {
  const { retentionDays = 30, table = 'job_executions' } = payload;

  logger.info({ retentionDays, table, jobId: context.jobId }, 'Starting database cleanup');

  const pool = getPool();

  // Idempotent: deleting already-deleted rows is a no-op
  const result = await pool.query(
    `DELETE FROM job_executions
     WHERE created_at < NOW() - INTERVAL '${parseInt(retentionDays)} days'
     AND status IN ('success', 'failed')`,
  );

  const deletedCount = result.rowCount;
  logger.info({ deletedCount, retentionDays }, 'Database cleanup complete');

  return { deletedCount, retentionDays };
}, { description: 'Clean up old execution records from the database', timeout: 120000 });


// ============================================================
// 2. HEALTH PING — Checks if an external URL is reachable
// ============================================================
registry.register('health-ping', async (payload, context) => {
  const { url, expectedStatus = 200, timeoutMs = 10000 } = payload;

  logger.info({ url, jobId: context.jobId }, 'Pinging health endpoint');

  const result = await httpGet(url, timeoutMs);

  if (result.statusCode !== expectedStatus) {
    throw new Error(`Health check failed: ${url} returned ${result.statusCode}, expected ${expectedStatus}`);
  }

  logger.info({ url, statusCode: result.statusCode, durationMs: result.durationMs }, 'Health ping successful');

  return { url, statusCode: result.statusCode, durationMs: result.durationMs, healthy: true };
}, { description: 'Ping an external URL and verify it returns expected status', timeout: 30000 });


// ============================================================
// 3. FILE CLEANUP — Removes old files from a directory
// ============================================================
registry.register('file-cleanup', async (payload, context) => {
  const { directory, maxAgeDays = 7, pattern = '*' } = payload;

  logger.info({ directory, maxAgeDays, pattern, jobId: context.jobId }, 'Starting file cleanup');

  const targetDir = path.resolve(directory);

  if (!fs.existsSync(targetDir)) {
    logger.warn({ directory: targetDir }, 'Directory does not exist, skipping');
    return { deleted: 0, skipped: true };
  }

  const cutoff = Date.now() - (maxAgeDays * 24 * 60 * 60 * 1000);
  const files = fs.readdirSync(targetDir);
  let deletedCount = 0;

  for (const file of files) {
    // Simple pattern matching (supports *.log, *.tmp, etc.)
    if (pattern !== '*') {
      const ext = pattern.replace('*', '');
      if (!file.endsWith(ext)) continue;
    }

    const filePath = path.join(targetDir, file);
    const stat = fs.statSync(filePath);

    if (stat.isFile() && stat.mtimeMs < cutoff) {
      fs.unlinkSync(filePath);
      deletedCount++;
      logger.debug({ file: filePath }, 'Deleted old file');
    }
  }

  logger.info({ deletedCount, directory: targetDir }, 'File cleanup complete');
  return { deletedCount, directory: targetDir };
}, { description: 'Remove old files from a directory based on age', timeout: 60000 });


// ============================================================
// 4. DATA AGGREGATOR — Computes stats and stores them
// ============================================================
registry.register('compute-stats', async (payload, context) => {
  const { period = '1 hour' } = payload;

  logger.info({ period, jobId: context.jobId }, 'Computing job execution stats');

  const pool = getPool();

  // Compute stats for the given period
  const stats = await pool.query(`
    SELECT 
      COUNT(*) FILTER (WHERE status = 'success') as success_count,
      COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
      COUNT(*) FILTER (WHERE status = 'running') as running_count,
      COUNT(*) FILTER (WHERE status = 'dlq') as dlq_count,
      ROUND(AVG(duration_ms) FILTER (WHERE status = 'success')) as avg_duration_ms,
      MAX(duration_ms) FILTER (WHERE status = 'success') as max_duration_ms,
      MIN(duration_ms) FILTER (WHERE status = 'success') as min_duration_ms
    FROM job_executions
    WHERE started_at > NOW() - INTERVAL '${period}'
  `);

  const row = stats.rows[0];
  const report = {
    period,
    successCount: parseInt(row.success_count || 0),
    failedCount: parseInt(row.failed_count || 0),
    runningCount: parseInt(row.running_count || 0),
    dlqCount: parseInt(row.dlq_count || 0),
    avgDurationMs: parseInt(row.avg_duration_ms || 0),
    maxDurationMs: parseInt(row.max_duration_ms || 0),
    minDurationMs: parseInt(row.min_duration_ms || 0),
    computedAt: new Date().toISOString(),
  };

  logger.info({ report }, 'Stats computed');

  return report;
}, { description: 'Compute execution statistics for monitoring', timeout: 30000 });


// ============================================================
// 5. WEBHOOK SENDER — POST data to an external webhook URL
// ============================================================
registry.register('send-webhook', async (payload, context) => {
  const { webhookUrl, event, data, secret } = payload;

  logger.info({ webhookUrl, event, jobId: context.jobId }, 'Sending webhook');

  const body = JSON.stringify({
    event,
    data,
    timestamp: new Date().toISOString(),
    jobId: context.jobId,
    attempt: context.attempt,
  });

  const result = await httpPost(webhookUrl, body, {
    'Content-Type': 'application/json',
    ...(secret ? { 'X-Webhook-Secret': secret } : {}),
  });

  if (result.statusCode >= 400) {
    throw new Error(`Webhook failed: ${webhookUrl} returned ${result.statusCode} - ${result.body}`);
  }

  logger.info({ webhookUrl, statusCode: result.statusCode }, 'Webhook sent successfully');
  return { webhookUrl, statusCode: result.statusCode, event };
}, { description: 'Send a POST request to an external webhook URL', timeout: 30000 });


// ============================================================
// 6. STALE JOB DETECTOR — Finds and reports stuck jobs
// ============================================================
registry.register('detect-stale-jobs', async (payload, context) => {
  const { staleThresholdMinutes = 30 } = payload;

  logger.info({ staleThresholdMinutes, jobId: context.jobId }, 'Detecting stale jobs');

  const pool = getPool();

  const result = await pool.query(`
    SELECT id, name, handler_name, locked_by, locked_at
    FROM jobs
    WHERE locked_at IS NOT NULL
      AND locked_at < NOW() - INTERVAL '${parseInt(staleThresholdMinutes)} minutes'
      AND status = 'active'
  `);

  const staleJobs = result.rows;

  if (staleJobs.length > 0) {
    logger.warn({ count: staleJobs.length, staleJobs }, 'Found stale jobs!');

    // Auto-unlock stale jobs so they can be retried
    for (const job of staleJobs) {
      await pool.query(
        'UPDATE jobs SET locked_by = NULL, locked_at = NULL WHERE id = $1',
        [job.id]
      );
      logger.info({ jobId: job.id, name: job.name }, 'Unlocked stale job');
    }
  } else {
    logger.info('No stale jobs found');
  }

  return { staleJobsFound: staleJobs.length, unlocked: staleJobs.map(j => j.name) };
}, { description: 'Detect and unlock jobs stuck in locked state', timeout: 30000 });


// ============================================================
// 7. LOG ROTATOR — Writes a summary log entry (simulates rotation)
// ============================================================
registry.register('log-rotation', async (payload, context) => {
  const { logDir = './logs', maxFiles = 10 } = payload;

  logger.info({ logDir, maxFiles, jobId: context.jobId }, 'Running log rotation');

  const targetDir = path.resolve(logDir);

  // Create log directory if it doesn't exist
  if (!fs.existsSync(targetDir)) {
    fs.mkdirSync(targetDir, { recursive: true });
  }

  // Write a timestamped log file (simulates rotation)
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const logFile = path.join(targetDir, `scheduler-${timestamp}.log`);
  const content = JSON.stringify({
    rotatedAt: new Date().toISOString(),
    workerId: context.workerId,
    jobId: context.jobId,
    message: 'Log rotation executed',
  }, null, 2);

  fs.writeFileSync(logFile, content);

  // Remove oldest files if exceeding maxFiles
  const files = fs.readdirSync(targetDir)
    .filter(f => f.startsWith('scheduler-') && f.endsWith('.log'))
    .sort()
    .reverse();

  let removedCount = 0;
  if (files.length > maxFiles) {
    const toRemove = files.slice(maxFiles);
    for (const file of toRemove) {
      fs.unlinkSync(path.join(targetDir, file));
      removedCount++;
    }
  }

  logger.info({ logFile, removedCount }, 'Log rotation complete');
  return { logFile, removedCount, totalFiles: Math.min(files.length, maxFiles) };
}, { description: 'Rotate log files, keeping only the most recent N files', timeout: 30000 });


// ============================================================
// 8. HEARTBEAT — Simple job that proves the system is working
// ============================================================
registry.register('heartbeat', async (payload, context) => {
  const { message = 'alive' } = payload;

  const heartbeat = {
    message,
    workerId: context.workerId,
    jobId: context.jobId,
    attempt: context.attempt,
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    memoryMB: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
  };

  logger.info({ heartbeat }, 'Heartbeat');
  return heartbeat;
}, { description: 'Simple heartbeat to verify the scheduler is running', timeout: 10000 });


// ============================================================
// 9. RANDOM FAILURE — For testing retry and DLQ behavior
// ============================================================
registry.register('chaos-monkey', async (payload, context) => {
  const { failureRate = 0.5, minDelayMs = 100, maxDelayMs = 2000 } = payload;

  const delay = Math.floor(Math.random() * (maxDelayMs - minDelayMs) + minDelayMs);
  logger.info({ failureRate, delay, attempt: context.attempt, jobId: context.jobId }, 'Chaos monkey running');

  // Simulate work
  await new Promise((resolve) => setTimeout(resolve, delay));

  // Randomly fail based on failure rate
  if (Math.random() < failureRate) {
    const errors = [
      'Connection timeout',
      'Service unavailable',
      'Rate limit exceeded',
      'Internal server error',
      'Database deadlock detected',
    ];
    const error = errors[Math.floor(Math.random() * errors.length)];
    throw new Error(`Chaos monkey: ${error} (attempt ${context.attempt})`);
  }

  logger.info({ attempt: context.attempt }, 'Chaos monkey survived!');
  return { survived: true, attempt: context.attempt, delayMs: delay };
}, { description: 'Randomly fails to test retry logic and DLQ', timeout: 10000 });


// ============================================================
// 10. BATCH PROCESSOR — Processes items in batches
// ============================================================
registry.register('batch-processor', async (payload, context) => {
  const { items = [], batchSize = 5, processingDelayMs = 50 } = payload;

  logger.info({ totalItems: items.length, batchSize, jobId: context.jobId }, 'Starting batch processing');

  const results = { processed: 0, failed: 0, batches: 0 };

  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    results.batches++;

    for (const item of batch) {
      try {
        // Simulate processing each item
        await new Promise((resolve) => setTimeout(resolve, processingDelayMs));
        results.processed++;
        logger.debug({ item, batch: results.batches }, 'Item processed');
      } catch (error) {
        results.failed++;
        logger.warn({ item, error: error.message }, 'Item processing failed');
      }
    }

    logger.info({ batch: results.batches, processed: results.processed }, 'Batch complete');
  }

  logger.info({ results }, 'Batch processing complete');
  return results;
}, { description: 'Process a list of items in configurable batches', timeout: 300000 });


// ============================================================
// UTILITY FUNCTIONS
// ============================================================

function httpGet(url, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const client = url.startsWith('https') ? https : http;

    const req = client.get(url, { timeout: timeoutMs }, (res) => {
      let body = '';
      res.on('data', (chunk) => (body += chunk));
      res.on('end', () => {
        resolve({
          statusCode: res.statusCode,
          body,
          durationMs: Date.now() - start,
        });
      });
    });

    req.on('timeout', () => {
      req.destroy();
      reject(new Error(`Request timeout after ${timeoutMs}ms: ${url}`));
    });

    req.on('error', reject);
  });
}

function httpPost(url, body, headers = {}, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const client = parsed.protocol === 'https:' ? https : http;

    const options = {
      hostname: parsed.hostname,
      port: parsed.port,
      path: parsed.pathname + parsed.search,
      method: 'POST',
      headers: {
        'Content-Length': Buffer.byteLength(body),
        ...headers,
      },
      timeout: timeoutMs,
    };

    const req = client.request(options, (res) => {
      let responseBody = '';
      res.on('data', (chunk) => (responseBody += chunk));
      res.on('end', () => {
        resolve({ statusCode: res.statusCode, body: responseBody });
      });
    });

    req.on('timeout', () => {
      req.destroy();
      reject(new Error(`Request timeout after ${timeoutMs}ms: ${url}`));
    });

    req.on('error', reject);
    req.write(body);
    req.end();
  });
}


logger.info({ handlerCount: registry.list().length }, 'All handlers registered');

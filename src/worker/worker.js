/**
 * Worker: Consumes jobs from Redis Stream and executes them.
 * 
 * Guarantees:
 *   1. Exactly-once execution via Redis lock + PostgreSQL idempotency key
 *   2. Automatic retry with exponential backoff on failure
 *   3. Dead Letter Queue for permanently failed jobs
 *   4. Lock auto-extension for long-running jobs
 *   5. Graceful shutdown (finish current job, release locks)
 * 
 * Execution flow:
 *   1. XREADGROUP from Redis Stream (blocks until message available)
 *   2. Acquire distributed lock for the specific job
 *   3. Verify job is still valid (not cancelled, not already completed)
 *   4. Execute handler with timeout
 *   5. On success: ACK message, update PostgreSQL, release lock
 *   6. On failure: retry or DLQ based on retry count
 */
const { StreamManager } = require('../lib/stream-manager');
const { DistributedLock } = require('../lib/distributed-lock');
const { JobService } = require('../services/job-service');
const { ExecutionService } = require('../services/execution-service');
const { registry } = require('../services/handler-registry');
const { shouldRetry, getRetryTime } = require('../lib/retry');
const config = require('../config');
const logger = require('../lib/logger');

class Worker {
  constructor() {
    this.streamManager = new StreamManager();
    this.jobService = new JobService();
    this.executionService = new ExecutionService();
    this.running = false;
    this.currentJob = null; // Track current job for graceful shutdown
    this.workerId = config.workerId;
  }

  /**
   * Start consuming and processing jobs.
   */
  async start() {
    await this.streamManager.initialize();
    this.running = true;

    logger.info({ workerId: this.workerId }, 'Worker started');

    // Main consumption loop
    while (this.running) {
      try {
        await this._processNext();
      } catch (error) {
        logger.error({ error }, 'Error in worker loop');
        // Brief pause before retrying the loop
        await this._sleep(1000);
      }
    }

    logger.info({ workerId: this.workerId }, 'Worker stopped');
  }

  /**
   * Graceful shutdown: finish current job, then stop.
   */
  async stop() {
    logger.info({ workerId: this.workerId }, 'Worker shutdown requested');
    this.running = false;

    // Wait for current job to finish (with timeout)
    if (this.currentJob) {
      logger.info('Waiting for current job to complete...');
      const deadline = Date.now() + 30000; // 30s grace period
      while (this.currentJob && Date.now() < deadline) {
        await this._sleep(500);
      }
    }
  }

  /**
   * Process the next available message from the stream.
   */
  async _processNext() {
    // Block-read from stream (waits up to blockTimeMs for a message)
    const messages = await this.streamManager.consume(this.workerId, 1);

    if (messages.length === 0) return; // Timeout, no messages

    const { messageId, data } = messages[0];

    logger.info({
      jobId: data.jobId,
      handler: data.handlerName,
      attempt: data.attempt,
      messageId,
    }, 'Received job from stream');

    await this._executeJob(messageId, data);
  }

  /**
   * Execute a single job with all safety guarantees.
   */
  async _executeJob(messageId, data) {
    const { jobId, handlerName, payload, attempt, maxRetries } = data;
    const attemptNum = parseInt(attempt);
    const maxRetriesNum = parseInt(maxRetries);

    // 1. Acquire distributed lock (prevents duplicate execution)
    const lockKey = `${config.execution.lockPrefix}${jobId}`;
    const lock = new DistributedLock(lockKey, config.execution.lockTTLMs);

    const acquired = await lock.acquire();
    if (!acquired) {
      // Another worker is already executing this job
      logger.warn({ jobId }, 'Could not acquire job lock (another worker has it)');
      // ACK the message so it doesn't get redelivered
      await this.streamManager.acknowledge(messageId);
      return;
    }

    // 2. Start auto-extending the lock (for long-running jobs)
    lock.startAutoExtend(config.execution.lockExtendIntervalMs);

    // Track current job for graceful shutdown
    this.currentJob = { jobId, messageId, lock };

    let executionRecord = null;

    try {
      // 3. Verify handler exists
      if (!registry.has(handlerName)) {
        throw new Error(`No handler registered: ${handlerName}`);
      }

      // 4. Record execution start (idempotency check)
      executionRecord = await this.executionService.recordStart(
        jobId, this.workerId, attemptNum
      );

      if (!executionRecord) {
        // Duplicate execution — already processed
        logger.warn({ jobId, attempt: attemptNum }, 'Duplicate execution, skipping');
        await this.streamManager.acknowledge(messageId);
        return;
      }

      // 5. Execute the handler
      const parsedPayload = typeof payload === 'string' ? JSON.parse(payload) : payload;
      const context = {
        jobId,
        attempt: attemptNum,
        workerId: this.workerId,
        executionId: executionRecord.id,
      };

      logger.info({ jobId, handler: handlerName, attempt: attemptNum }, 'Executing handler');

      const result = await registry.execute(handlerName, parsedPayload, context);

      // 6. Success! Record it and update job state
      await this.executionService.recordSuccess(executionRecord.id);
      await this.jobService.completeExecution(jobId);
      await this.streamManager.acknowledge(messageId);

      logger.info({ jobId, handler: handlerName, attempt: attemptNum }, 'Job completed successfully');

    } catch (error) {
      // 7. Failure handling
      logger.error({ jobId, handler: handlerName, attempt: attemptNum, error: error.message }, 'Job execution failed');

      if (executionRecord) {
        await this.executionService.recordFailure(
          executionRecord.id, error.message, error.stack
        );
      }

      if (shouldRetry(attemptNum, maxRetriesNum)) {
        // Schedule retry with exponential backoff
        const retryAt = getRetryTime(attemptNum);
        await this.jobService.failExecution(jobId, error.message, retryAt);

        logger.info({
          jobId,
          nextAttempt: attemptNum + 1,
          retryAt,
        }, 'Job scheduled for retry');
      } else {
        // Max retries exceeded — send to DLQ
        await this.jobService.failExecution(jobId, error.message, null);
        await this.streamManager.sendToDLQ(data, error.message, error.stack);
        await this.executionService.writeToDLQTable(
          { id: jobId, handler_name: handlerName, payload, retry_count: attemptNum, max_retries: maxRetriesNum },
          error.message,
          error.stack
        );

        if (executionRecord) {
          await this.executionService.recordDLQ(executionRecord.id);
        }

        logger.error({ jobId, attempts: attemptNum }, 'Job permanently failed, sent to DLQ');
      }

      // Always ACK the stream message (we've handled it, even if failed)
      await this.streamManager.acknowledge(messageId);

    } finally {
      // 8. Release lock and clear current job
      lock.stopAutoExtend();
      await lock.release();
      this.currentJob = null;
    }
  }

  /**
   * Utility: async sleep.
   */
  _sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

module.exports = { Worker };

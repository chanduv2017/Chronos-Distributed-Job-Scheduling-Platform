/**
 * Scheduler: Runs on the LEADER node only.
 * 
 * Responsibilities:
 *   1. Poll PostgreSQL for jobs where next_run_at <= NOW()
 *   2. Publish due jobs to Redis Stream for workers to consume
 *   3. Reclaim stale/abandoned messages from crashed workers
 *   4. Compute next_run_at for cron jobs after they're enqueued
 * 
 * This is the "clock" of the system — it decides WHEN jobs should run.
 * Workers decide HOW to run them.
 */
const { JobService } = require('../services/job-service');
const { StreamManager } = require('../lib/stream-manager');
const config = require('../config');
const logger = require('../lib/logger');

class Scheduler {
  constructor() {
    this.jobService = new JobService();
    this.streamManager = new StreamManager();
    this.running = false;
    this.pollTimer = null;
    this.reclaimTimer = null;
  }

  /**
   * Start the scheduler (called when this node becomes leader).
   */
  async start() {
    if (this.running) return;

    await this.streamManager.initialize();
    this.running = true;

    logger.info('Scheduler started (leader duties active)');

    // Start polling for due jobs
    this._pollLoop();

    // Start reclaiming stale messages
    this._reclaimLoop();
  }

  /**
   * Stop the scheduler (called when this node loses leadership).
   */
  stop() {
    this.running = false;

    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }

    if (this.reclaimTimer) {
      clearTimeout(this.reclaimTimer);
      this.reclaimTimer = null;
    }

    logger.info('Scheduler stopped (leader duties suspended)');
  }

  /**
   * Main poll loop: find due jobs and enqueue them.
   */
  async _pollLoop() {
    if (!this.running) return;

    try {
      await this._enqueueDueJobs();
    } catch (error) {
      logger.error({ error }, 'Error in scheduler poll loop');
    }

    // Schedule next poll
    if (this.running) {
      this.pollTimer = setTimeout(
        () => this._pollLoop(),
        config.scheduler.cronPollIntervalMs
      );
    }
  }

  /**
   * Find and enqueue all due jobs.
   */
  async _enqueueDueJobs() {
    const dueJobs = await this.jobService.getDueJobs(config.execution.batchSize);

    if (dueJobs.length === 0) return;

    logger.info({ count: dueJobs.length }, 'Found due jobs to enqueue');

    for (const job of dueJobs) {
      try {
        // Lock the job in PostgreSQL (prevents double-enqueue)
        const locked = await this.jobService.lockJob(job.id, 'scheduler');

        if (!locked) {
          logger.debug({ jobId: job.id }, 'Job already locked, skipping');
          continue;
        }

        // Publish to Redis Stream
        await this.streamManager.publish(job);

        logger.info({ jobId: job.id, name: job.name, handler: job.handler_name }, 'Job enqueued');
      } catch (error) {
        logger.error({ jobId: job.id, error }, 'Failed to enqueue job');
        // Unlock so it can be retried next cycle
        await this.jobService.unlockJob(job.id).catch(() => {});
      }
    }
  }

  /**
   * Reclaim loop: recover abandoned messages from crashed workers.
   */
  async _reclaimLoop() {
    if (!this.running) return;

    try {
      const reclaimed = await this.streamManager.reclaimStale(
        config.workerId,
        config.streams.maxPendingAgeMs
      );

      if (reclaimed.length > 0) {
        logger.info({ count: reclaimed.length }, 'Reclaimed stale messages');
      }
    } catch (error) {
      logger.error({ error }, 'Error in reclaim loop');
    }

    if (this.running) {
      this.reclaimTimer = setTimeout(
        () => this._reclaimLoop(),
        config.streams.maxPendingAgeMs / 2 // Check at half the max pending age
      );
    }
  }
}

module.exports = { Scheduler };

/**
 * Redis Streams manager for job distribution.
 * 
 * Architecture:
 *   - Leader publishes due jobs to the pending stream
 *   - Workers consume from the stream via consumer groups
 *   - Failed jobs go to the DLQ stream
 *   - Stale pending entries are reclaimed by the leader
 * 
 * Consumer groups guarantee:
 *   - Each message is delivered to exactly one consumer in the group
 *   - Messages must be ACK'd or they remain in the PEL (Pending Entries List)
 *   - Unacknowledged messages can be reclaimed after timeout
 */
const { getClient } = require('./redis');
const config = require('../config');
const logger = require('./logger');

class StreamManager {
  constructor() {
    this.stream = config.streams.jobStream;
    this.dlqStream = config.streams.dlqStream;
    this.group = config.streams.consumerGroup;
    this.initialized = false;
  }

  /**
   * Initialize consumer group (idempotent — safe to call multiple times).
   */
  async initialize() {
    if (this.initialized) return;

    const redis = getClient();

    try {
      // Create consumer group starting from new messages
      // MKSTREAM creates the stream if it doesn't exist
      await redis.xgroup('CREATE', this.stream, this.group, '0', 'MKSTREAM');
      logger.info({ stream: this.stream, group: this.group }, 'Consumer group created');
    } catch (error) {
      // BUSYGROUP means group already exists — that's fine
      if (!error.message.includes('BUSYGROUP')) {
        throw error;
      }
    }

    try {
      await redis.xgroup('CREATE', this.dlqStream, 'dlq-processors', '0', 'MKSTREAM');
    } catch (error) {
      if (!error.message.includes('BUSYGROUP')) {
        throw error;
      }
    }

    this.initialized = true;
  }

  /**
   * Publish a job to the pending stream (called by leader).
   * @param {object} job - Job data to enqueue
   * @returns {string} Stream message ID
   */
  async publish(job) {
    const redis = getClient();

    const messageId = await redis.xadd(
      this.stream,
      '*', // Auto-generate ID
      'jobId', job.id,
      'handlerName', job.handler_name,
      'payload', JSON.stringify(job.payload),
      'attempt', (job.retry_count + 1).toString(),
      'maxRetries', job.max_retries.toString(),
      'enqueuedAt', new Date().toISOString()
    );

    logger.debug({ jobId: job.id, messageId }, 'Job published to stream');
    return messageId;
  }

  /**
   * Consume jobs from the stream (called by workers).
   * Uses XREADGROUP for consumer-group semantics.
   * 
   * @param {string} consumerId - Unique consumer name (worker ID)
   * @param {number} count - Max messages to read
   * @returns {Array} Array of { messageId, data } objects
   */
  async consume(consumerId, count = 1) {
    const redis = getClient();

    try {
      // '>' means only new messages not yet delivered to this consumer
      const results = await redis.xreadgroup(
        'GROUP', this.group, consumerId,
        'COUNT', count,
        'BLOCK', config.streams.blockTimeMs,
        'STREAMS', this.stream, '>'
      );

      if (!results) return [];

      const messages = [];
      for (const [, entries] of results) {
        for (const [messageId, fields] of entries) {
          const data = {};
          for (let i = 0; i < fields.length; i += 2) {
            data[fields[i]] = fields[i + 1];
          }
          messages.push({ messageId, data });
        }
      }

      return messages;
    } catch (error) {
      if (error.message.includes('NOGROUP')) {
        await this.initialize();
        return [];
      }
      throw error;
    }
  }

  /**
   * Acknowledge a message (mark as successfully processed).
   * @param {string} messageId - Stream message ID to ACK
   */
  async acknowledge(messageId) {
    const redis = getClient();
    await redis.xack(this.stream, this.group, messageId);
    logger.debug({ messageId }, 'Message acknowledged');
  }

  /**
   * Send a failed job to the Dead Letter Queue.
   * @param {object} job - Failed job data
   * @param {string} error - Error message
   */
  async sendToDLQ(job, errorMessage, errorStack) {
    const redis = getClient();

    await redis.xadd(
      this.dlqStream,
      '*',
      'jobId', job.jobId || job.id,
      'handlerName', job.handlerName || job.handler_name,
      'payload', typeof job.payload === 'string' ? job.payload : JSON.stringify(job.payload),
      'errorMessage', errorMessage || 'Unknown error',
      'errorStack', errorStack || '',
      'attempt', (job.attempt || job.retry_count || 0).toString(),
      'failedAt', new Date().toISOString()
    );

    logger.warn({ jobId: job.jobId || job.id }, 'Job sent to DLQ');
  }

  /**
   * Reclaim stale pending messages from crashed consumers.
   * Called by the leader to recover abandoned jobs.
   * 
   * @param {string} claimingConsumer - Consumer ID that will take ownership
   * @param {number} minIdleMs - Only reclaim messages idle longer than this
   * @returns {Array} Reclaimed messages
   */
  async reclaimStale(claimingConsumer, minIdleMs = config.streams.maxPendingAgeMs) {
    const redis = getClient();

    try {
      // XAUTOCLAIM: automatically claim messages that have been pending too long
      const [, messages] = await redis.xautoclaim(
        this.stream,
        this.group,
        claimingConsumer,
        minIdleMs,
        '0-0', // Start from beginning of PEL
        'COUNT', 10
      );

      if (messages && messages.length > 0) {
        logger.info({ count: messages.length }, 'Reclaimed stale messages');

        return messages.map(([messageId, fields]) => {
          const data = {};
          for (let i = 0; i < fields.length; i += 2) {
            data[fields[i]] = fields[i + 1];
          }
          return { messageId, data };
        });
      }

      return [];
    } catch (error) {
      // XAUTOCLAIM not available in older Redis versions
      if (error.message.includes('unknown command')) {
        logger.warn('XAUTOCLAIM not available, falling back to XPENDING + XCLAIM');
        return this._reclaimStaleFallback(claimingConsumer, minIdleMs);
      }
      throw error;
    }
  }

  /**
   * Fallback reclaim for Redis < 6.2 (uses XPENDING + XCLAIM).
   */
  async _reclaimStaleFallback(claimingConsumer, minIdleMs) {
    const redis = getClient();

    // Get pending entries
    const pending = await redis.xpending(
      this.stream, this.group, '-', '+', 10
    );

    if (!pending || pending.length === 0) return [];

    const staleIds = pending
      .filter(([, , idleTime]) => parseInt(idleTime) > minIdleMs)
      .map(([id]) => id);

    if (staleIds.length === 0) return [];

    // Claim them
    const claimed = await redis.xclaim(
      this.stream, this.group, claimingConsumer,
      minIdleMs, ...staleIds
    );

    return claimed.map(([messageId, fields]) => {
      const data = {};
      for (let i = 0; i < fields.length; i += 2) {
        data[fields[i]] = fields[i + 1];
      }
      return { messageId, data };
    });
  }

  /**
   * Get stream info (for monitoring).
   */
  async getInfo() {
    const redis = getClient();

    const streamInfo = await redis.xinfo('STREAM', this.stream).catch(() => null);
    const groupInfo = await redis.xinfo('GROUPS', this.stream).catch(() => []);

    return { stream: streamInfo, groups: groupInfo };
  }
}

module.exports = { StreamManager };

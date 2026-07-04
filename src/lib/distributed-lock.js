/**
 * Redis-based distributed lock with TTL.
 * 
 * Guarantees:
 * - Mutual exclusion: only one holder at a time
 * - Deadlock-free: TTL ensures locks expire even if holder crashes
 * - Safe release: only the holder can release (uses unique token)
 * 
 * Uses SET NX EX pattern (atomic acquire) and Lua script (atomic release).
 */
const { v4: uuidv4 } = require('uuid');
const { getClient } = require('./redis');
const logger = require('./logger');

// Lua script for safe release: only delete if value matches our token
const RELEASE_SCRIPT = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  else
    return 0
  end
`;

// Lua script for safe extend: only extend if we still hold the lock
const EXTEND_SCRIPT = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
  else
    return 0
  end
`;

class DistributedLock {
  /**
   * @param {string} key - Lock key in Redis
   * @param {number} ttlMs - Lock TTL in milliseconds
   */
  constructor(key, ttlMs) {
    this.key = key;
    this.ttlMs = ttlMs;
    this.token = uuidv4(); // Unique token to identify this lock holder
    this.acquired = false;
    this.extendInterval = null;
  }

  /**
   * Attempt to acquire the lock.
   * @returns {boolean} true if lock was acquired
   */
  async acquire() {
    const redis = getClient();
    const result = await redis.set(this.key, this.token, 'PX', this.ttlMs, 'NX');

    if (result === 'OK') {
      this.acquired = true;
      logger.debug({ key: this.key, ttlMs: this.ttlMs }, 'Lock acquired');
      return true;
    }

    return false;
  }

  /**
   * Release the lock (only if we still hold it).
   * @returns {boolean} true if lock was released
   */
  async release() {
    if (!this.acquired) return false;

    this.stopAutoExtend();

    const redis = getClient();
    const result = await redis.eval(RELEASE_SCRIPT, 1, this.key, this.token);

    this.acquired = false;

    if (result === 1) {
      logger.debug({ key: this.key }, 'Lock released');
      return true;
    }

    logger.warn({ key: this.key }, 'Lock release failed (already expired or stolen)');
    return false;
  }

  /**
   * Extend the lock TTL (only if we still hold it).
   * @returns {boolean} true if lock was extended
   */
  async extend() {
    if (!this.acquired) return false;

    const redis = getClient();
    const result = await redis.eval(EXTEND_SCRIPT, 1, this.key, this.token, this.ttlMs.toString());

    if (result === 1) {
      logger.debug({ key: this.key, ttlMs: this.ttlMs }, 'Lock extended');
      return true;
    }

    this.acquired = false;
    logger.warn({ key: this.key }, 'Lock extend failed (lost ownership)');
    return false;
  }

  /**
   * Start auto-extending the lock at regular intervals.
   * Useful for long-running jobs that need to hold the lock beyond initial TTL.
   * @param {number} intervalMs - How often to extend
   */
  startAutoExtend(intervalMs) {
    this.stopAutoExtend();

    this.extendInterval = setInterval(async () => {
      const extended = await this.extend();
      if (!extended) {
        this.stopAutoExtend();
        logger.error({ key: this.key }, 'Auto-extend failed, lock lost');
      }
    }, intervalMs);
  }

  /**
   * Stop auto-extending.
   */
  stopAutoExtend() {
    if (this.extendInterval) {
      clearInterval(this.extendInterval);
      this.extendInterval = null;
    }
  }

  /**
   * Check if this instance currently holds the lock.
   */
  isHeld() {
    return this.acquired;
  }
}

/**
 * Convenience: acquire a lock, run a function, release the lock.
 * @param {string} key - Lock key
 * @param {number} ttlMs - Lock TTL
 * @param {Function} fn - Async function to execute while holding the lock
 * @param {object} options - { autoExtendMs, maxWaitMs, retryIntervalMs }
 */
async function withLock(key, ttlMs, fn, options = {}) {
  const { autoExtendMs, maxWaitMs = 0, retryIntervalMs = 100 } = options;
  const lock = new DistributedLock(key, ttlMs);

  const deadline = maxWaitMs > 0 ? Date.now() + maxWaitMs : 0;

  // Try to acquire, with optional retry
  let acquired = await lock.acquire();
  while (!acquired && deadline > 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, retryIntervalMs));
    acquired = await lock.acquire();
  }

  if (!acquired) {
    throw new Error(`Failed to acquire lock: ${key}`);
  }

  try {
    if (autoExtendMs) {
      lock.startAutoExtend(autoExtendMs);
    }
    return await fn();
  } finally {
    await lock.release();
  }
}

module.exports = { DistributedLock, withLock };

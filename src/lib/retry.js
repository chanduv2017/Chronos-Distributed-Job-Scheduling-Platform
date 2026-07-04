/**
 * Exponential backoff retry logic with jitter.
 * 
 * Formula: delay = min(baseDelay * 2^attempt + jitter, maxDelay)
 * 
 * Jitter prevents thundering herd when multiple workers retry simultaneously.
 */
const config = require('../config');

/**
 * Calculate the delay before the next retry attempt.
 * @param {number} attempt - Current attempt number (0-based)
 * @param {object} options - Override defaults
 * @returns {number} Delay in milliseconds
 */
function calculateBackoff(attempt, options = {}) {
  const {
    baseDelayMs = config.execution.baseRetryDelayMs,
    maxDelayMs = config.execution.maxRetryDelayMs,
    jitterFactor = 0.2, // ±20% jitter
  } = options;

  // Exponential: 1s, 2s, 4s, 8s, 16s, ...
  const exponentialDelay = baseDelayMs * Math.pow(2, attempt);

  // Cap at max
  const cappedDelay = Math.min(exponentialDelay, maxDelayMs);

  // Add jitter (random ±20%)
  const jitter = cappedDelay * jitterFactor * (Math.random() * 2 - 1);

  return Math.max(0, Math.round(cappedDelay + jitter));
}

/**
 * Calculate the absolute time when a retry should be attempted.
 * @param {number} attempt - Current attempt number (0-based)
 * @returns {Date} When to retry
 */
function getRetryTime(attempt, options = {}) {
  const delayMs = calculateBackoff(attempt, options);
  return new Date(Date.now() + delayMs);
}

/**
 * Check if a job should be retried or sent to DLQ.
 * @param {number} retryCount - How many times it has been retried
 * @param {number} maxRetries - Maximum allowed retries
 * @returns {boolean} true if should retry, false if should go to DLQ
 */
function shouldRetry(retryCount, maxRetries) {
  return retryCount < maxRetries;
}

module.exports = { calculateBackoff, getRetryTime, shouldRetry };

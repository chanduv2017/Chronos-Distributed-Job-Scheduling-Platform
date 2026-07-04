/**
 * Handler Registry: Maps handler names to executable functions.
 * 
 * All job handlers must be:
 *   1. Registered here before jobs can reference them
 *   2. Idempotent — safe to run multiple times with the same input
 *   3. Async — return a Promise
 * 
 * Handler signature: async (payload, context) => result
 *   - payload: the job's payload data (from jobs.payload)
 *   - context: { jobId, attempt, workerId, executionId }
 *   - result: any (logged but not stored)
 */
const logger = require('../lib/logger');

class HandlerRegistry {
  constructor() {
    this.handlers = new Map();
  }

  /**
   * Register a job handler.
   * @param {string} name - Handler name (must match jobs.handler_name)
   * @param {Function} fn - Async handler function
   * @param {object} options - { timeout, description }
   */
  register(name, fn, options = {}) {
    if (this.handlers.has(name)) {
      logger.warn({ handler: name }, 'Handler already registered, overwriting');
    }

    this.handlers.set(name, {
      fn,
      timeout: options.timeout || 300000, // Default 5 min timeout
      description: options.description || '',
    });

    logger.info({ handler: name, description: options.description }, 'Handler registered');
  }

  /**
   * Get a handler by name.
   * @param {string} name - Handler name
   * @returns {object|null} { fn, timeout, description }
   */
  get(name) {
    return this.handlers.get(name) || null;
  }

  /**
   * Check if a handler exists.
   */
  has(name) {
    return this.handlers.has(name);
  }

  /**
   * List all registered handlers.
   */
  list() {
    const result = [];
    for (const [name, { description, timeout }] of this.handlers) {
      result.push({ name, description, timeout });
    }
    return result;
  }

  /**
   * Execute a handler with timeout protection.
   * @param {string} name - Handler name
   * @param {object} payload - Job payload
   * @param {object} context - Execution context
   * @returns {any} Handler result
   */
  async execute(name, payload, context) {
    const handler = this.get(name);

    if (!handler) {
      throw new Error(`No handler registered for: ${name}`);
    }

    // Race between handler execution and timeout
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => {
        reject(new Error(`Handler '${name}' timed out after ${handler.timeout}ms`));
      }, handler.timeout);
    });

    return Promise.race([
      handler.fn(payload, context),
      timeoutPromise,
    ]);
  }
}

// Singleton instance
const registry = new HandlerRegistry();

module.exports = { HandlerRegistry, registry };

/**
 * Structured logger using pino.
 */
const pino = require('pino');
const config = require('../config');

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  base: { workerId: config.workerId },
  timestamp: pino.stdTimeFunctions.isoTime,
});

module.exports = logger;

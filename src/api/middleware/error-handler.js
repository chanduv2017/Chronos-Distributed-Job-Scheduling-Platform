/**
 * Centralized error handling middleware.
 */
const createError = require('http-errors');
const logger = require('../../lib/logger');

/**
 * 404 handler — catches requests that didn't match any route.
 */
function notFoundHandler(req, res, next) {
  next(createError(404, `Route not found: ${req.method} ${req.originalUrl}`));
}

/**
 * Global error handler — formats all errors consistently.
 */
function errorHandler(err, req, res, next) {
  const status = err.status || err.statusCode || 500;
  const message = err.message || 'Internal Server Error';

  // Log 5xx errors as errors, 4xx as warnings
  if (status >= 500) {
    logger.error({ err, method: req.method, url: req.originalUrl }, 'Server error');
  } else {
    logger.warn({ status, message, method: req.method, url: req.originalUrl }, 'Client error');
  }

  res.status(status).json({
    error: {
      status,
      message,
      ...(process.env.NODE_ENV !== 'production' && err.stack ? { stack: err.stack } : {}),
    },
  });
}

module.exports = { notFoundHandler, errorHandler };

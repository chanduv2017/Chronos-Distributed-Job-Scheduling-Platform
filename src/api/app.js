/**
 * Express application setup with middleware stack.
 */
const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const pinoHttp = require('pino-http');
const logger = require('../lib/logger');
const { errorHandler, notFoundHandler } = require('./middleware/error-handler');
const jobRoutes = require('./routes/jobs');
const dlqRoutes = require('./routes/dlq');
const healthRoutes = require('./routes/health');

function createApp() {
  const app = express();

  // --- Security ---
  app.use(helmet());                          // Security headers (XSS, HSTS, etc.)
  app.use(cors());                            // CORS for cross-origin requests

  // --- Performance ---
  app.use(compression());                     // Gzip response compression

  // --- Rate limiting ---
  const limiter = rateLimit({
    windowMs: 60 * 1000,                      // 1 minute window
    max: 100,                                 // 100 requests per window per IP
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many requests, please try again later' },
  });
  app.use('/api/', limiter);

  // --- Request logging ---
  app.use(pinoHttp({ logger, autoLogging: { ignore: (req) => req.url === '/health' } }));

  // --- Body parsing ---
  app.use(express.json({ limit: '1mb' }));
  app.use(express.urlencoded({ extended: true }));

  // --- Routes ---
  app.use('/health', healthRoutes);
  app.use('/api/jobs', jobRoutes);
  app.use('/api/dlq', dlqRoutes);

  // --- Error handling ---
  app.use(notFoundHandler);
  app.use(errorHandler);

  return app;
}

module.exports = { createApp };

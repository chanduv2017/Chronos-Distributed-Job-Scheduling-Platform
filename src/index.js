/**
 * Main entry point: starts Express API + Leader Election + Worker.
 * 
 * Every node runs:
 *   - Express API server (for job management)
 *   - Worker (consumes and executes jobs from Redis Stream)
 *   - Leader election participant
 * 
 * One node additionally runs:
 *   - Scheduler (polls PG for due jobs, publishes to stream)
 */
require('dotenv').config();

const { createApp } = require('./api/app');
const { LeaderElection } = require('./lib/leader-election');
const { Scheduler } = require('./scheduler/scheduler');
const { Worker } = require('./worker/worker');
const { closePool } = require('./db/pool');
const { closeRedis } = require('./lib/redis');
const config = require('./config');
const logger = require('./lib/logger');

// --- Register job handlers ---
require('./handlers');

async function main() {
  logger.info({ workerId: config.workerId }, 'Starting distributed job scheduler node');

  // 1. Start Express API
  const app = createApp();
  const port = parseInt(process.env.API_PORT || '3000');
  const server = app.listen(port, () => {
    logger.info({ port }, 'Express API server started');
  });

  // 2. Initialize scheduler and worker
  const scheduler = new Scheduler();
  const worker = new Worker();

  // 3. Leader election
  const election = new LeaderElection(
    () => {
      logger.info('This node is now the LEADER — starting scheduler');
      scheduler.start();
    },
    () => {
      logger.warn('This node LOST leadership — stopping scheduler');
      scheduler.stop();
    }
  );

  election.start();

  // 4. Start worker (runs consumption loop)
  const workerPromise = worker.start();

  // 5. Graceful shutdown
  const shutdown = async (signal) => {
    logger.info({ signal }, 'Shutdown signal received');

    // Stop accepting new HTTP requests
    server.close();

    // Stop leader duties
    await election.stop();
    scheduler.stop();

    // Stop worker (waits for current job to finish)
    await worker.stop();

    // Close connections
    await closePool();
    await closeRedis();

    logger.info('Shutdown complete');
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  process.on('uncaughtException', (error) => {
    logger.fatal({ error }, 'Uncaught exception');
    shutdown('uncaughtException');
  });

  process.on('unhandledRejection', (reason) => {
    logger.fatal({ reason }, 'Unhandled rejection');
    shutdown('unhandledRejection');
  });

  await workerPromise;
}

main().catch((error) => {
  logger.fatal({ error }, 'Fatal error during startup');
  process.exit(1);
});

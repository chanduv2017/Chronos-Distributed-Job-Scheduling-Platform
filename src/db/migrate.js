/**
 * Database migration script.
 * Creates all tables needed for the job scheduling platform.
 */
const { Pool } = require('pg');
require('dotenv').config();
const config = require('../config');
const logger = require('../lib/logger');

const MIGRATION_SQL = `
-- Jobs table: the source of truth for all job definitions
CREATE TABLE IF NOT EXISTS jobs (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name            VARCHAR(255) NOT NULL,
  type            VARCHAR(20) NOT NULL CHECK (type IN ('cron', 'delayed', 'immediate')),
  status          VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'completed', 'failed_permanent')),
  
  -- Schedule
  cron_expression VARCHAR(100),                    -- For cron jobs (standard 5-field)
  scheduled_at    TIMESTAMPTZ,                     -- For delayed jobs (exact fire time)
  timezone        VARCHAR(50) DEFAULT 'UTC',
  
  -- Handler
  handler_name    VARCHAR(255) NOT NULL,            -- Maps to registered handler function
  payload         JSONB NOT NULL DEFAULT '{}',      -- Data passed to handler
  
  -- Retry configuration
  max_retries     INT NOT NULL DEFAULT 5,
  retry_count     INT NOT NULL DEFAULT 0,
  last_error      TEXT,
  
  -- Idempotency
  idempotency_key VARCHAR(255) UNIQUE,             -- Prevents duplicate scheduling
  
  -- Execution tracking
  last_run_at     TIMESTAMPTZ,
  next_run_at     TIMESTAMPTZ,                     -- Pre-computed next fire time
  locked_by       VARCHAR(255),                    -- Worker that currently owns execution
  locked_at       TIMESTAMPTZ,
  
  -- Metadata
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  
  -- Indexes for common queries
  CONSTRAINT valid_cron CHECK (type != 'cron' OR cron_expression IS NOT NULL),
  CONSTRAINT valid_delayed CHECK (type != 'delayed' OR scheduled_at IS NOT NULL)
);

-- Job execution history (audit trail)
CREATE TABLE IF NOT EXISTS job_executions (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  worker_id       VARCHAR(255) NOT NULL,
  status          VARCHAR(20) NOT NULL CHECK (status IN ('running', 'success', 'failed', 'timeout', 'dlq')),
  attempt         INT NOT NULL DEFAULT 1,
  
  started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at    TIMESTAMPTZ,
  duration_ms     INT,
  
  error_message   TEXT,
  error_stack     TEXT,
  
  -- Idempotency: prevent duplicate execution records for same job+scheduled time
  execution_key   VARCHAR(255) UNIQUE,
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Dead Letter Queue table (permanent storage for failed jobs)
CREATE TABLE IF NOT EXISTS dead_letter_queue (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  handler_name    VARCHAR(255) NOT NULL,
  payload         JSONB NOT NULL,
  error_message   TEXT,
  error_stack     TEXT,
  retry_count     INT NOT NULL,
  max_retries     INT NOT NULL,
  
  failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  requeued_at     TIMESTAMPTZ,                     -- Set when manually retried
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_jobs_next_run ON jobs(next_run_at) WHERE status = 'active' AND next_run_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_type_status ON jobs(type, status);
CREATE INDEX IF NOT EXISTS idx_jobs_locked ON jobs(locked_by, locked_at) WHERE locked_by IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_handler ON jobs(handler_name);
CREATE INDEX IF NOT EXISTS idx_executions_job_id ON job_executions(job_id);
CREATE INDEX IF NOT EXISTS idx_executions_status ON job_executions(status, started_at);
CREATE INDEX IF NOT EXISTS idx_dlq_job_id ON dead_letter_queue(job_id);
CREATE INDEX IF NOT EXISTS idx_dlq_failed_at ON dead_letter_queue(failed_at);

-- Function to auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for auto-updating updated_at
DROP TRIGGER IF EXISTS update_jobs_updated_at ON jobs;
CREATE TRIGGER update_jobs_updated_at
  BEFORE UPDATE ON jobs
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at_column();
`;

async function migrate() {
  const pool = new Pool(config.postgres);

  try {
    logger.info('Running database migrations...');
    await pool.query(MIGRATION_SQL);
    logger.info('Migrations completed successfully');
  } catch (error) {
    logger.error({ error }, 'Migration failed');
    throw error;
  } finally {
    await pool.end();
  }
}

if (require.main === module) {
  migrate().catch(() => process.exit(1));
}

module.exports = { migrate };

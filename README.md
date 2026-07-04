# Distributed Job Scheduler

A fault-tolerant distributed job scheduling platform built with Node.js, Redis Streams, and PostgreSQL.

## Features

- **Cron jobs** — recurring schedules using standard 5-field cron expressions
- **Delayed jobs** — one-time execution at a specific future time
- **Immediate jobs** — fire ASAP
- **Exactly-once execution** — Redis distributed locks + PostgreSQL idempotency keys
- **10+ concurrent workers** — horizontal scaling via Redis consumer groups
- **Leader election** — single leader handles scheduling, all nodes handle execution
- **Exponential backoff retries** — with jitter to prevent thundering herd
- **Dead Letter Queue** — permanently failed jobs stored for inspection/replay
- **Graceful shutdown** — finish current job before stopping
- **Lock auto-extension** — long-running jobs don't lose their lock

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         LEADER NODE                                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐   │
│  │   Leader     │    │  Scheduler   │    │  Stale Message       │   │
│  │  Election    │───▶│  (polls PG)  │    │  Reclaimer           │   │
│  └──────────────┘    └──────┬───────┘    └──────────────────────┘   │
│                              │                                        │
│                              │ publish due jobs                        │
│                              ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    Redis Stream                                │   │
│  │              (scheduler:jobs:pending)                          │   │
│  └──────────────────────────────────────────────────────────────┘   │
│         │              │              │              │                │
│         ▼              ▼              ▼              ▼                │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐        │
│  │ Worker 1 │   │ Worker 2 │   │ Worker 3 │   │Worker 10 │  ...    │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘        │
│       │               │               │               │              │
│       ▼               ▼               ▼               ▼              │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │              Redis Distributed Locks                           │   │
│  │         (one lock per job = exactly-once)                     │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                              │                                        │
│                              ▼                                        │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    PostgreSQL                                  │   │
│  │   jobs │ job_executions │ dead_letter_queue                   │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Node.js 20+ (for local development)

### Run with Docker (10 workers)

```bash
docker-compose up --build
```

### Run locally

```bash
# Start dependencies
docker-compose up postgres redis -d

# Install dependencies
npm install

# Run migrations
npm run migrate

# Seed sample jobs
npm run seed

# Start a worker node (run multiple terminals for multiple workers)
npm start
```

### Scale workers

```bash
# Docker: scale to 15 workers
docker-compose up --scale worker=15

# Local: just start more processes
WORKER_ID=worker-1 npm start
WORKER_ID=worker-2 npm start
# ... etc
```

## API

Start the API server by setting `API_PORT`:

```bash
API_PORT=3000 npm start
```

### Create a cron job
```bash
curl -X POST http://localhost:3000/jobs/cron \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Hourly Cleanup",
    "cronExpression": "0 * * * *",
    "handlerName": "cleanup-sessions",
    "payload": { "maxAge": "24h" }
  }'
```

### Create a delayed job
```bash
curl -X POST http://localhost:3000/jobs/delayed \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Send Welcome Email",
    "scheduledAt": "2026-05-09T10:00:00Z",
    "handlerName": "send-email",
    "payload": { "to": "user@example.com", "subject": "Welcome!" }
  }'
```

### Create an immediate job
```bash
curl -X POST http://localhost:3000/jobs/immediate \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Urgent Sync",
    "handlerName": "sync-external-data",
    "payload": { "sourceUrl": "https://api.example.com/data" }
  }'
```

### List jobs
```bash
curl http://localhost:3000/jobs
curl http://localhost:3000/jobs?status=active&type=cron
```

### Pause/Resume/Delete
```bash
curl -X POST http://localhost:3000/jobs/{id}/pause
curl -X POST http://localhost:3000/jobs/{id}/resume
curl -X DELETE http://localhost:3000/jobs/{id}
```

## Writing Handlers

Register handlers in `src/handlers/index.js`:

```javascript
const { registry } = require('../services/handler-registry');

registry.register('my-handler', async (payload, context) => {
  // payload = job's payload data
  // context = { jobId, attempt, workerId, executionId }
  
  // YOUR BUSINESS LOGIC HERE
  // Must be IDEMPOTENT (safe to run multiple times)
  
  return { result: 'done' };
}, {
  description: 'What this handler does',
  timeout: 60000, // 60 second timeout
});
```

### Idempotency Patterns

```javascript
// Pattern 1: Check-before-write
registry.register('send-notification', async (payload) => {
  const alreadySent = await db.query(
    'SELECT 1 FROM sent_notifications WHERE idempotency_key = $1',
    [payload.notificationId]
  );
  if (alreadySent.rows.length > 0) return { skipped: true };
  
  await sendNotification(payload);
  await db.query(
    'INSERT INTO sent_notifications (idempotency_key) VALUES ($1)',
    [payload.notificationId]
  );
});

// Pattern 2: Upsert (INSERT ON CONFLICT)
registry.register('sync-record', async (payload) => {
  await db.query(
    `INSERT INTO records (external_id, data, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (external_id) DO UPDATE SET data = $2, updated_at = NOW()`,
    [payload.externalId, payload.data]
  );
});

// Pattern 3: Idempotent state transition
registry.register('process-order', async (payload) => {
  // Only transition if in expected state (prevents double-processing)
  const result = await db.query(
    `UPDATE orders SET status = 'processed'
     WHERE id = $1 AND status = 'pending'
     RETURNING id`,
    [payload.orderId]
  );
  if (result.rowCount === 0) return { skipped: true }; // Already processed
  
  await fulfillOrder(payload.orderId);
});
```

## How Exactly-Once Works

The system uses multiple layers to prevent duplicate execution:

1. **Redis Stream Consumer Groups** — each message delivered to exactly one consumer
2. **Distributed Lock** — only one worker can hold the lock for a specific job
3. **PostgreSQL `locked_by`** — database-level ownership tracking
4. **Idempotency Key** — `job_executions.execution_key` prevents duplicate records
5. **Handler-level idempotency** — business logic checks before acting

If any layer fails, the others catch it. This provides "effectively exactly-once" semantics.

## Configuration

All settings via environment variables (see `src/config.js`):

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_ID` | auto-generated | Unique worker identifier |
| `PG_HOST` | localhost | PostgreSQL host |
| `PG_PORT` | 5432 | PostgreSQL port |
| `PG_DATABASE` | job_scheduler | Database name |
| `PG_USER` | postgres | Database user |
| `PG_PASSWORD` | postgres | Database password |
| `REDIS_HOST` | localhost | Redis host |
| `REDIS_PORT` | 6379 | Redis port |
| `LOG_LEVEL` | info | Logging level |

## Monitoring

Check stream health:
```bash
redis-cli XINFO STREAM scheduler:jobs:pending
redis-cli XINFO GROUPS scheduler:jobs:pending
redis-cli XLEN scheduler:jobs:dlq
```

Check PostgreSQL:
```sql
-- Active jobs due to run
SELECT * FROM jobs WHERE status = 'active' AND next_run_at <= NOW();

-- Failed jobs in DLQ
SELECT * FROM dead_letter_queue ORDER BY failed_at DESC LIMIT 20;

-- Execution stats
SELECT status, COUNT(*) FROM job_executions GROUP BY status;

-- Stuck jobs (locked too long)
SELECT * FROM jobs WHERE locked_at < NOW() - INTERVAL '10 minutes';
```

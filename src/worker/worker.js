// src/worker/worker.js
import db from "../db.js";
import redis from "../redis.js";
import { CronExpressionParser } from 'cron-parser';

const QUEUE_NAME = "job_queue";

async function processJob(jobId) {
  try {
    console.log(`Processing job ${jobId}`);

    // Get job from DB
    const result = await db.query(
      `SELECT * FROM jobs WHERE id = $1`,
      [jobId]
    );

    if (result.rows.length === 0) {
      return;
    }

    const job = result.rows[0];

    // ---- JOB LOGIC (FAKE WORK) ----
    console.log("Job payload:", job.payload);
    // Simulate work
    await new Promise((r) => setTimeout(r, 2000));
    // --------------------------------

    // Calculate next run time (for cron jobs)
    const interval = CronExpressionParser.parse(job.cron_expression);
    const nextRunAt = interval.next().toDate();

    // Mark success
    await db.query(
      `UPDATE jobs
       SET status = 'PENDING',
           next_run_at = $1,
           retry_count = 0
       WHERE id = $2`,
      [nextRunAt, jobId]
    );

    console.log(`Job ${jobId} completed`);
  } catch (err) {
    console.error(`Job ${jobId} failed`, err);
  }
}

async function startWorker() {
  console.log("Worker started");

  while (true) {
    try {
      // Block until a job is available
      const jobId = await redis.rPop(QUEUE_NAME);

      if (jobId) {
        await processJob(jobId);
      } else {
        // Sleep briefly to avoid tight loop
        await new Promise((r) => setTimeout(r, 1000));
      }
    } catch (err) {
      console.error("Worker error:", err);
    }
  }
}

startWorker();

// src/scheduler/scheduler.js
import db from "../db.js";
import redis from "../redis.js";

const QUEUE_NAME = "job_queue";

async function scheduleJobs() {
  try {
    // 1. Find jobs that are ready to run
    const result = await db.query(
      `SELECT id
       FROM jobs
       WHERE status = 'PENDING'
       AND next_run_at <= NOW()
       LIMIT 5`
    );

    for (const row of result.rows) {
      const jobId = row.id;

      // 2. Push job ID to Redis queue
      await redis.lPush(QUEUE_NAME, jobId);

      // 3. Mark job as QUEUED
      await db.query(
        `UPDATE jobs SET status = 'QUEUED' WHERE id = $1`,
        [jobId]
      );

      console.log(`Scheduled job ${jobId}`);
    }
  } catch (err) {
    console.error("Scheduler error:", err);
  }
}

// Run scheduler every 5 seconds
setInterval(scheduleJobs, 5000);

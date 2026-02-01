// src/routes/jobs.js
import express from "express";
import { v4 as uuidv4 } from "uuid";
// const cronParser = require("cron-parser");
import { CronExpressionParser } from 'cron-parser';
import db from "../db.js";

const router = express.Router();

// POST /jobs
router.post("/", async (req, res) => {
  try {
    const { cron, payload, maxRetries = 3 } = req.body;

    if (!cron) {
      return res.status(400).json({ error: "cron expression required" });
    }

    // calculate next run time
    const interval = CronExpressionParser.parse(cron);
    const nextRunAt = interval.next().toDate();

    const jobId = uuidv4();

    await db.query(
      `INSERT INTO jobs (id, cron_expression, payload, status, next_run_at, max_retries)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        jobId,
        cron,
        payload || {},
        "PENDING",
        nextRunAt,
        maxRetries
      ]
    );

    res.status(201).json({
      jobId,
      status: "PENDING",
      nextRunAt
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to create job" });
  }
});

export default router;

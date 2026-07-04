/**
 * Cron expression parser and next-fire-time calculator.
 * Supports standard 5-field cron: minute hour day-of-month month day-of-week
 * 
 * Uses node-cron for validation and a manual calculator for next fire time.
 */
const cron = require('node-cron');

/**
 * Validate a cron expression.
 * @param {string} expression - 5-field cron expression
 * @returns {boolean}
 */
function isValid(expression) {
  return cron.validate(expression);
}

/**
 * Calculate the next fire time after a given date.
 * Handles standard 5-field cron expressions.
 * 
 * @param {string} expression - Cron expression (minute hour dom month dow)
 * @param {Date} after - Calculate next time after this date (default: now)
 * @returns {Date|null} Next fire time, or null if expression is invalid
 */
function getNextFireTime(expression, after = new Date()) {
  if (!isValid(expression)) return null;

  const parts = expression.trim().split(/\s+/);
  if (parts.length < 5) return null;

  const [minuteExpr, hourExpr, domExpr, monthExpr, dowExpr] = parts;

  // Start checking from the next minute
  const candidate = new Date(after);
  candidate.setSeconds(0, 0);
  candidate.setMinutes(candidate.getMinutes() + 1);

  // Search up to 2 years ahead (prevent infinite loop)
  const maxDate = new Date(after);
  maxDate.setFullYear(maxDate.getFullYear() + 2);

  while (candidate < maxDate) {
    if (
      matchesField(candidate.getMonth() + 1, monthExpr, 1, 12) &&
      matchesDayOfMonth(candidate, domExpr) &&
      matchesField(candidate.getDay(), dowExpr, 0, 7) &&
      matchesField(candidate.getHours(), hourExpr, 0, 23) &&
      matchesField(candidate.getMinutes(), minuteExpr, 0, 59)
    ) {
      return candidate;
    }

    candidate.setMinutes(candidate.getMinutes() + 1);
  }

  return null;
}

/**
 * Check if a value matches a cron field expression.
 */
function matchesField(value, expr, min, max) {
  // Handle day-of-week where 7 = 0 (Sunday)
  if (max === 7 && value === 0) value = 7;

  const parts = expr.split(',');

  for (const part of parts) {
    if (part === '*') return true;

    // Step values: */5 or 1-10/2
    if (part.includes('/')) {
      const [range, step] = part.split('/');
      const stepNum = parseInt(step);
      let start = min;
      let end = max;

      if (range !== '*') {
        if (range.includes('-')) {
          [start, end] = range.split('-').map(Number);
        } else {
          start = parseInt(range);
        }
      }

      for (let i = start; i <= end; i += stepNum) {
        if (i === value) return true;
      }
      continue;
    }

    // Range: 1-5
    if (part.includes('-')) {
      const [start, end] = part.split('-').map(Number);
      if (value >= start && value <= end) return true;
      continue;
    }

    // Exact value
    if (parseInt(part) === value) return true;
  }

  return false;
}

/**
 * Check if a date matches the day-of-month expression.
 */
function matchesDayOfMonth(date, expr) {
  const day = date.getDate();
  const lastDay = new Date(date.getFullYear(), date.getMonth() + 1, 0).getDate();

  if (expr === 'L') return day === lastDay;

  return matchesField(day, expr, 1, 31);
}

module.exports = { isValid, getNextFireTime };

/**
 * Request validation middleware using express-validator.
 */
const { validationResult } = require('express-validator');

/**
 * Middleware that checks validation results and returns 400 if invalid.
 * Use after express-validator check/body/param chains.
 */
function validate(req, res, next) {
  const errors = validationResult(req);

  if (!errors.isEmpty()) {
    return res.status(400).json({
      error: {
        status: 400,
        message: 'Validation failed',
        details: errors.array().map((e) => ({
          field: e.path,
          message: e.msg,
          value: e.value,
        })),
      },
    });
  }

  next();
}

module.exports = { validate };

/**
 * Redis client singleton with connection management.
 */
const Redis = require('ioredis');
const config = require('../config');
const logger = require('./logger');

let client = null;
let subscriber = null;

function getClient() {
  if (!client) {
    client = new Redis(config.redis);

    client.on('error', (err) => {
      logger.error({ error: err.message }, 'Redis client error');
    });

    client.on('connect', () => {
      logger.info('Redis client connected');
    });
  }
  return client;
}

function getSubscriber() {
  if (!subscriber) {
    subscriber = new Redis(config.redis);

    subscriber.on('error', (err) => {
      logger.error({ error: err.message }, 'Redis subscriber error');
    });
  }
  return subscriber;
}

async function closeRedis() {
  if (client) {
    await client.quit();
    client = null;
  }
  if (subscriber) {
    await subscriber.quit();
    subscriber = null;
  }
}

module.exports = { getClient, getSubscriber, closeRedis };

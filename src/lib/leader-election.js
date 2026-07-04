/**
 * Leader Election using Redis.
 * 
 * Only one node in the cluster is the "leader" at any time.
 * The leader is responsible for:
 *   - Evaluating cron expressions and enqueuing due jobs
 *   - Enqueuing delayed jobs when their scheduled_at time arrives
 *   - Reclaiming stale/abandoned jobs from crashed workers
 * 
 * Non-leaders are workers that only consume and execute jobs.
 * 
 * Algorithm:
 *   1. Try to SET leader lock with NX + TTL
 *   2. If acquired, start renewing periodically
 *   3. If lost, stop leader duties and re-enter election
 */
const { DistributedLock } = require('./distributed-lock');
const config = require('../config');
const logger = require('./logger');

class LeaderElection {
  constructor(onBecomeLeader, onLoseLeadership) {
    this.lock = null;
    this.isLeader = false;
    this.running = false;
    this.electionTimer = null;
    this.renewTimer = null;
    this.onBecomeLeader = onBecomeLeader;
    this.onLoseLeadership = onLoseLeadership;
  }

  /**
   * Start participating in leader election.
   */
  start() {
    this.running = true;
    this._runElection();
    logger.info('Leader election started');
  }

  /**
   * Stop participating (resign if leader).
   */
  async stop() {
    this.running = false;

    if (this.electionTimer) {
      clearTimeout(this.electionTimer);
      this.electionTimer = null;
    }

    if (this.renewTimer) {
      clearInterval(this.renewTimer);
      this.renewTimer = null;
    }

    if (this.isLeader && this.lock) {
      await this.lock.release();
      this.isLeader = false;
      logger.info('Resigned from leadership');
    }
  }

  /**
   * Internal: attempt to become leader.
   */
  async _runElection() {
    if (!this.running) return;

    try {
      if (!this.isLeader) {
        this.lock = new DistributedLock(
          config.leader.lockKey,
          config.leader.lockTTLMs
        );

        const acquired = await this.lock.acquire();

        if (acquired) {
          this.isLeader = true;
          logger.info('Became leader');

          // Start renewing the lock
          this._startRenewal();

          // Notify callback
          if (this.onBecomeLeader) {
            this.onBecomeLeader();
          }
        }
      }
    } catch (error) {
      logger.error({ error }, 'Error during leader election');
    }

    // Schedule next election attempt
    if (this.running) {
      const interval = this.isLeader
        ? config.leader.lockTTLMs // If leader, check less frequently
        : config.leader.electionIntervalMs; // If not leader, try often

      this.electionTimer = setTimeout(() => this._runElection(), interval);
    }
  }

  /**
   * Internal: periodically renew the leader lock.
   */
  _startRenewal() {
    if (this.renewTimer) {
      clearInterval(this.renewTimer);
    }

    this.renewTimer = setInterval(async () => {
      if (!this.running || !this.isLeader) return;

      try {
        const extended = await this.lock.extend();

        if (!extended) {
          // Lost leadership
          this.isLeader = false;
          clearInterval(this.renewTimer);
          this.renewTimer = null;

          logger.warn('Lost leadership (lock extend failed)');

          if (this.onLoseLeadership) {
            this.onLoseLeadership();
          }
        }
      } catch (error) {
        logger.error({ error }, 'Error renewing leader lock');
        this.isLeader = false;
        clearInterval(this.renewTimer);
        this.renewTimer = null;

        if (this.onLoseLeadership) {
          this.onLoseLeadership();
        }
      }
    }, config.leader.renewIntervalMs);
  }

  /**
   * Check if this node is currently the leader.
   */
  amILeader() {
    return this.isLeader;
  }
}

module.exports = { LeaderElection };

const Storage = require('./storage');
const Config = require('./config');
const { calculateBackoff } = require('./utils');

class Queue {
  constructor() {
    this.storage = new Storage();
    this.config = new Config();
  }

  async enqueue(jobData) {
    // Validate job data
    if (!jobData.id || !jobData.command) {
      throw new Error('Job must have id and command fields');
    }

    // Check if job already exists
    const existing = await this.storage.getJob(jobData.id);
    if (existing) {
      throw new Error(`Job with id ${jobData.id} already exists`);
    }

    const job = {
      id: jobData.id,
      command: jobData.command,
      state: 'pending',
      attempts: 0,
      max_retries: jobData.max_retries || this.config.get('max_retries')
    };

    return await this.storage.createJob(job);
  }

  async getJob(id) {
    return await this.storage.getJob(id);
  }

  async listJobs(state = null) {
    return await this.storage.listJobs(state);
  }

  async getStats() {
    const stats = await this.storage.getStats();
    const statsMap = {};
    stats.forEach(stat => {
      statsMap[stat.state] = stat.count;
    });
    
    return {
      pending: statsMap.pending || 0,
      processing: statsMap.processing || 0,
      completed: statsMap.completed || 0,
      failed: statsMap.failed || 0,
      dead: statsMap.dead || 0,
      total: Object.values(statsMap).reduce((a, b) => a + b, 0)
    };
  }

  async getDLQJobs() {
    return await this.storage.getDLQJobs();
  }

  async retryDLQJob(id) {
    const maxRetries = this.config.get('max_retries');
    return await this.storage.retryDLQJob(id, maxRetries);
  }

  async acquireJob(workerId) {
    return await this.storage.acquireJob(workerId);
  }

  async completeJob(jobId, result) {
    const updates = {
      state: 'completed',
      output: result.stdout,
      worker_id: null
    };
    
    if (result.stderr) {
      updates.error = result.stderr;
    }
    
    return await this.storage.updateJob(jobId, updates);
  }

  async failJob(jobId, result, workerId) {
    const job = await this.storage.getJob(jobId);
    if (!job) return null;

    const attempts = job.attempts + 1;
    const maxRetries = job.max_retries;
    const backoffBase = this.config.get('backoff_base');

    if (attempts >= maxRetries) {
      // Move to DLQ
      return await this.storage.updateJob(jobId, {
        state: 'dead',
        attempts,
        error: result.stderr || result.error || 'Max retries exceeded',
        output: result.stdout,
        worker_id: null,
        next_retry_at: null
      });
    } else {
      // Schedule retry with exponential backoff
      const delaySeconds = calculateBackoff(attempts, backoffBase);
      const nextRetryAt = new Date(Date.now() + delaySeconds * 1000).toISOString();
      
      return await this.storage.updateJob(jobId, {
        state: 'failed',
        attempts,
        error: result.stderr || result.error || 'Job execution failed',
        output: result.stdout,
        worker_id: null,
        next_retry_at: nextRetryAt
      });
    }
  }

  async releaseJob(jobId) {
    const job = await this.storage.getJob(jobId);
    if (!job) return null;

    // If job was processing, reset to pending (in case of worker crash)
    if (job.state === 'processing') {
      return await this.storage.updateJob(jobId, {
        state: 'pending',
        worker_id: null
      });
    }

    return job;
  }

  async close() {
    await this.storage.close();
  }
}

module.exports = Queue;

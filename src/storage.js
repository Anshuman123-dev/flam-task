const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

class Storage {
  constructor(dbPath = path.join(process.cwd(), 'queuectl.db')) {
    this.dbPath = dbPath;
    this.db = new Database(dbPath);
    this.init();
  }

  init() {
    // Create jobs table
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        next_retry_at TEXT,
        worker_id TEXT,
        output TEXT,
        error TEXT
      );

      CREATE INDEX IF NOT EXISTS idx_state ON jobs(state);
      CREATE INDEX IF NOT EXISTS idx_next_retry ON jobs(next_retry_at);
      CREATE INDEX IF NOT EXISTS idx_worker ON jobs(worker_id);
    `);
  }

  createJob(job) {
    const now = new Date().toISOString();
    const stmt = this.db.prepare(`
      INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    
    stmt.run(
      job.id,
      job.command,
      job.state || 'pending',
      job.attempts || 0,
      job.max_retries || 3,
      now,
      now
    );
    
    return this.getJob(job.id);
  }

  getJob(id) {
    const stmt = this.db.prepare('SELECT * FROM jobs WHERE id = ?');
    return stmt.get(id);
  }

  updateJob(id, updates) {
    const fields = [];
    const values = [];
    
    updates.updated_at = new Date().toISOString();
    
    for (const [key, value] of Object.entries(updates)) {
      fields.push(`${key} = ?`);
      values.push(value);
    }
    
    values.push(id);
    
    const stmt = this.db.prepare(`
      UPDATE jobs SET ${fields.join(', ')} WHERE id = ?
    `);
    
    stmt.run(...values);
    return this.getJob(id);
  }

  acquireJob(workerId) {
    // Try to acquire a pending job or a failed job that's ready for retry
    const now = new Date().toISOString();
    const stmt = this.db.prepare(`
      UPDATE jobs 
      SET state = 'processing', worker_id = ?, updated_at = ?
      WHERE id = (
        SELECT id FROM jobs
        WHERE (state = 'pending' OR (state = 'failed' AND (next_retry_at IS NULL OR next_retry_at <= ?)))
        ORDER BY created_at ASC
        LIMIT 1
      )
      RETURNING *
    `);
    
    const job = stmt.get(workerId, now, now);
    return job || null;
  }

  listJobs(state = null) {
    let query = 'SELECT * FROM jobs';
    const params = [];
    
    if (state) {
      query += ' WHERE state = ?';
      params.push(state);
    }
    
    query += ' ORDER BY created_at DESC';
    
    const stmt = this.db.prepare(query);
    return stmt.all(...params);
  }

  getDLQJobs() {
    const stmt = this.db.prepare("SELECT * FROM jobs WHERE state = 'dead' ORDER BY updated_at DESC");
    return stmt.all();
  }

  getStats() {
    const stmt = this.db.prepare(`
      SELECT 
        state,
        COUNT(*) as count
      FROM jobs
      GROUP BY state
    `);
    return stmt.all();
  }

  retryDLQJob(id, maxRetries) {
    const job = this.getJob(id);
    if (!job || job.state !== 'dead') {
      throw new Error(`Job ${id} not found in DLQ`);
    }
    
    return this.updateJob(id, {
      state: 'pending',
      attempts: 0,
      max_retries: maxRetries,
      next_retry_at: null,
      worker_id: null,
      error: null
    });
  }

  close() {
    this.db.close();
  }
}

module.exports = Storage;


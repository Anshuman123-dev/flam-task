const { MongoClient } = require('mongodb');
require('dotenv').config();

// Shared MongoDB client instance for connection pooling
let sharedClient = null;
let sharedClientConnectionString = null;

class Storage {
  constructor(connectionString = null) {
    this.connectionString = connectionString || process.env.MONGODB_URI || 'mongodb://localhost:27017';
    this.dbName = process.env.MONGODB_DB_NAME || 'queuectl';
    this.collectionName = 'jobs';
    this.client = null;
    this.db = null;
    this.collection = null;
  }

  async connect() {
    // Use shared client if connection string matches
    if (sharedClient && sharedClientConnectionString === this.connectionString) {
      try {
        // Verify connection is still alive
        await sharedClient.db(this.dbName).admin().ping();
        this.client = sharedClient;
        this.db = sharedClient.db(this.dbName);
        this.collection = this.db.collection(this.collectionName);
        return;
      } catch (error) {
        // Connection lost, reset shared client
        sharedClient = null;
        sharedClientConnectionString = null;
      }
    }

    if (this.client && this.db && this.collection) {
      // Verify connection is still alive
      try {
        await this.db.admin().ping();
        return;
      } catch (error) {
        // Connection lost, reconnect
        this.client = null;
        this.db = null;
        this.collection = null;
      }
    }

    try {
      // Create or reuse shared client
      if (!sharedClient || sharedClientConnectionString !== this.connectionString) {
        // Close old client if connection string changed
        if (sharedClient && sharedClientConnectionString !== this.connectionString) {
          try {
            await sharedClient.close();
          } catch (error) {
            // Ignore errors when closing old client
          }
        }
        
        sharedClient = new MongoClient(this.connectionString, {
          maxPoolSize: 10,
          minPoolSize: 2,
          serverSelectionTimeoutMS: 5000,
          socketTimeoutMS: 45000,
        });
        await sharedClient.connect();
        sharedClientConnectionString = this.connectionString;
      }
      
      this.client = sharedClient;
      this.db = this.client.db(this.dbName);
      this.collection = this.db.collection(this.collectionName);
      
      // Create indexes (only once, but safe to call multiple times)
      await this.collection.createIndex({ state: 1 });
      await this.collection.createIndex({ next_retry_at: 1 });
      await this.collection.createIndex({ worker_id: 1 });
      await this.collection.createIndex({ created_at: 1 });
      await this.collection.createIndex({ id: 1 }, { unique: true });
    } catch (error) {
      throw new Error(`Failed to connect to MongoDB at ${this.connectionString}. Please ensure MongoDB is running. Error: ${error.message}`);
    }
  }

  async close() {
    // Don't close shared client, just clear local references
    // The shared client will be closed when the process exits
    this.client = null;
    this.db = null;
    this.collection = null;
  }

  // Static method to close shared client (useful for cleanup)
  static async closeSharedClient() {
    if (sharedClient) {
      await sharedClient.close();
      sharedClient = null;
      sharedClientConnectionString = null;
    }
  }

  async createJob(job) {
    await this.connect();
    const now = new Date().toISOString();
    
    const jobDoc = {
      _id: job.id,
      id: job.id,
      command: job.command,
      state: job.state || 'pending',
      attempts: job.attempts || 0,
      max_retries: job.max_retries || 3,
      created_at: now,
      updated_at: now,
      next_retry_at: null,
      worker_id: null,
      output: null,
      error: null
    };

    await this.collection.insertOne(jobDoc);
    return this.getJob(job.id);
  }

  async getJob(id) {
    await this.connect();
    const job = await this.collection.findOne({ id: id });
    return job;
  }

  async updateJob(id, updates) {
    await this.connect();
    updates.updated_at = new Date().toISOString();
    
    await this.collection.updateOne(
      { id: id },
      { $set: updates }
    );
    
    return this.getJob(id);
  }

  async acquireJob(workerId) {
    await this.connect();
    const now = new Date().toISOString();
    
    // Try to find and acquire a pending job or a failed job ready for retry
    // Query: (state = 'pending') OR (state = 'failed' AND (next_retry_at IS NULL OR next_retry_at <= now))
    const query = {
      $or: [
        { state: 'pending' },
        { 
          state: 'failed',
          $or: [
            { next_retry_at: null },
            { next_retry_at: { $lte: now } }
          ]
        }
      ]
    };

    // Use findOneAndUpdate with atomic operation to prevent race conditions
    // This ensures only one worker can acquire a job at a time
    const result = await this.collection.findOneAndUpdate(
      query,
      {
        $set: {
          state: 'processing',
          worker_id: workerId,
          updated_at: now
        }
      },
      {
        sort: { created_at: 1 },
        returnDocument: 'after'
      }
    );

    return result.value || null;
  }

  async listJobs(state = null) {
    await this.connect();
    const query = state ? { state: state } : {};
    
    const jobs = await this.collection
      .find(query)
      .sort({ created_at: -1 })
      .toArray();
    
    return jobs;
  }

  async getDLQJobs() {
    await this.connect();
    const jobs = await this.collection
      .find({ state: 'dead' })
      .sort({ updated_at: -1 })
      .toArray();
    
    return jobs;
  }

  async getStats() {
    await this.connect();
    const pipeline = [
      {
        $group: {
          _id: '$state',
          count: { $sum: 1 }
        }
      }
    ];
    
    const stats = await this.collection.aggregate(pipeline).toArray();
    return stats.map(stat => ({
      state: stat._id,
      count: stat.count
    }));
  }

  async retryDLQJob(id, maxRetries) {
    await this.connect();
    const job = await this.getJob(id);
    
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
}

module.exports = Storage;

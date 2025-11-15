const Queue = require('./queue');
const { executeCommand } = require('./utils');

const workerId = process.argv[2];
let isShuttingDown = false;
let currentJob = null;

async function process() {
  const queue = new Queue();
  
  // Setup graceful shutdown
  process.on('SIGINT', () => shutdown(queue));
  process.on('SIGTERM', () => shutdown(queue));
  
  console.log(`Worker ${workerId} started`);
  
  while (!isShuttingDown) {
    try {
      // Try to acquire a job
      const job = await queue.acquireJob(workerId);
      
      if (job) {
        currentJob = job;
        await executeJob(job, queue);
        currentJob = null;
      } else {
        // No jobs available, wait a bit before retrying
        await sleep(1000);
      }
    } catch (error) {
      console.error(`Worker ${workerId} error:`, error.message);
      await sleep(1000);
    }
  }
  
  // If shutting down, wait for current job to finish
  if (currentJob) {
    console.log(`Worker ${workerId} finishing current job...`);
    await executeJob(currentJob, queue);
  }
  
  await queue.close();
  console.log(`Worker ${workerId} stopped`);
  process.exit(0);
}

async function executeJob(job, queue) {
  try {
    console.log(`Worker ${workerId} processing job ${job.id}: ${job.command}`);
    
    const result = await executeCommand(job.command);
    
    if (result.success) {
      await queue.completeJob(job.id, result);
      console.log(`Worker ${workerId} completed job ${job.id}`);
    } else {
      await queue.failJob(job.id, result, workerId);
      console.log(`Worker ${workerId} failed job ${job.id} (attempt ${job.attempts + 1}/${job.max_retries})`);
    }
  } catch (error) {
    await queue.failJob(job.id, {
      success: false,
      exitCode: 1,
      stdout: '',
      stderr: error.message,
      error: error.message
    }, workerId);
    console.error(`Worker ${workerId} error executing job ${job.id}:`, error.message);
  }
}

async function shutdown(queue) {
  if (isShuttingDown) {
    return;
  }
  
  isShuttingDown = true;
  console.log(`Worker ${workerId} shutting down...`);
  
  // Wait for current job to finish (with timeout)
  if (currentJob) {
    const timeout = setTimeout(() => {
      console.log(`Worker ${workerId} timeout waiting for job, forcing shutdown`);
      process.exit(0);
    }, 30000); // 30 second timeout
    
    // Job will finish in executeJob
    while (currentJob) {
      await sleep(100);
    }
    
    clearTimeout(timeout);
  }
  
  await queue.close();
  process.exit(0);
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

if (require.main === module) {
  process().catch(error => {
    console.error('Worker process error:', error);
    process.exit(1);
  });
}


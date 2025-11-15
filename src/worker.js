const { spawn } = require('child_process');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

const workerPidFile = path.join(process.cwd(), '.queuectl-workers.pid');

async function startWorkers(count) {
  const workerScript = path.join(__dirname, 'worker-process.js');
  
  // Save worker PIDs and worker IDs
  const workerInfo = [];
  
  for (let i = 0; i < count; i++) {
    const workerId = `worker-${uuidv4()}`;
    const workerProcess = spawn('node', [workerScript, workerId], {
      detached: false,
      stdio: 'inherit'
    });
    
    workerInfo.push({
      pid: workerProcess.pid,
      workerId: workerId
    });
    
    console.log(`Started worker ${workerId} (PID: ${workerProcess.pid})`);
    
    workerProcess.on('exit', (code) => {
      console.log(`Worker ${workerId} exited with code ${code}`);
    });
  }
  
  // Save worker info to file
  fs.writeFileSync(workerPidFile, JSON.stringify(workerInfo, null, 2));
  
  // Setup graceful shutdown handlers
  process.on('SIGINT', async () => {
    await stopWorkers();
    process.exit(0);
  });
  
  process.on('SIGTERM', async () => {
    await stopWorkers();
    process.exit(0);
  });
  
  // Keep the process running
  console.log(`\n${count} worker(s) running. Press Ctrl+C to stop.\n`);
  return new Promise(() => {});
}

async function stopWorkers() {
  try {
    if (fs.existsSync(workerPidFile)) {
      const workerInfo = JSON.parse(fs.readFileSync(workerPidFile, 'utf8'));
      console.log(`\nStopping ${workerInfo.length} worker(s)...`);
      
      for (const info of workerInfo) {
        try {
          process.kill(info.pid, 'SIGTERM');
          console.log(`Sent SIGTERM to worker ${info.workerId} (PID: ${info.pid})`);
        } catch (error) {
          // Process might already be dead
          if (error.code !== 'ESRCH') {
            console.log(`Worker ${info.workerId} (PID: ${info.pid}) not found`);
          }
        }
      }
      
      // Wait a bit for graceful shutdown
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      // Force kill if still running
      for (const info of workerInfo) {
        try {
          process.kill(info.pid, 0); // Check if process exists
          process.kill(info.pid, 'SIGKILL');
          console.log(`Force killed worker ${info.workerId} (PID: ${info.pid})`);
        } catch (error) {
          // Process already dead
        }
      }
      
      fs.unlinkSync(workerPidFile);
      console.log('All workers stopped');
    } else {
      console.log('No workers found (PID file does not exist)');
    }
  } catch (error) {
    console.error('Error stopping workers:', error.message);
    throw error;
  }
}

module.exports = {
  startWorkers,
  stopWorkers
};

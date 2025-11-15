const { exec } = require('child_process');
const { promisify } = require('util');

const execAsync = promisify(exec);

function calculateBackoff(attempts, base = 2) {
  return Math.pow(base, attempts);
}

function formatDate(dateString) {
  if (!dateString) return 'N/A';
  const date = new Date(dateString);
  return date.toLocaleString();
}

function formatDuration(ms) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60000).toFixed(1)}m`;
}

async function executeCommand(command, timeout = 300000) {
  return new Promise((resolve, reject) => {
    const startTime = Date.now();
    const child = exec(command, { timeout }, (error, stdout, stderr) => {
      const duration = Date.now() - startTime;
      if (error) {
        // error.code can be exit code (number) or error code (string like 'ENOENT')
        const exitCode = typeof error.code === 'number' ? error.code : (error.code === 'ENOENT' ? 127 : 1);
        resolve({
          success: false,
          exitCode: exitCode,
          stdout: stdout || '',
          stderr: stderr || error.message,
          duration,
          error: error.message
        });
      } else {
        resolve({
          success: true,
          exitCode: 0,
          stdout: stdout || '',
          stderr: stderr || '',
          duration
        });
      }
    });
  });
}

module.exports = {
  calculateBackoff,
  formatDate,
  formatDuration,
  executeCommand
};


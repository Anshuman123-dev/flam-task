#!/usr/bin/env node

const { Command } = require('commander');
const chalk = require('chalk');
const Queue = require('./queue');
const Config = require('./config');
const { formatDate } = require('./utils');
const path = require('path');

const program = new Command();

program
  .name('queuectl')
  .description('CLI-based background job queue system')
  .version('1.0.0');

// Enqueue command
program
  .command('enqueue')
  .description('Add a new job to the queue')
  .argument('<jobData>', 'Job data as JSON string')
  .action(async (jobData) => {
    try {
      const queue = new Queue();
      const job = JSON.parse(jobData);
      const result = await queue.enqueue(job);
      console.log(chalk.green('✓ Job enqueued successfully:'));
      console.log(JSON.stringify(result, null, 2));
      await queue.close();
    } catch (error) {
      console.error(chalk.red('✗ Error:'), error.message);
      process.exit(1);
    }
  });

// Worker commands
const workerCommand = program
  .command('worker')
  .description('Manage worker processes');

workerCommand
  .command('start')
  .description('Start one or more workers')
  .option('-c, --count <number>', 'Number of workers to start', '1')
  .action(async (options) => {
    try {
      const count = parseInt(options.count, 10);
      if (isNaN(count) || count < 1) {
        throw new Error('Count must be a positive number');
      }

      const worker = require('./worker');
      console.log(chalk.blue(`Starting ${count} worker(s)...`));
      await worker.startWorkers(count);
    } catch (error) {
      console.error(chalk.red('✗ Error:'), error.message);
      process.exit(1);
    }
  });

workerCommand
  .command('stop')
  .description('Stop running workers gracefully')
  .action(async () => {
    try {
      const worker = require('./worker');
      await worker.stopWorkers();
      console.log(chalk.green('✓ Workers stopped'));
    } catch (error) {
      console.error(chalk.red('✗ Error:'), error.message);
      process.exit(1);
    }
  });

// Status command
program
  .command('status')
  .description('Show summary of all job states & active workers')
  .action(async () => {
    try {
      const queue = new Queue();
      const stats = await queue.getStats();
      
      console.log(chalk.bold('\n=== Queue Status ===\n'));
      console.log(`Pending:    ${chalk.yellow(stats.pending)}`);
      console.log(`Processing: ${chalk.blue(stats.processing)}`);
      console.log(`Completed:  ${chalk.green(stats.completed)}`);
      console.log(`Failed:     ${chalk.magenta(stats.failed)}`);
      console.log(`Dead (DLQ): ${chalk.red(stats.dead)}`);
      console.log(`Total:      ${stats.total}`);
      
      await queue.close();
    } catch (error) {
      console.error(chalk.red('✗ Error:'), error.message);
      process.exit(1);
    }
  });

// List jobs command
program
  .command('list')
  .description('List jobs by state')
  .option('-s, --state <state>', 'Filter by state (pending, processing, completed, failed, dead)')
  .action(async (options) => {
    try {
      const queue = new Queue();
      const state = options.state || null;
      const jobs = await queue.listJobs(state);
      
      if (jobs.length === 0) {
        console.log(chalk.yellow('No jobs found'));
      } else {
        console.log(chalk.bold(`\n=== Jobs${state ? ` (${state})` : ''} ===\n`));
        jobs.forEach(job => {
          console.log(chalk.bold(`ID: ${job.id}`));
          console.log(`  Command: ${job.command}`);
          console.log(`  State: ${getStateColor(job.state)}`);
          console.log(`  Attempts: ${job.attempts}/${job.max_retries}`);
          console.log(`  Created: ${formatDate(job.created_at)}`);
          console.log(`  Updated: ${formatDate(job.updated_at)}`);
          if (job.next_retry_at) {
            console.log(`  Next Retry: ${formatDate(job.next_retry_at)}`);
          }
          if (job.error) {
            console.log(`  Error: ${chalk.red(job.error)}`);
          }
          console.log('');
        });
      }
      
      await queue.close();
    } catch (error) {
      console.error(chalk.red('✗ Error:'), error.message);
      process.exit(1);
    }
  });

// DLQ commands
const dlqCommand = program
  .command('dlq')
  .description('Dead Letter Queue operations');

dlqCommand
  .command('list')
  .description('List all jobs in the Dead Letter Queue')
  .action(async () => {
    try {
      const queue = new Queue();
      const jobs = await queue.getDLQJobs();
      
      if (jobs.length === 0) {
        console.log(chalk.yellow('No jobs in DLQ'));
      } else {
        console.log(chalk.bold('\n=== Dead Letter Queue ===\n'));
        jobs.forEach(job => {
          console.log(chalk.bold(`ID: ${job.id}`));
          console.log(`  Command: ${job.command}`);
          console.log(`  Attempts: ${job.attempts}/${job.max_retries}`);
          console.log(`  Error: ${chalk.red(job.error || 'Unknown error')}`);
          console.log(`  Failed at: ${formatDate(job.updated_at)}`);
          console.log('');
        });
      }
      
      await queue.close();
    } catch (error) {
      console.error(chalk.red('✗ Error:'), error.message);
      process.exit(1);
    }
  });

dlqCommand
  .command('retry')
  .description('Retry a job from the Dead Letter Queue')
  .argument('<jobId>', 'Job ID to retry')
  .action(async (jobId) => {
    try {
      const queue = new Queue();
      const result = await queue.retryDLQJob(jobId);
      console.log(chalk.green(`✓ Job ${jobId} moved back to queue`));
      console.log(JSON.stringify(result, null, 2));
      await queue.close();
    } catch (error) {
      console.error(chalk.red('✗ Error:'), error.message);
      process.exit(1);
    }
  });

// Config commands
const configCommand = program
  .command('config')
  .description('Manage configuration');

configCommand
  .command('set')
  .description('Set a configuration value')
  .argument('<key>', 'Configuration key (max-retries, backoff-base)')
  .argument('<value>', 'Configuration value')
  .action((key, value) => {
    try {
      const config = new Config();
      const configKey = key.replace(/-/g, '_');
      const configValue = isNaN(value) ? value : parseInt(value, 10);
      
      config.set(configKey, configValue);
      console.log(chalk.green(`✓ Configuration updated: ${key} = ${value}`));
    } catch (error) {
      console.error(chalk.red('✗ Error:'), error.message);
      process.exit(1);
    }
  });

configCommand
  .command('get')
  .description('Get a configuration value')
  .argument('<key>', 'Configuration key')
  .action((key) => {
    try {
      const config = new Config();
      const configKey = key.replace(/-/g, '_');
      const value = config.get(configKey);
      console.log(`${key}: ${value}`);
    } catch (error) {
      console.error(chalk.red('✗ Error:'), error.message);
      process.exit(1);
    }
  });

configCommand
  .command('list')
  .description('List all configuration values')
  .action(() => {
    try {
      const config = new Config();
      const all = config.getAll();
      console.log(chalk.bold('\n=== Configuration ===\n'));
      Object.entries(all).forEach(([key, value]) => {
        const displayKey = key.replace(/_/g, '-');
        console.log(`${displayKey}: ${value}`);
      });
    } catch (error) {
      console.error(chalk.red('✗ Error:'), error.message);
      process.exit(1);
    }
  });

// Helper function to colorize job states
function getStateColor(state) {
  switch (state) {
    case 'pending':
      return chalk.yellow(state);
    case 'processing':
      return chalk.blue(state);
    case 'completed':
      return chalk.green(state);
    case 'failed':
      return chalk.magenta(state);
    case 'dead':
      return chalk.red(state);
    default:
      return state;
  }
}

program.parse();


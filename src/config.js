const fs = require('fs');
const path = require('path');

class Config {
  constructor(configPath = path.join(process.cwd(), '.queuectl.json')) {
    this.configPath = configPath;
    this.defaults = {
      max_retries: 3,
      backoff_base: 2,
      worker_count: 1
    };
    this.config = this.load();
  }

  load() {
    try {
      if (fs.existsSync(this.configPath)) {
        const data = fs.readFileSync(this.configPath, 'utf8');
        return { ...this.defaults, ...JSON.parse(data) };
      }
    } catch (error) {
      console.error('Error loading config:', error.message);
    }
    return { ...this.defaults };
  }

  save() {
    try {
      fs.writeFileSync(this.configPath, JSON.stringify(this.config, null, 2));
    } catch (error) {
      throw new Error(`Failed to save config: ${error.message}`);
    }
  }

  get(key) {
    return this.config[key];
  }

  set(key, value) {
    this.config[key] = value;
    this.save();
  }

  getAll() {
    return { ...this.config };
  }
}

module.exports = Config;


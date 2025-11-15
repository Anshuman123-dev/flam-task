# 🚀 Launch Checklist

## Pre-Launch Requirements

### ✅ Code Status
- [x] All source files present and correct
- [x] MongoDB integration implemented
- [x] Node.js runtime configured
- [x] All CLI commands implemented
- [x] No syntax errors
- [x] Connection pooling optimized

### 📦 Dependencies Installation
**Required Step:** Install npm dependencies
```bash
npm install
```

### 🗄️ MongoDB Setup
**Required:** MongoDB must be running

**Option 1: Local MongoDB**
- Ensure MongoDB is installed and running on `localhost:27017`
- Default connection: `mongodb://localhost:27017`

**Option 2: MongoDB Atlas (Cloud)**
- Create a `.env` file with:
  ```
  MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/
  MONGODB_DB_NAME=queuectl
  ```

**Option 3: Custom MongoDB**
- Create a `.env` file with your connection string:
  ```
  MONGODB_URI=mongodb://your-host:27017
  MONGODB_DB_NAME=queuectl
  ```

### ✅ Quick Start Commands

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Verify MongoDB is running:**
   ```bash
   # Test connection (will fail if MongoDB is not running)
   node src/index.js status
   ```

3. **Enqueue a test job:**
   ```bash
   node src/index.js enqueue '{"id":"test1","command":"echo Hello World"}'
   ```

4. **Start workers:**
   ```bash
   node src/index.js worker start --count 2
   ```

5. **Check status:**
   ```bash
   node src/index.js status
   ```

### 🔧 Optional: Make CLI Global
To use `queuectl` command directly:
```bash
npm link
```

Then you can use:
```bash
queuectl status
queuectl enqueue '{"id":"job1","command":"sleep 2"}'
```

## ✅ Ready to Launch!

After installing dependencies and ensuring MongoDB is running, you're good to go!


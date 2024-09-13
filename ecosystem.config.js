module.exports = {
  apps: [
    {
      name: 'OG Post Score Job',
      script: 'app.py',
      args: 'start',
      watch: false,
      instances: 1,
      exec_mode: 'fork',
      wait_ready: false,
      listen_timeout: 10000,
      kill_timeout: 5000,
      max_memory_restart: '5000M',
      interpreter : "/usr/bin/python3",
      env: {
        "NODE_ENV": "production",
      }
    },
  ],
};
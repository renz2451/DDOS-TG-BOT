import os

workers = int(os.environ.get('WEB_CONCURRENCY', 1))
threads = int(os.environ.get('PYTHON_MAX_THREADS', 1))

bind = f"0.0.0.0:{os.environ.get('PORT', 10000)}"
timeout = 120

# For long-running tasks
keepalive = 2
worker_class = 'sync'
import multiprocessing

# 工作进程数
workers = multiprocessing.cpu_count() * 2 + 1

# 工作模式
worker_class = 'uvicorn.workers.UvicornWorker'

# 绑定的IP和端口
bind = '0.0.0.0:8000'

# 超时时间
timeout = 300

# 最大请求数
max_requests = 1000
max_requests_jitter = 50

# 日志配置
accesslog = 'logs/access.log'
errorlog = 'logs/error.log'
loglevel = 'info' 
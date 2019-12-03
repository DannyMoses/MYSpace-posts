#!/usr/bin/python3
from pathlib import Path

LOG_DIR = "/var/log/gunicorn/"
ERROR_LOG = "/var/log/gunicorn/error.log"
ACCESS_LOG = "/var/log/gunicorn/access.log"
#LOG_DIR = Path.home().joinpath("log/gunicorn/")
#ERROR_LOG = LOG_DIR.joinpath("error.log")
#ACCESS_LOG = LOG_DIR.joinpath("access.log")

#Path.mkdir(LOG_DIR, parents=True, exist_ok=True)
#ERROR_LOG.touch(exist_ok=True)
#ACCESS_LOG.touch(exist_ok=True)

bind = "127.0.0.1:5000"
backlog = 2048

workers = 4 
worker_class = "gevent"

capture_output = False	# Send stdout/stderr print to errorlog
loglevel = "warning"
errorlog = ERROR_LOG
accesslog = ACCESS_LOG


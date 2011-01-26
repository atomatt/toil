from datetime import datetime
import logging
import time
import sys
from toil import jobqueue


def echo(arg):
    print 'echo', arg, datetime.utcnow().time().isoformat()
    time.sleep(1)
    return list(reversed(arg))


def bang(arg):
    oops


tasks = {
    'echo.bg': echo,
    'echo.fg': echo,
    'bang': bang,
}

logging.basicConfig(level=logging.DEBUG)
worker = jobqueue.Worker(sys.argv[1])
for name in tasks:
    if not sys.argv[2:] or name in sys.argv[2:]:
        print "Registering task:", name
        worker.register(name, tasks[name])
try:
    worker.run()
except KeyboardInterrupt:
    pass

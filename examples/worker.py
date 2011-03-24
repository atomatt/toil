from datetime import datetime
import logging
import time
import sys
import toil


def echo(arg):
    print 'echo', arg, datetime.utcnow().time().isoformat()
    time.sleep(0.1)
    return list(reversed(arg))


def bang(arg):
    oops


tasks = {
    'echo': echo,
    'bang': bang,
}

logging.basicConfig(level=logging.DEBUG)
worker = toil.worker(sys.argv[1])
for name in tasks:
    if not sys.argv[2:] or name in sys.argv[2:]:
        print "Registering task:", name
        worker.register(name, tasks[name])
try:
    worker.run()
except KeyboardInterrupt:
    pass

import sys
from toil import jobqueue

client = jobqueue.Client(sys.argv[1])
# One task.
client.bg('echo.bg', [1, 2, 3])
# Multiple tasks.
client.bgmulti([('echo.bg', [1, 2, 3]) for i in xrange(3)])
# Multiple tasks, as a generator.
client.bg(('echo.bg', [1, 2, 3]) for i in xrange(3))

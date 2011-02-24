import sys
from toil import jobqueue

client = jobqueue.Client(sys.argv[1])
# One task.
client.bg(jobqueue.task('echo.bg', [1, 2, 3]))
# Multiple tasks.
client.bg(*[jobqueue.task('echo.bg', [1, 2, 3]) for i in xrange(3)])
# Multiple tasks, without having to unpack args.
client.bg([jobqueue.task('echo.bg', [1, 2, 3]) for i in xrange(3)])
# Multiple tasks, as a generator.
client.bg(jobqueue.task('echo.bg', [1, 2, 3]) for i in xrange(3))

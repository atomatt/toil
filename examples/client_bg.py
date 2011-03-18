import sys
import toil

client = toil.client(sys.argv[1])
# One task.
client.send('echo.bg', [1, 2, 3])
# Multiple tasks.
client.sendmulti([('echo.bg', [1, 2, 3]) for i in xrange(3)])
# Multiple tasks, as a generator.
client.sendmulti(('echo.bg', [1, 2, 3]) for i in xrange(3))

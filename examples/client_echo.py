import sys
from toil import jobqueue

client = jobqueue.Client(sys.argv[1])
for i in range(10):
    client.send('echo.bg', [1, 2, 3])
for i in range(1):
    print client.call('echo.fg', [1, 2, 3])

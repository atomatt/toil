import sys
from toil import jobqueue

client = jobqueue.Client(sys.argv[1])
for i in range(10):
    client.bg('echo.bg', [1, 2, 3])
for i in range(1):
    print client.fg('echo.fg', [1, 2, 3])


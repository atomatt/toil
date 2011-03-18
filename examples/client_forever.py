import sys
import time
from toil import jobqueue

client = jobqueue.Client(sys.argv[1])
while True:
    print client.fg('echo.fg', [1, 2, 3])
    time.sleep(0.1)

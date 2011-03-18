import sys
import time
import toil

client = toil.client(sys.argv[1])
while True:
    print client.call('echo.fg', [1, 2, 3])
    time.sleep(0.1)

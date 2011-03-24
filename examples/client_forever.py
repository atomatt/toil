import sys
import time
import toil

client = toil.client(sys.argv[1])
while True:
    print client.call('echo', [1, 2, 3])
    time.sleep(0.1)

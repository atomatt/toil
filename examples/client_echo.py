import sys
import toil

client = toil.client(sys.argv[1])
for i in range(10):
    client.send('echo', [1, 2, 3])
for i in range(1):
    print client.call('echo', [1, 2, 3])

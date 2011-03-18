import sys
import toil

client = toil.client(sys.argv[1])
client.send('bang')
client.call('bang')

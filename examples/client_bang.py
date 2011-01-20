import sys
from toil import jobqueue

client = jobqueue.Client(sys.argv[1])
client.bg('bang')
client.fg('bang')

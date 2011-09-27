import sys
from datetime import datetime, timedelta
import time
import toil

now = datetime.utcnow()

client = toil.client(sys.argv[1])
client.schedule(now + timedelta(seconds=5), 'echo', '3')
client.schedule(timedelta(seconds=10), 'echo', '2')
client.schedule(time.time() + 0, 'echo', '1')

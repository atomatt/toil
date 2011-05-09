import sys
from datetime import datetime, timedelta
import time
import toil

now = datetime.utcnow()

client = toil.client(sys.argv[1])
client.schedule(time.time() + 1, 'echo', '1')
client.schedule(timedelta(seconds=2), 'echo', '2')
client.schedule(now + timedelta(seconds=3), 'echo', '3')

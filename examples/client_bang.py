import sys
from toil import jobqueue

client = jobqueue.Client(sys.argv[1])
client.bg(jobqueue.task('bang'))
client.fg(jobqueue.task('bang'))

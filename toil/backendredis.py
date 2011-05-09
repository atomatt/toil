"""
TODO:
    * Redis reconnect.
    * Error handling.
    * Track claimed tasks in case of complete failure.
"""

import copy
from datetime import datetime, timedelta
import logging
import uuid
import random
import simplejson as json
import time
import traceback


log = logging.getLogger(__name__)


MAX_ERRORS = 5


class Client(object):

    def __init__(self, redis):
        self._redis = redis

    def close(self):
        pass

    def call(self, name, arg=None):
        reply_to = 'toil:reply:%s' % (unicode(uuid.uuid4()),)
        self._queue(name, {'arg': arg, 'reply_to': reply_to})
        _, response = self._redis.blpop(reply_to)
        return json.loads(response)['result']

    def send(self, name, arg=None):
        self.sendmulti([(name, arg)])

    def sendmulti(self, items):
        for name, arg in items:
            self._queue(name, {'arg': arg})

    def _queue(self, name, task):
        task_id = uuid.uuid4().hex
        pipeline = self._redis.pipeline()
        pipeline.hset('toil:task', task_id, json.dumps(task))
        pipeline.lpush('toil:queue:%s' % (name,), task_id)
        pipeline.execute()

    def schedule(self, when, name, arg=None):
        # Convert datetime types to float.
        if isinstance(when, timedelta):
            when = datetime.utcnow() + when
        if isinstance(when, datetime):
            when = time.mktime(when.utctimetuple())
        # Add task to scheduler.
        task = {'name': name, 'arg': arg}
        task_id = uuid.uuid4().hex
        pipeline = self._redis.pipeline()
        pipeline.hset('toil:scheduler:task', task_id, json.dumps(task))
        pipeline.zadd('toil:scheduler:schedule', task_id, when)
        pipeline.execute()


class Worker(object):

    max_errors = MAX_ERRORS

    def __init__(self, redis, max_errors=None):
        self._redis = redis
        self._registrations = {}
        if max_errors is not None:
            self.max_errors = max_errors
        self.client = Client(redis)

    def close(self):
        pass

    def register(self, name, callable):
        self._registrations[name] = callable

    def run_forever(self, *a, **k):
        for _ in self.run_cooperatively(*a, **k):
            pass

    def run_cooperatively(self):
        while True:
            yield
            self._process_scheduler()
            self._process_queues(timeout=1)

    def _process_queues(self, timeout):
        # Randomise the queues to reduce chance of starvation.
        qnames = list(self._registrations)
        random.shuffle(qnames)
        qnames = ['toil:queue:%s' % (name,) for name in qnames]
        # Take task from a queue.
        response = self._redis.brpop(qnames, timeout=timeout)
        if response is None:
            return
        qname, task_id = response
        task = json.loads(self._redis.hget('toil:task', task_id))
        taskname = qname.split(':')[-1]
        # Call task func.
        try:
            result = self._registrations[taskname](copy.deepcopy(task['arg']))
        except Exception, e:
            log.exception(unicode(e))
            now = datetime.utcnow().isoformat()
            # Record error in task document.
            errors = task.setdefault('errors', [])
            errors.append({'time': now,
                           'error': '%s: %s' % (e.__class__.__name__,
                                                unicode(e)),
                           'detail': traceback.format_exc()})
            # Update task record.
            self._redis.hset('toil:task', task_id, json.dumps(task))
            # Move to error queue if errors has reached maximum.  Otherwise,
            # requeue it for another attempt.
            if len(errors) >= self.max_errors:
                self._redis.lpush('toil:error:%s'%taskname, task_id)
            else:
                self._redis.lpush('toil:queue:%s'%taskname, task_id)
        else:
            # Send reply, if wanted.
            reply_to = task.get('reply_to')
            if reply_to:
                self._redis.lpush(reply_to, json.dumps({'result': result}))
            # Delete task
            self._redis.hdel('toil:task', task_id)

    def _process_scheduler(self):
        # Get a list of tasks that are ready to run.
        now = time.time()
        task_ids = self._redis.zrangebyscore('toil:scheduler:schedule', '-inf', now)
        for task_id in task_ids:
            # Claim the task by removing it from the list.
            if not self._redis.zrem('toil:scheduler:schedule', task_id):
                continue
            # I win! Move the task from the scheduler to a queue.
            task = json.loads(self._redis.hget('toil:scheduler:task', task_id))
            self.client._queue(task['name'], task)
            self._redis.hdel('toil:scheduler:task', task_id)

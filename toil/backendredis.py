"""
TODO:
    * Redis reconnect.
    * Error handling.
    * Track claimed tasks in case of complete failure.
"""

import logging
import uuid
import simplejson as json


log = logging.getLogger(__name__)


MAX_ERRORS = 5


class Client(object):

    def __init__(self, redis):
        self._redis = redis

    def close(self):
        pass

    def call(self, name, arg=None):
        reply_to = 'toil:reply:%s' % (unicode(uuid.uuid4()),)
        task = {'arg': arg, 'reply_to': reply_to}
        self._redis.lpush('toil:task:%s' % (name,), json.dumps(task))
        _, response = self._redis.blpop(reply_to)
        return json.loads(response)['result']

    def send(self, name, arg=None):
        self.sendmulti([(name, arg)])

    def sendmulti(self, items):
        for name, arg in items:
            task = {'arg': arg}
            self._redis.lpush('toil:task:%s' % (name,), json.dumps(task))


class Worker(object):

    max_errors = MAX_ERRORS

    def __init__(self, redis, max_errors=None):
        self._redis = redis
        self._registrations = {}
        if max_errors is not None:
            self.max_errors = max_errors

    def close(self):
        pass

    def register(self, name, callable):
        self._registrations[name] = callable

    def run(self, timeout=None):
        qnames = ['toil:task:%s' % (name,) for name in self._registrations]
        while True:
            qname, task = self._redis.brpop(qnames)
            taskname = qname.split(':')[-1]
            task = json.loads(task)
            result = self._registrations[taskname](task['arg'])
            reply_to = task.get('reply_to')
            if reply_to:
                self._redis.lpush(reply_to, json.dumps({'result': result}))

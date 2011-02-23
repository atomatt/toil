from datetime import datetime
import logging
import random
import uuid
import couchdb
import traceback


log = logging.getLogger(__name__)


MAX_ERRORS = 5


class Client(object):

    def __init__(self, db):
        self._db = make_db(db)
        self.__since = 0

    def close(self):
        pass

    def fg(self, name, arg=None):
        reply_to = _reply_docid()
        task = {'_id': _task_docid(name), 'arg': arg, 'reply-to': reply_to}
        self._db.update([task])
        changes = self._db.changes(feed='longpoll', filter='toil/response',
                                  docid=reply_to, include_docs=True)
        response = changes['results'][0]['doc']
        if response:
            response['_deleted'] = True
            self._db.update([response])
            return response['result']

    def bg(self, name, arg=None):
        task = {'_id': _task_docid(name), 'arg': arg}
        self._db.update([task])


def _task_docid(name):
    return 'toil.task~%s~%s~%s' % (name, datetime.utcnow().isoformat(),
                              random.randint(0, 1000))


def _reply_docid():
    return 'toil.reply~%s' % (unicode(uuid.uuid4()),)


class Worker(object):

    max_errors = MAX_ERRORS

    def __init__(self, db, max_errors=None):
        self._db = make_db(db)
        self._registrations = {}
        self.client = Client(self._db)
        if max_errors is not None:
            self.max_errors = max_errors

    def close(self):
        pass

    def register(self, name, callable):
        self._registrations[name] = callable

    def run(self, timeout=None):
        # Use a 15sec heartbeat to keep the connection alive unless there's a
        # timeout (a heartbeat disables a timeout).
        heartbeat = 15000 if timeout is None else None
        for change in self._db.changes(feed='continuous',
                                       filter='toil/task',
                                       name=','.join(self._registrations),
                                       include_docs=True, timeout=timeout,
                                       heartbeat=heartbeat):
            if 'last_seq' in change:
                break
            task = change['doc']
            log.debug('claim?: %s', task['_id'])
            task['claimed'] = datetime.utcnow().isoformat()
            result = self._db.update([task])[0]
            if result[0]:
                log.debug('claimed: %s', task['_id'])
                func = self._registrations[task['_id'].split('~')[1]]
                try:
                    result = func(task['arg'])
                except Exception, e:
                    log.error(unicode(e))
                    now = datetime.utcnow().isoformat()
                    # Record error in task document.
                    errors = task.setdefault('errors', [])
                    errors.append({'time': now,
                                   'error': '%s: %s' % (e.__class__.__name__,
                                                        unicode(e)),
                                   'detail': traceback.format_exc()})
                    # Pause task if errors has reached maximum.
                    if len(errors) >= self.max_errors:
                        task['paused'] = now
                    # Remove claim on task.
                    del task['claimed']
                    self._db.update([task])
                else:
                    task['_deleted'] = True
                    if 'reply-to' in task:
                        reply_doc = {'_id': task['reply-to'], 'result': result}
                        r = self._db.update([task, reply_doc])
                        log.debug('reply: %s, %s', reply_doc['_id'], r[0][0])
                    else:
                        self._db.update([task])


def make_db(db):
    if isinstance(db, couchdb.Database):
        return db
    return couchdb.Database(db)

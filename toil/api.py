import urlparse


def client(uri):
    scheme, uri = _splituri(uri)
    return _client_factories[scheme](uri)


def worker(uri):
    scheme, uri = _splituri(uri)
    return _worker_factories[scheme](uri)


##
# Helpers.
#

def _splituri(uri):
    # A bug in urlsplit means we have to strip the fake scheme off manually.
    scheme, uri = uri.split(':', 1)
    uri = urlparse.urlsplit(uri)
    return scheme, uri


##
# CouchDB.
#

def _couchdb_client_factory(uri):
    from toil import backendcouchdb
    db = _couchdb_database(uri)
    return backendcouchdb.Client(db)


def _couchdb_worker_factory(uri):
    from toil import backendcouchdb
    db = _couchdb_database(uri)
    args = dict(urlparse.parse_qsl(uri.query))
    if 'max_errors' in args:
        args['max_errors'] = int(args['max_errors'])
    return backendcouchdb.Worker(db, **args)


def _couchdb_database(uri):
    import couchdb
    uri = urlparse.urlunsplit(['http', uri.netloc, uri.path, '', ''])
    return couchdb.Database(uri)


##
# Redis
#

def _redis_client_factory(uri):
    from toil import backendredis
    redis = _redis(uri)
    return backendredis.Client(redis)


def _redis_worker_factory(uri):
    from toil import backendredis
    redis = _redis(uri)
    return backendredis.Worker(redis)


def _redis(uri):
    import redis
    host, port = uri.netloc.split(':')
    return redis.Redis(host, int(port))


_client_factories = {'couchdb': _couchdb_client_factory,
                     'redis': _redis_client_factory}
_worker_factories = {'couchdb': _couchdb_worker_factory,
                     'redis': _redis_worker_factory}

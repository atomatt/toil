import urlparse


def client(uri):
    uri = urlparse.urlsplit(uri)
    return _client_factories[uri.scheme](uri)


def worker(uri):
    uri = urlparse.urlsplit(uri)
    return _worker_factories[uri.scheme](uri)


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
    return backendcouchdb.Worker(db)


def _couchdb_database(uri):
    import couchdb
    # Replace fake 'couchdb' scheme and remove query from path.
    uri = urlparse.urlunsplit(['http', uri.netloc,
                               urlparse.urlsplit(uri.path).path, '', ''])
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

import urlparse


def client(uri):
    uri = urlparse.urlsplit(uri)
    return _client_factories[uri.scheme](uri)


def worker(uri):
    uri = urlparse.urlsplit(uri)
    return _worker_factories[uri.scheme](uri)


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
    uri = ['http'] + list(uri[1:])
    return couchdb.Database(urlparse.urlunsplit(uri))


_client_factories = {'couchdb': _couchdb_client_factory}
_worker_factories = {'couchdb': _couchdb_worker_factory}

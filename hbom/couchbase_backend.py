# std-lib
import hashlib
import re

# 3rd-party (optional)
try:
    import couchbase  # noqa
except ImportError:
    couchbase = None

# internal modules
from . import model
from .pipeline import Pipeline
from .exceptions import OperationUnsupportedException, FieldError


__all__ = ['CouchbaseModel']


default_expire_time = 60


class CouchbasePipelineWrapper(object):
    __slots__ = ['invoker']

    def __init__(self, instance, pipe):
        allocator = pipe.allocate_response

        def invoker(command):
            def fn(*args, **kwargs):
                r, p = allocator(instance)
                getattr(p, command)(*args, **kwargs)
                return r
            return fn

        self.invoker = invoker

    def __getattr__(self, command):
        return self.invoker(command)


class CouchbaseConnectionMixin(object):

    @classmethod
    def db(cls):
        db = getattr(cls, '_db', None)
        if db is None:
            raise RuntimeError('no db object set on %s' % cls.__name__)
        else:
            return db

    @classmethod
    def db_pipeline(cls):
        return cls.db().pipeline()

    @classmethod
    def _ks(cls):
        """
        The internal keyspace used to namespace the data in redis.
        defaults to the class name.
        """
        return getattr(cls, '_keyspace', cls.__name__)

    @classmethod
    def db_key(cls, key):
        return '%s{%s}' % (cls._ks(), key)


class CouchbaseModel(model.BaseModel, CouchbaseConnectionMixin):

    def _backend(self, pipe=None):
        if pipe is None:
            return self.db()
        else:
            return CouchbasePipelineWrapper(instance=self, pipe=pipe)

    def _apply_changes(self, full=False, delete=False, pipe=None):
        state = self._calc_changes(full=full, delete=delete)
        if not state['changes']:
            return 0

        if pipe is None:
            pipe = Pipeline()
            do_commit = True
        else:
            do_commit = False

        backend = self._backend(pipe)
        cb_pk = self.db_key(self.primary_key())

        if state['add'] or state['remove']:
            backend.upsert(cb_pk, self.to_dict())

        if do_commit:
            pipe.execute()

        return state['changes']

    def prepare(self, pipe):
        fields = getattr(self, '_fields', None)
        if fields is None:
            pipe.hmgetall(self.db_key(self.primary_key()))
        else:
            pipe.hmget(self.db_key(self.primary_key()), *fields)
        return lambda data: self.load(data)

    @classmethod
    def get(cls, ids):
        # prepare the ids
        single = not isinstance(ids, (list, set))
        if single:
            ids = [ids]

        # Fetch data
        models = [cls.ref(i) for i in ids]
        Pipeline().hydrate(models)
        models = [model for model in models if model.exists()]

        if single:
            return models[0] if models else None
        return models

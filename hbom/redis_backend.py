# std-lib
from builtins import map
from builtins import range
from builtins import object
import hashlib
import redpipe
import redpipe.keyspaces
import redis.exceptions
from six import add_metaclass


# 3rd-party (optional)
try:
    import rediscluster  # noqa
except ImportError:
    rediscluster = None

try:
    import redis  # noqa
    from redis.exceptions import RedisError
except ImportError:
    redis = None

    class RedisError(Exception):
        pass

# internal modules
from .pipeline import Pipeline

__all__ = ['RedisContainer', 'RedisList', 'RedisIndex',
           'RedisString', 'RedisSet', 'RedisSortedSet', 'RedisHash',
           'RedisHashBinary', 'RedisDistributedHash', 'RedisObject',
           'RedisColdStorageObject']

EXPIRE_DEFAULT = 60
FREEZE_TTL_DEFAULT = 300

lua_restorenx = """
local key = KEYS[1]
local pttl = ARGV[1]
local data = ARGV[2]
local res = redis.call('exists', key)
if res == 0 then
    redis.call('restore', key, pttl, data)
    return 1
else
    return 0
end
"""

lua_object_info = """
local key = KEYS[1]
local subcommand = ARGV[1]
return redis.call('object', subcommand, key)
"""


def _parse_values(values):
    (_values,) = values if len(values) == 1 else (None,)
    if _values and isinstance(_values, list):
        return _values
    return values


class RedisContainerMeta(type):
    _base_classes = ['RedisContainer']

    def __new__(mcs, name, bases, d):
        if name in mcs._base_classes and d.get('__module__', '') == mcs.__module__:
            return type.__new__(mcs, name, bases, d)

        def get_core_property(field, default=None):
            prop = None
            try:
                prop = d[field]
            except KeyError:
                pass

            if prop is not None:
                return prop

            for base in bases:
                prop = getattr(base, field, None)
                if prop is not None:
                    return prop
            return default

        coretype = get_core_property('_core_type', redpipe.keyspaces.Keyspace)

        fields = get_core_property('_fields')
        memberparse = get_core_property('_memberparse', None)

        class Inner(coretype):
            keyspace_template = get_core_property('_key_tpl')
            keyspace = get_core_property('_keyspace', name)
            connection = get_core_property('_db')
            keyparse = get_core_property('_keyparse')
            valueparse = get_core_property('_valueparse')

        if fields:
            Inner.fields = fields

        if memberparse:
            Inner.memberparse = memberparse

        d['_core'] = Inner

        return type.__new__(mcs, name, bases, d)


@add_metaclass(RedisContainerMeta)
class RedisContainer(object):
    """
    Base class for all containers. This class should not
    be used and does not provide anything except the ``db``
    member.
    :members:
    db can be either pipeline or redis object
    """

    _db = None
    _core_type = redpipe.keyspaces.Keyspace
    _keyparse = redpipe.TextField
    _valueparse = redpipe.TextField
    _key_tpl = "%s{%s}"

    def __init__(self, key, pipe=None):
        self._key = key
        self.key = self.db_key(self._key)
        self._pipe = pipe
        self.core = self._core(pipe=pipe)

    @property
    def pipe(self):
        """
        Get a fresh pipeline() to be used in a `with` block.

        :return: Pipeline or NestedPipeline with autoexec set to true.
        """
        return redpipe.pipeline(self._pipe, name=self._db, autoexec=True)

    @classmethod
    def _ks(cls):
        """
        The internal keyspace used to namespace the data in redis.
        defaults to the class name.
        """
        return getattr(cls, '_keyspace', cls.__name__)

    @classmethod
    def db_key(cls, key):
        return cls._key_tpl % (cls._ks(), key)

    def primary_key(self):
        return self._key

    def delete(self):
        return self.core.delete(self._key)

    clear = delete

    def expire(self, time=None):
        if time is None:
            time = EXPIRE_DEFAULT
        return self.core.expire(self._key, time)

    set_expire = expire

    def exists(self):
        return self.core.exists(self._key)

    def eval(self, script, *args):
        return self.core.eval(script, 1, self._key, *args)

    def dump(self):
        return self.core.dump(self._key)

    def restore(self, data, pttl=0):
        return self.core.restorenx(self._key, data, pttl=pttl)

    def ttl(self):
        return self.core.ttl(self._key)

    def persist(self):
        return self.core.persist(self._key)

    def object(self, subcommand):
        return self.eval(lua_object_info, subcommand)

    @classmethod
    def scan(cls, cursor=0, match=None, count=None):
        return cls._core().scan(cursor=cursor, match=match, count=count)

    @classmethod
    def scan_iter(cls, match=None, count=None):
        return cls._core().scan_iter(match=match, count=count)

    @classmethod
    def ids(cls):
        for item in cls.scan_iter(count=100):
            yield item


class RedisString(RedisContainer):
    _core_type = redpipe.String

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    def get(self):
        return self.core.get(self._key)

    def set(self, value):
        return self.core.set(self._key, value)

    def incr(self):
        return self.core.incr(self._key)

    def incrby(self, value=1):
        return self.core.incrby(self._key, value)

    def incrbyfloat(self, value=1.0):
        return self.core.incrbyfloat(self._key, value)

    def setnx(self, value):
        return self.core.setnx(self._key, value)


class RedisSet(RedisContainer):
    """
    .. default-domain:: set

    This class represent a Set in redis.
    """
    _core_type = redpipe.Set

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    def sadd(self, *values):
        return self.core.sadd(self._key, *_parse_values(values))

    def srem(self, *values):
        return self.core.srem(self._key, *_parse_values(values))

    def spop(self):
        return self.core.spop(self._key)

    def all(self):
        return self.core.smembers(self._key)

    members = property(all)

    def scard(self):
        return self.core.scard(self._key)

    def sismember(self, value):
        return self.core.sismember(self._key, value)

    def srandmember(self):
        return self.core.srandmember(self._key)

    add = sadd
    pop = spop
    remove = srem


class RedisList(RedisContainer):
    """
    This class represent a list object as seen in redis.
    """
    _core_type = redpipe.List

    def all(self):
        return self.lrange(0, -1)

    members = property(all)
    """Return all items in the list."""

    def llen(self):
        return self.core.llen(self._key)

    def lrange(self, start, stop):
        return self.core.lrange(self._key, start, stop)

    def lpush(self, *values):
        return self.core.lpush(self._key, *_parse_values(values))

    def rpush(self, *values):
        return self.core.rpush(self._key, *_parse_values(values))

    def extend(self, iterable):
        self.rpush(*[e for e in iterable])

    def count(self, value):
        return self.members.count(value)

    def lpop(self):
        return self.core.lpop(self._key)

    def rpop(self):
        return self.core.rpop(self._key)

    def rpoplpush(self, key):
        return self.core.rpoplpush(self._key, key)

    def lrem(self, value, num=1):
        return self.core.lrem(self._key, value=value, num=num)

    def reverse(self):
        r = self.members[:]
        r.reverse()
        self.clear()
        self.extend(r)

    def ltrim(self, start, end):
        return self.core.ltrim(self._key, start, end)

    def lindex(self, idx):
        return self.core.lindex(self._key, idx)

    def lset(self, idx, value=0):
        return self.core.lset(self._key, idx, value)

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    # noinspection PyRedeclaration
    remove = lrem
    trim = ltrim
    shift = lpop
    unshift = lpush
    pop = rpop
    pop_onto = rpoplpush
    push = rpush
    append = rpush


class RedisSortedSet(RedisContainer):
    """
    This class represents a SortedSet in redis.
    Use it if you want to arrange your set in any order.

    """
    _core_type = redpipe.SortedSet

    @property
    def members(self):
        return self.zrange(0, -1)

    @property
    def revmembers(self):
        return self.zrevrange(0, -1)

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    def lt(self, v, limit=None, offset=None):
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("-inf", "(%f" % v, start=offset, num=limit)

    def le(self, v, limit=None, offset=None):
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("-inf", v, start=offset, num=limit)

    def gt(self, v, limit=None, offset=None, withscores=False):
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore(
            "(%f" % v, "+inf",
            start=offset,
            num=limit,
            withscores=withscores)

    def ge(self, v, limit=None, offset=None, withscores=False):
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore(
            "%f" % v, "+inf",
            start=offset,
            num=limit,
            withscores=withscores)

    def between(self, low, high, limit=None, offset=None):
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore(low, high, start=offset, num=limit)

    def zadd(self, members, score=1, nx=False, xx=False, ch=False, incr=False):
        return self.core.zadd(self._key, members, score=score, nx=nx, xx=xx, ch=ch, incr=incr)

    def zrem(self, *values):
        return self.core.zrem(self._key, *_parse_values(values))

    def zincrby(self, att, value=1):
        return self.core.zincrby(self._key, value=att, amount=value)

    def zrevrank(self, member):
        return self.core.zrevrank(self._key, member)

    def zrange(self, start, end, **kwargs):
        return self.core.zrange(self._key, start, end, **kwargs)

    def zrevrange(self, start, end, **kwargs):
        return self.core.zrevrange(self._key, start, end, **kwargs)

    # noinspection PyShadowingBuiltins
    def zrangebyscore(self, min, max, **kwargs):
        return self.core.zrangebyscore(self._key, min, max, **kwargs)

    # noinspection PyShadowingBuiltins
    def zrevrangebyscore(self, max, min, **kwargs):
        return self.core.zrevrangebyscore(self._key, max, min, **kwargs)

    def zcard(self):
        return self.core.zcard(self._key)

    def zscore(self, elem):
        return self.core.zscore(self._key, elem)

    def zremrangebyrank(self, start, stop):
        return self.core.zremrangebyrank(self._key, start, stop)

    def zremrangebyscore(self, min_value, max_value):
        return self.core.zremrangebyscore(self._key, min_value, max_value)

    def zrank(self, elem):
        return self.core.zrank(self._key, elem)

    def zcount(self, min, max):
        return self.core.zcount(self._key, min, max)

    def eq(self, value):
        return self.zrangebyscore(value, value)

    revrank = zrevrank
    score = zscore
    rank = zrank
    incr_by = zincrby
    add = zadd
    remove = zrem


class RedisHash(RedisContainer):
    _core_type = redpipe.Hash

    # @classmethod
    # def _core(cls, pipe=None):
    #
    #     class Inner(redpipe.Hash):
    #         keyspace = getattr(cls, '_keyspace', cls.__name__)
    #         connection = getattr(cls, '_db', None)
    #         valueparse = redpipe.TextField
    #
    #     return Inner(pipe=pipe)

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    def hlen(self):
        return self.core.hlen(self._key)

    def hset(self, member, value):
        return self.core.hset(self._key, member, value)

    def hsetnx(self, member, value):
        return self.core.hsetnx(self._key, member, value)

    def hdel(self, *members):
        return self.core.hdel(self._key, *_parse_values(members))

    def hkeys(self):
        return self.core.hkeys(self._key)

    def hgetall(self):
        return self.core.hgetall(self._key)

    def hvals(self):
        return self.core.hvals(self._key)

    def hget(self, field):
        return self.core.hget(self._key, field)

    def hexists(self, field):
        return self.core.hexists(self._key, field)

    def hincrby(self, field, increment=1):
        return self.core.hincrby(self._key, field, increment)

    def hmget(self, fields):
        return self.core.hmget(self._key, fields)

    def hmset(self, mapping):
        return self.core.hmset(self._key, mapping)


class RedisHashBinary(RedisHash):
    _valueparse = redpipe.BinaryField


class RedisDistributedHash(RedisContainer):
    _shards = 1000

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    def redis_sharded_key(self, member):
        return "%s:%s" % (
            self.key,
            int(hashlib.md5(member).hexdigest(), 16) % self._shards)
        # int(hashlib.md5(member.encode('utf-8')).hexdigest(), 16) % self._shards)

    def hlen(self):
        """
        Returns the number of elements in the Hash.
        """
        with self.pipe as p:
            data = [p.hlen("%s:%s" % (self.key, i)) for i in range(0, self._shards)]
            f = redpipe.Future()

            def cb():
                f.set(sum(data))

            p.on_execute(cb)
            return f

    def hset(self, member, value):
        """
        Set ``member`` in the Hash at ``value``.

        :param value:
        :param member:
        :returns: 1 if member is a new field and the value has been
                  stored, 0 if the field existed and the value has been
                  updated.

        > h = Hash("foo")
        > h.hset("bar", "value")
        1L
        > h.clear()
        """
        with self.pipe as p:
            return p.hset(self.redis_sharded_key(member), member, value)

    def hdel(self, *members):
        """
        Delete one or more hash field.

        :param members: on or more fields to remove.
        :return: the number of fields that were removed

        > h = Hash("foo")
        > h.hset("bar", "value")
        1L
        > h.hdel("bar")
        1
        > h.clear()
        """

        with self.pipe as p:
            data = [p.hdel(self.redis_sharded_key(member), member)
                    for member in _parse_values(members)]
            f = redpipe.Future()

            def cb():
                f.set(sum(data))

            p.on_execute(cb)
            return f

    def hget(self, field):
        """
        Returns the value stored in the field, None if the field doesn't exist.
        :param field:
        """
        with self.pipe as p:
            return p.hget(self.redis_sharded_key(field), field)

    def hexists(self, field):
        """
        Returns ``True`` if the field exists, ``False`` otherwise.
        :param field:
        """
        with self.pipe as p:
            return p.hexists(self.redis_sharded_key(field), field)

    def hincrby(self, field, increment=1):
        """
        Increment the value of the field.
        :param increment:
        :param field:
        :returns: the value of the field after incrementation

        > h = Hash("foo")
        > h.hincrby("bar", 10)
        10L
        > h.hincrby("bar", 2)
        12L
        > h.clear()
        """
        with self.pipe as p:
            return p.hincrby(self.redis_sharded_key(field), field, increment)


class RedisIndex(RedisHash):
    _key_tpl = "%s:%s:u:"

    @classmethod
    def shard(cls, key, pipe=None):
        shard_ct = cls.shard_count()
        keyhash = hashlib.md5(key.encode('utf-8')).hexdigest()
        return cls(int(keyhash, 16) % shard_ct, pipe=pipe)

    @classmethod
    def shard_count(cls):
        return getattr(cls, '_shard_count', 64)

    @classmethod
    def get(cls, key, pipe=None):
        return cls.shard(key, pipe=pipe).hget(key)

    @classmethod
    def mget(cls, keys, pipe=None):

        with redpipe.pipeline(pipe=pipe, autoexec=True) as p:
            f = redpipe.Future()
            mapping = {k: cls.shard(k, pipe=p).hget(k) for k in keys}

            def cb():
                f.set({k: v for k, v in mapping.items() if v.result})

            p.on_execute(cb)

            return f

    @classmethod
    def remove(cls, key, pipe=None):
        return cls.shard(key, pipe=pipe).hdel(key)

    @classmethod
    def setnx(cls, key, value, pipe=None):
        return cls.shard(key, pipe=pipe).hsetnx(key, value)

    @classmethod
    def set(cls, key, value, pipe=None):
        return cls.shard(key, pipe=pipe).hset(key, value)

    @classmethod
    def all(cls):
        shards = range(0, cls.shard_count() - 1)
        core = cls._core()
        for shard in shards:
            for k, v in core.hscan_iter(shard):
                yield k, v


class classproperty(object):
    def __init__(self, fget):
        self.fget = fget

    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)


class RedisObjectMeta(type):
    base_classes = ['RedisObject', 'RedisColdStorageObject']

    def __new__(mcs, name, bases, d):
        if name in mcs.base_classes and d.get('__module__', '') == mcs.__module__:
            return type.__new__(mcs, name, bases, d)

        def get_core_property(field, default=None):
            prop = None
            try:
                prop = d[field]
            except KeyError:
                pass

            if prop is not None:
                return prop

            for base in bases:
                prop = getattr(base, field, None)
                if prop is not None:
                    return prop
            return default

        definition = get_core_property('definition')

        fields = {k: v._parser for k, v in
                  getattr(definition, '_fields').items()}

        class inner(RedisHash):
            _db = get_core_property('_db')
            _keyspace = get_core_property('_keyspace', name)
            _valueparse = redpipe.BinaryField
            _fields = fields

        d['storage'] = inner

        return type.__new__(mcs, name, bases, d)


@add_metaclass(RedisObjectMeta)
class RedisObject(object):

    @classmethod
    def save(cls, instance, pipe=None, full=False):

        # we can save as long as the fields match.
        # this allows us to use wrapper classes that
        # implement the same interface.
        if getattr(instance, '_fields') != getattr(
                cls.definition, '_fields'):
            raise RuntimeError(
                'incorrect instance type for %s:save' % cls.__name__)

        state = instance.changes_(full=full)

        if not state:
            return 0
        p = Pipeline() if pipe is None else pipe
        _pk = instance.primary_key()
        s = cls.storage(_pk, pipe=p)

        # apply remove to the record
        remove = [k for k, v in state.items() if v is None]
        if remove:
            s.hdel(*remove)

        # apply add to the record
        add = {k: v for k, v in state.items() if v is not None}
        if add:
            s.hmset(add)

        p.on_execute(instance.persisted_)

        if not pipe:
            p.execute()

        return len(state)

    @classmethod
    def delete(cls, _pk, pipe=None):
        fields = getattr(cls.definition, '_fields')
        res = cls.storage(_pk, pipe=pipe).hdel(*fields)
        return res

    @classmethod
    def expire(cls, _pk, delay, pipe=None):
        return cls.storage(_pk, pipe=pipe).expire(delay)

    @classmethod
    def ttl(cls, _pk, pipe=None):
        return cls.storage(_pk, pipe=pipe).ttl()

    @classmethod
    def persist(cls, _pk, pipe=None):
        return cls.storage(_pk, pipe=pipe).persist()

    @classmethod
    def get_field(cls, _pk, field, pipe=None):
        return cls.storage(_pk, pipe=pipe).hget(field)

    @classmethod
    def set_field(cls, _pk, field, value, pipe=None):
        return cls.storage(_pk, pipe=pipe).hset(field, value)

    @classmethod
    def delete_field(cls, _pk, field, pipe=None):
        return cls.storage(_pk, pipe=pipe).hdel(field)

    @classmethod
    def incr_field(cls, _pk, field, incr=1, pipe=None):
        return cls.storage(_pk, pipe=pipe).hincrby(field, incr)

    @classmethod
    def ids(cls):
        return cls.storage.ids()

    @classmethod
    def new(cls, **kwargs):
        definition = cls.definition
        obj = definition(**kwargs)
        return obj

    @classmethod
    def get(cls, _pk, pipe=None):
        return cls.get_multi([_pk], pipe=pipe)[0]

    @classmethod
    def get_multi(cls, _pks, pipe=None):
        definition = cls.definition
        with Pipeline(pipe=pipe, autoexec=True) as p:
            storage = cls.storage
            fields = getattr(definition, '_fields')

            def prep(pk):
                ref = definition(_ref=pk, _parent=cls)
                s = storage(pk, pipe=p)
                r = s.hgetall()

                def set_data():
                    data = r.result
                    result = [data.get(k, None) for k in fields]
                    if any(v is not None for v in result):
                        ref.load_(result)
                    else:
                        setattr(ref, '_new', True)

                p.on_execute(set_data)
                return ref

            refs = [prep(pk) for pk in _pks]

            return refs

    @classmethod
    def ref(cls, pk, pipe=None):
        if pipe is not None:
            return cls.get(pk, pipe=pipe)

        return cls.definition(_ref=pk, _parent=cls)

    @classmethod
    def is_hot_key(cls, key):
        """
        override this method to define keys that should not ever go down
        into cold storage.
        Args:
            key:

        Returns:

        """
        return False

    @classmethod
    def prepare(cls, ref, pipe):
        _pk = ref.primary_key()
        definition = ref.__class__
        fields = getattr(definition, '_fields')
        s = cls.storage(_pk, pipe=pipe)
        r = s.hmget(list(fields.keys()))

        def set_data():
            if any(v is not None for v in r.result):
                ref.load_(r.result)
            else:
                setattr(ref, '_new', True)

        pipe.on_execute(set_data)


class RedisColdStorageObject(RedisObject):

    MYSQL_BLOB_LENGTH = 65535

    @classmethod
    def delete(cls, _pk, pipe=None):
        res = super(RedisColdStorageObject, cls).delete(_pk, pipe=pipe)
        # can we get away with not deleting from cold storage on hot key?
        cold_storage = cls.coldstorage
        cold_storage.delete(_pk)
        return res

    @classmethod
    def is_hot_key(cls, key):
        """
        override this method to define keys that should not ever go down
        into cold storage.
        Args:
            key:

        Returns:

        """
        return False

    @classmethod
    def _coldstorage_value_is_at_limit(cls, v):
        """
        This is to protect us against a bug we introduced when
        we used mysql blob field for cold storage and it silently truncated
        content longer than 65k. In those rare cases the content is
        corrupted and lost forever.
        """
        return len(v) >= cls.MYSQL_BLOB_LENGTH

    @classmethod
    def _load_from_cold_storage_dump(cls, k, v, pipe):
        storage = getattr(cls, 'storage')
        fields = getattr(cls.definition, '_fields')
        try:
            # if we use the pipe passed in, the try/catch does nothing.
            # but if the value is over the limit for mysql blob fields
            # we do it outside of the pipe.
            # This should be rare enough that we can take the perf hit.
            # and it will almost certainly be the case that the content
            # is corrupt anyway so we want to trap that exception in a
            # way that lets us handle it and remove the data from
            # cold storage.
            with Pipeline(autoexec=True,
                          pipe=None if cls._coldstorage_value_is_at_limit(
                              v) else pipe) as p:
                s = storage(k, pipe=p)
                s.persist()
                s.restore(v)
                return s.hmget(list(fields.keys()))
        except redis.exceptions.ResponseError as e:
            errstr = str(e)
            if 'ERR DUMP' not in errstr or 'checksum' not in errstr:
                raise
            # only do this if somehow the checksum is corrupt.
            response = redpipe.Future()
            response.set(None)
            return response

    @classmethod
    def _no_load_from_cold_storage_dump(cls, k, pipe):
        storage = getattr(cls, 'storage')
        s = storage(k)
        storage_name = getattr(storage, '_db')
        freeze_ttl = getattr(cls, 'freeze_ttl', FREEZE_TTL_DEFAULT)
        with Pipeline(name=storage_name, autoexec=True, pipe=pipe) as p:
            p.set('%s__xx' % s.key, '1')
            p.expire('%s__xx' % s.key, freeze_ttl - 1)

    @classmethod
    def get_multi(cls, _pks, pipe=None):
        storage = getattr(cls, 'storage')
        storage_name = getattr(storage, '_db')
        with Pipeline(pipe=pipe, name=storage_name, autoexec=True) as p:

            cold_storage = cls.coldstorage
            cold_keys = {pk for pk in _pks if not cls.is_hot_key(pk)}
            missing_cache = {}
            for pk in cold_keys:
                s = storage(pk, pipe=p)
                s.persist()
                missing_cache[pk] = p.exists("%s__xx" % s.key)

            refs = super(RedisColdStorageObject, cls).get_multi(_pks, pipe=p)

            def cb():
                for pk, ref in missing_cache.items():
                    if ref.result:
                        cold_keys.remove(pk)

                missing = {r.primary_key() for r in refs
                           if not r.exists() and r.primary_key() in cold_keys}
                found = {k: v for k, v in cold_storage.get_multi(missing).items()
                         if v is not None}

                with Pipeline(name=storage_name, autoexec=True) as pp:
                    found = {k: cls._load_from_cold_storage_dump(k, v, pipe=pp)
                             for k, v in found.items()}

                    for k in missing:
                        if k in found:
                            continue
                        cls._no_load_from_cold_storage_dump(k, pipe=pp)

                for ref in refs:
                    if ref.exists() or ref.primary_key() not in missing:
                        continue
                    try:
                        ref.load_(found[ref.primary_key()].result)
                    except KeyError:
                        setattr(ref, '_new', True)
                cold_storage.delete_multi(found.keys())

            p.on_execute(cb)
            return refs

    @classmethod
    def prepare(cls, ref, pipe):
        _pk = ref.primary_key()
        definition = ref.__class__
        fields = getattr(definition, '_fields')
        storage = getattr(cls, 'storage')
        s = storage(_pk, pipe=pipe)
        cold_storage = cls.coldstorage
        missing_cache = False
        frozen_key_cache = "%s__xx" % s.key
        if not cls.is_hot_key(_pk):
            s.persist()
            with s.pipe as pp:
                missing_cache = pp.exists(frozen_key_cache)

        r = s.hmget(list(fields.keys()))

        def set_data():
            if any(v is not None for v in r.result):
                ref.load_(r.result)
                return

            if cls.is_hot_key(_pk):
                return

            if missing_cache and missing_cache.result:
                return

            frozen = cold_storage.get(_pk)

            with Pipeline(name=getattr(storage, '_db')) as p:

                s = storage(_pk, pipe=p)

                if frozen is None:
                    freeze_ttl = getattr(cls, 'freeze_ttl', FREEZE_TTL_DEFAULT)
                    p.set(frozen_key_cache, '1')
                    p.expire(frozen_key_cache, freeze_ttl - 1)
                    p.execute()
                    return

                s.restore(frozen)
                rr = s.hmget(list(fields.keys()))
                p.on_execute(lambda: ref.load_(rr.result))
                p.execute()
                cold_storage.delete(_pk)

        pipe.on_execute(set_data)

    @classmethod
    def save(cls, instance, pipe=None, full=False):
        storage = getattr(cls, 'storage')
        with Pipeline(pipe=pipe, name=getattr(storage, '_db'), autoexec=True) as p:
            res = super(RedisColdStorageObject, cls).save(instance, pipe=p,
                                                          full=full)
            if res != 0:
                s = storage(instance.primary_key(), pipe=p)
                p.delete('%s__xx' % s.key)
            return res

    @classmethod
    def freeze(cls, *ids):
        cold_storage = cls.coldstorage

        ids = [k for k in ids if not cls.is_hot_key(k)]

        if not ids:
            return 0

        p = Pipeline()
        storage = getattr(cls, 'storage')

        freeze_ttl = getattr(cls, 'freeze_ttl', FREEZE_TTL_DEFAULT)

        def dump(k):
            s = storage(k, pipe=p)
            s.expire(freeze_ttl)
            return s.dump()

        results = {k: dump(k) for k in ids}
        try:
            p.execute()
            results = {k: res.result for k, res in results.items() if
                       res.result is not None}
            if results:
                cold_storage.set_multi(results)
        except (Exception, KeyboardInterrupt, SystemExit):
            # if anything at all goes wrong, make sure to clear the TTL on
            # any objects
            # we were trying to freeze
            p = Pipeline()
            map(lambda k: storage(k, pipe=p).persist(), ids)
            p.execute()
            raise

        return len(ids)

    @classmethod
    def thaw(cls, *ids):
        cold_storage = cls.coldstorage
        p = Pipeline()
        storage = getattr(cls, 'storage')
        for k, v in cold_storage.get_multi(ids).items():
            if v is None:
                continue
            s = storage(k, pipe=p)
            s.persist()
            s.restore(v)
        p.execute()
        cold_storage.delete_multi(ids)

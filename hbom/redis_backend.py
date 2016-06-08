# std-lib
import hashlib
import re

# 3rd-party (optional)
try:
    import rediscluster  # noqa
except ImportError:
    rediscluster = None

try:
    import redis  # noqa
except ImportError:
    redis = None

# internal modules
from . import model
from .pipeline import Pipeline
from .exceptions import OperationUnsupportedException, FieldError


__all__ = ['RedisModel', 'RedisContainer', 'RedisList', 'RedisIndex',
           'RedisString', 'RedisSet', 'RedisSortedSet', 'RedisHash',
           'RedisDistributedHash']


default_expire_time = 60


def _parse_values(values):
    (_values,) = values if len(values) == 1 else (None,)
    if _values and isinstance(_values, list):
        return _values
    return values


class RedisPipelineWrapper(object):
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


class RedisConnectionMixin(object):

    @classmethod
    def db(cls):
        db = getattr(cls, '_db', None)
        if db is None:
            raise RuntimeError('no db object set on %s' % cls.__name__)
        else:
            return db

    @classmethod
    def db_pipeline(cls):
        return cls.db().pipeline(transaction=False)

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


class RedisContainer(RedisConnectionMixin):
    """
    Base class for all containers. This class should not
    be used and does not provide anything except the ``db``
    member.
    :members:
    db can be either pipeline or redis object
    """

    def __init__(self, key, pipe=None):
        self._key = key
        self.pipeline = None
        if pipe is not None:
            self.attach(pipe)

    def primary_key(self):
        return self._key

    def attach(self, pipe):
        self.pipeline = RedisPipelineWrapper(instance=self, pipe=pipe)

    def detach(self):
        self.pipeline = None

    @property
    def key(self):
        return self.db_key(self._key)

    @property
    def _backend(self):
        if self.pipeline is None:
            return self.db()
        else:
            return self.pipeline

    def clear(self):
        """
        Remove the container from the redis storage
        > s = Set('test')
        > s.add('1')
        1
        > s.clear()
        > s.members
        set([])

        """
        return self._backend.delete(self.key)

    def set_expire(self, time=None):
        """
        Allow the key to expire after ``time`` seconds.

        > s = Set("test")
        > s.add("1")
        1
        > s.set_expire(1)
        > # from time import sleep
        > # sleep(1)
        > # s.members
        # set([])
        > s.clear()

        :param time: time expressed in seconds.
            If time is not specified,
            then ``default_expire_time`` will be used.
        :rtype: None
        """
        if time is None:
            time = default_expire_time

        return self._backend.expire(self.key, time)

    def exists(self):
        return self._backend.exists(self.key)

    def eval(self, script, *args):
        return self._backend.eval(script, 1, self.key, *args)

    @classmethod
    def ids(cls):
        if rediscluster and isinstance(cls.db(),
                                       rediscluster.StrictRedisCluster):
            conns = [
                redis.StrictRedis(host=node['host'], port=node['port'])
                for node in cls.db().connection_pool.nodes.nodes.values()
                if node.get('server_type', None) == 'master']
        elif redis:
            conns = [cls.db()]
        else:
            raise RuntimeError('redis package not imported')

        cursor = 0
        keyspace = cls._ks()
        redis_pattern = "%s{*}" % keyspace
        pattern = re.compile(r'^%s\{(.*)\}$' % keyspace)
        for conn in conns:
            while True:
                cursor, keys = conn.scan(
                    cursor=cursor,
                    match=redis_pattern,
                    count=500)
                for key in keys:
                    res = pattern.match(key)
                    if not res:
                        continue
                    yield res.group(1)
                if cursor == 0:
                    break


class RedisString(RedisContainer):
    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    def get(self):
        """
        set the value as a string in the key
        """
        return self._backend.get(self.key)

    def set(self, value):
        """
        set the value as a string in the key
        :param value:
        """
        return self._backend.set(self.key, value)

    def incr(self):
        """
        increment the value for key by 1
        """
        return self._backend.incr(self.key)

    def incrby(self, value=1):
        """
        increment the value for key by value: int
        :param value:
        """
        return self._backend.incrby(self.key, value)

    def incrbyfloat(self, value=1.0):
        """
        increment the value for key by value: float
        :param value:
        """
        return self._backend.incrbyfloat(self.key, value)


class RedisSet(RedisContainer):
    """
    .. default-domain:: set

    This class represent a Set in redis.
    """

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    def sadd(self, *values):
        """
        Add the specified members to the Set.

        :param values: a list of values or a simple value.
        :rtype: integer representing the number of value added to the set.

        > s = Set("test")
        > s.clear()
        > s.add(["1", "2", "3"])
        3
        > s.add(["4"])
        1
        > print s
        <Set 'test' set(['1', '3', '2', '4'])>
        > s.clear()

        """
        return self._backend.sadd(self.key, *_parse_values(values))

    def srem(self, *values):
        """
        Remove the values from the Set if they are present.

        :param values: a list of values or a simple value.
        :rtype: boolean indicating if the values have been removed.

        > s = Set("test")
        > s.add(["1", "2", "3"])
        3
        > s.srem(["1", "3"])
        2
        > s.clear()

        """
        return self._backend.srem(self.key, *_parse_values(values))

    def spop(self):
        """
        Remove and return (pop) a random element from the Set.

        :rtype: String representing the value poped.

        > s = Set("test")
        > s.add("1")
        1
        > s.spop()
        '1'
        > s.members
        set([])

        """
        return self._backend.spop(self.key)

    def all(self):
        return self._backend.smembers(self.key)

    members = property(all)
    """
    return the real content of the Set.
    """

    def __iter__(self):
        if self.pipeline:
            raise OperationUnsupportedException()
        return self.members.__iter__()

    def scard(self):
        """
        Returns the cardinality of the Set.

        :rtype: String containing the cardinality.

        """
        return self._backend.scard(self.key)

    def sismember(self, value):
        """
        Return ``True`` if the provided value is in the ``Set``.
        :param value:

        """
        return self._backend.sismember(self.key, value)

    def srandmember(self):
        """
        Return a random member of the set.

        > s = Set("test")
        > s.add(['a', 'b', 'c'])
        3
        > s.srandmember() # doctest: +ELLIPSIS
        '...'
        > # 'a', 'b' or 'c'
        """
        return self._backend.srandmember(self.key)

    add = sadd
    """see sadd"""
    pop = spop
    """see spop"""
    remove = srem
    """see srem"""
    __contains__ = sismember
    __len__ = scard


class RedisList(RedisContainer):
    """
    This class represent a list object as seen in redis.
    """

    def all(self):
        """
        Returns all items in the list.
        """
        return self.lrange(0, -1)

    members = property(all)
    """Return all items in the list."""

    def llen(self):
        """
        Returns the length of the list.
        """
        return self._backend.llen(self.key)

    __len__ = llen

    def __getitem__(self, index):
        if isinstance(index, int):
            return self.lindex(index)
        elif isinstance(index, slice):
            indices = index.indices(len(self))
            return self.lrange(indices[0], indices[1] - 1)
        else:
            raise TypeError

    def __setitem__(self, index, value):
        self.lset(index, value)

    def lrange(self, start, stop):
        """
        Returns a range of items.

        :param start: integer representing the start index of the range
        :param stop: integer representing the size of the list.

        > l = List("test")
        > l.push(['a', 'b', 'c', 'd'])
        4L
        > l.lrange(1, 2)
        ['b', 'c']
        > l.clear()

        """
        return self._backend.lrange(self.key, start, stop)

    def lpush(self, *values):
        """
        Push the value into the list from the *left* side

        :param values: a list of values or single value to push
        :rtype: long representing the number of values pushed.

        > l = List("test")
        > l.lpush(['a', 'b'])
        2L
        > l.clear()
        """
        return self._backend.lpush(self.key, *_parse_values(values))

    def rpush(self, *values):
        """
        Push the value into the list from the *right* side

        :param values: a list of values or single value to push
        :rtype: long representing the size of the list.

        > l = List("test")
        > l.lpush(['a', 'b'])
        2L
        > l.rpush(['c', 'd'])
        4L
        > l.members
        ['b', 'a', 'c', 'd']
        > l.clear()
        """
        return self._backend.rpush(self.key, *_parse_values(values))

    def extend(self, iterable):
        """
        Extend list by appending elements from the iterable.

        :param iterable: an iterable objects.
        """
        self.rpush(*[e for e in iterable])

    def count(self, value):
        """
        Return number of occurrences of value.

        :param value: a value tha *may* be contained in the list
        """
        return self.members.count(value)

    def lpop(self):
        """
        Pop the first object from the left.

        :return: the popped value.

        """
        return self._backend.lpop(self.key)

    def rpop(self):
        """
        Pop the first object from the right.

        :return: the popped value.
        """
        return self._backend.rpop(self.key)

    def rpoplpush(self, key):
        """
        Remove an element from the list,
        atomically add it to the head of the list indicated by key

        :param key: the key of the list receiving the popped value.
        :return: the popped (and pushed) value

        > l = List('list1')
        > l.push(['a', 'b', 'c'])
        3L
        > l.rpoplpush('list2')
        'c'
        > l2 = List('list2')
        > l2.members
        ['c']
        > l.clear()
        > l2.clear()
        """
        return self._backend.rpoplpush(self.key, key)

    def lrem(self, value, num=1):
        """
        Remove first occurrence of value.
        :param num:
        :param value:
        :return: 1 if the value has been removed, 0 otherwise
        if you see an error here, did you use redis.StrictRedis()?
        """
        return self._backend.lrem(self.key, num, value)

    def reverse(self):
        """
        Reverse the list in place.

        :return: None
        """
        r = self[:]
        r.reverse()
        self.clear()
        self.extend(r)

    def ltrim(self, start, end):
        """
        Trim the list from start to end.

        :param start:
        :param end:
        :return: None
        """
        return self._backend.ltrim(self.key, start, end)

    def lindex(self, idx):
        """
        Return the value at the index *idx*

        :param idx: the index to fetch the value.
        :return: the value or None if out of range.
        """
        return self._backend.lindex(self.key, idx)

    def lset(self, idx, value=0):
        """
        Set the value in the list at index *idx*

        :param value:
        :param idx:
        :return: True is the operation succeed.

        > l = List('test')
        > l.push(['a', 'b', 'c'])
        3L
        > l.lset(0, 'e')
        True
        > l.members
        ['e', 'b', 'c']
        > l.clear()

        """
        return self._backend.lset(self.key, idx, value)

    def __iter__(self):
        return self.members.__iter__()

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    # noinspection PyRedeclaration
    __len__ = llen
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

    def __getitem__(self, index):
        if isinstance(index, slice):
            return self.zrange(index.start, index.stop)
        else:
            return self.zrange(index, index)[0]

    def __contains__(self, val):
        return self.zscore(val) is not None

    @property
    def members(self):
        """
        Returns the members of the set.
        """
        return self.zrange(0, -1)

    @property
    def revmembers(self):
        """
        Returns the members of the set in reverse.
        """
        return self.zrevrange(0, -1)

    def __iter__(self):
        return self.members.__iter__()

    def __reversed__(self):
        return self.revmembers.__iter__()

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    @property
    def _min_score(self):
        """
        Returns the minimum score in the SortedSet.
        """
        try:
            return self.zscore(self.__getitem__(0))
        except IndexError:
            return None

    @property
    def _max_score(self):
        """
        Returns the maximum score in the SortedSet.
        """
        try:
            self.zscore(self.__getitem__(-1))
        except IndexError:
            return None

    def lt(self, v, limit=None, offset=None):
        """
        Returns the list of the members of the set that have scores
        less than v.

        :param v: the score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements
        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("-inf", "(%f" % v, start=offset, num=limit)

    def le(self, v, limit=None, offset=None):
        """
        Returns the list of the members of the set that have scores
        less than or equal to v.

        :param v: the score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements

        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore("-inf", v, start=offset, num=limit)

    def gt(self, v, limit=None, offset=None, withscores=False):
        """Returns the list of the members of the set that have scores
        greater than v.
        :param withscores:
        :param offset:
        :param limit:
        :param v:
        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore(
            "(%f" % v, "+inf",
            start=offset,
            num=limit,
            withscores=withscores)

    def ge(self, v, limit=None, offset=None, withscores=False):
        """Returns the list of the members of the set that have scores
        greater than or equal to v.

        :param withscores:
        :param v: the score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements

        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore(
            "%f" % v, "+inf",
            start=offset,
            num=limit,
            withscores=withscores)

    def between(self, low, high, limit=None, offset=None):
        """
        Returns the list of the members of the set that have scores
        between min and max.

        .. Note::
            The min and max are inclusive when comparing the values.

        :param low: the minimum score to compare to.
        :param high: the maximum score to compare to.
        :param limit: limit the result to *n* elements
        :param offset: Skip the first *n* elements

        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.add('c', 30)
        1
        > s.between(20, 30)
        ['b', 'c']
        > s.clear()
        """
        if limit is not None and offset is None:
            offset = 0
        return self.zrangebyscore(low, high, start=offset, num=limit)

    def zadd(self, members, score=1):
        """
        Add members in the set and assign them the score.

        :param members: a list of item or a single item
        :param score: the score the assign to the item(s)

        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.clear()
        """
        _members = []
        if not isinstance(members, dict):
            _members = [score, members]
        else:
            for member, score in members.items():
                _members += [score, member]

        return self._backend.zadd(self.key, *_members)

    def zrem(self, *values):
        """
        Remove the values from the SortedSet

        :param values:
        :return: True if **at least one** value is successfully
                 removed, False otherwise

        > s = SortedSet('foo')
        > s.add('a', 10)
        1
        > s.zrem('a')
        1
        > s.members
        []
        > s.clear()
        """
        return self._backend.zrem(self.key, *_parse_values(values))

    def zincrby(self, att, value=1):
        """
        Increment the score of the item by ``value``

        :param att: the member to increment
        :param value: the value to add to the current score
        :returns: the new score of the member

        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.zincrby("a", 10)
        20.0
        > s.clear()
        """
        return self._backend.zincrby(self.key, att, value)

    def zrevrank(self, member):
        """
        Returns the ranking in reverse order for the member

        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.revrank('a')
        1
        > s.clear()
        :param member:
        """
        return self._backend.zrevrank(self.key, member)

    def zrange(self, start, end, **kwargs):
        """
        Returns all the elements including between ``start`` (non included) and
        ``stop`` (included).

        :param kwargs:
        :param end:
        :param start:
        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.add('c', 30)
        1
        > s.zrange(1, 3)
        ['b', 'c']
        > s.zrange(1, 3, withscores=True)
        [('b', 20.0), ('c', 30.0)]
        > s.clear()
        """
        return self._backend.zrange(self.key, start, end, **kwargs)

    def zrevrange(self, start, end, **kwargs):
        """
        Returns the range of items included between ``start`` and ``stop``
        in reverse order (from high to low)

        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.add('c', 30)
        1
        > s.zrevrange(1, 2)
        ['b', 'a']
        > s.clear()
        :param kwargs:
        :param kwargs:
        :param end:
        :param start:
        :param start:
        """
        return self._backend.zrevrange(self.key, start, end, **kwargs)

    # noinspection PyShadowingBuiltins
    def zrangebyscore(self, min, max, **kwargs):
        """
        Returns the range of elements included between the scores (min and max)

        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.add('c', 30)
        1
        > s.zrangebyscore(20, 30)
        ['b', 'c']
        > s.clear()
        :param min: int
        :param max: int
        :param kwargs: dict
        """
        return self._backend.zrangebyscore(self.key, min, max, **kwargs)

    # noinspection PyShadowingBuiltins
    def zrevrangebyscore(self, max, min, **kwargs):
        """
        Returns the range of elements included between the scores (min and max)

        > s = SortedSet("foo")
        > s.add('a', 10)
        1
        > s.add('b', 20)
        1
        > s.add('c', 30)
        1
        > s.zrangebyscore(20, 20)
        ['b']
        > s.clear()
        :param kwargs:
        :param min:
        :param max:
        """
        return self._backend.zrevrangebyscore(self.key, max, min, **kwargs)

    def zcard(self):
        """
        Returns the cardinality of the SortedSet.

        > s = SortedSet("foo")
        > s.add("a", 1)
        1
        > s.add("b", 2)
        1
        > s.add("c", 3)
        1
        > s.zcard()
        3
        > s.clear()
        """
        return self._backend.zcard(self.key)

    def zscore(self, elem):
        """
        Return the score of an element

        > s = SortedSet("foo")
        > s.add("a", 10)
        1
        > s.score("a")
        10.0
        > s.clear()
        :param elem:
        """
        return self._backend.zscore(self.key, elem)

    def zremrangebyrank(self, start, stop):
        """
        Remove a range of element between the rank ``start`` and
        ``stop`` both included.

        :param stop:
        :param start:
        :return: the number of item deleted

        > s = SortedSet("foo")
        > s.add("a", 10)
        1
        > s.add("b", 20)
        1
        > s.add("c", 30)
        1
        > s.zremrangebyrank(1, 2)
        2
        > s.members
        ['a']
        > s.clear()
        """
        return self._backend.zremrangebyrank(self.key, start, stop)

    def zremrangebyscore(self, min_value, max_value):
        """
        Remove a range of element by between score ``min_value`` and
        ``max_value`` both included.

        :param max_value:
        :param min_value:
        :returns: the number of items deleted.

        > s = SortedSet("foo")
        > s.add("a", 10)
        1
        > s.add("b", 20)
        1
        > s.add("c", 30)
        1
        > s.zremrangebyscore(10, 20)
        2
        > s.members
        ['c']
        > s.clear()
        """
        return self._backend.zremrangebyscore(self.key, min_value, max_value)

    def zrank(self, elem):
        """
        Returns the rank of the element.

        > s = SortedSet("foo")
        > s.add("a", 10)
        1
        > s.zrank("a")
        0
        > s.clear()
        :param elem:
        """
        return self._backend.zrank(self.key, elem)

    def eq(self, value):
        """
        Returns the elements that have ``value`` for score.
        :param value:
        """
        return self.zrangebyscore(value, value)

    def __len__(self):
        return self.zcard()

    revrank = zrevrank
    score = zscore
    rank = zrank
    incr_by = zincrby
    add = zadd
    remove = zrem


class RedisHash(RedisContainer):
    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    def hlen(self):
        """
        Returns the number of elements in the Hash.
        """
        return self._backend.hlen(self.key)

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
        return self._backend.hset(self.key, member, value)

    def hsetnx(self, member, value):
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
        return self._backend.hsetnx(self.key, member, value)

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
        return self._backend.hdel(self.key, *_parse_values(members))

    def hkeys(self):
        """
        Returns all fields name in the Hash
        """
        return self._backend.hkeys(self.key)

    def hgetall(self):
        """
        Returns all the fields and values in the Hash.

        :rtype: dict
        """
        return self._backend.hgetall(self.key)

    def hvals(self):
        """
        Returns all the values in the Hash

        :rtype: list
        """
        return self._backend.hvals(self.key)

    def hget(self, field):
        """
        Returns the value stored in the field, None if the field doesn't exist.
        :param field:
        """
        return self._backend.hget(self.key, field)

    def hexists(self, field):
        """
        Returns ``True`` if the field exists, ``False`` otherwise.
        :param field:
        """
        return self._backend.hexists(self.key, field)

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
        return self._backend.hincrby(self.key, field, increment)

    def hmget(self, fields):
        """
        Returns the values stored in the fields.
        :param fields:
        """
        return self._backend.hmget(self.key, fields)

    def hmset(self, mapping):
        """
        Sets or updates the fields with their corresponding values.

        :param mapping: a dict with keys and values
        """
        return self._backend.hmset(self.key, mapping)

    __getitem__ = hget
    __setitem__ = hset
    __delitem__ = hdel
    __len__ = hlen
    __contains__ = hexists


class RedisDistributedHash(RedisContainer):
    _shards = 1000

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.key)

    def redis_sharded_key(self, member):
        return "%s:%s" % (
            self.key,
            long(hashlib.md5(member).hexdigest(), 16) % self._shards)

    def hlen(self):
        """
        Returns the number of elements in the Hash.
        """
        if self.pipeline:
            raise OperationUnsupportedException()
        else:
            return sum([self._backend.hlen("%s:%s" % (self.key, i))
                        for i in range(0, self._shards)])

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
        return self._backend.hset(
            self.redis_sharded_key(member), member, value)

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
        if self.pipeline:
            raise OperationUnsupportedException()
        else:
            return sum(
                [self._backend.hdel(self.redis_sharded_key(member), member)
                    for member in _parse_values(members)])

    def hget(self, field):
        """
        Returns the value stored in the field, None if the field doesn't exist.
        :param field:
        """
        return self._backend.hget(self.redis_sharded_key(field), field)

    def hexists(self, field):
        """
        Returns ``True`` if the field exists, ``False`` otherwise.
        :param field:
        """
        return self._backend.hexists(self.redis_sharded_key(field), field)

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
        return self._backend.hincrby(
            self.redis_sharded_key(field), field, increment)


class RedisIndex(RedisHash):

    @classmethod
    def db_key(cls, shard):
        return getattr(cls, '_key_tpl', cls.__name__ + ":%s:u") % shard

    @classmethod
    def shard(cls, key, pipe=None):
        shard_ct = getattr(cls, '_shard_count', 64)
        keyhash = hashlib.md5(key).hexdigest()
        return cls(long(keyhash, 16) % shard_ct, pipe=pipe)

    @classmethod
    def get(cls, key, pipe=None):
        return cls.shard(key, pipe=pipe).hget(key)

    @classmethod
    def mget(cls, keys, pipe=None):
        p = Pipeline() if pipe is None else pipe
        mapping = {k: cls.shard(k, pipe=p).hget(k) for k in keys}
        if pipe is not None:
            return mapping
        p.execute()
        return {k: v.data for k, v in mapping.items() if v.data}

    @classmethod
    def remove(cls, key, pipe=None):
        return cls.shard(key, pipe=pipe).hdel(key)

    @classmethod
    def setnx(cls, key, value, pipe=None):
        return cls.shard(key, pipe=pipe).hsetnx(key, value)

    @classmethod
    def set(cls, key, value, pipe=None):
        return cls.shard(key, pipe=pipe).hset(key, value)


class RedisModel(model.BaseModel, RedisConnectionMixin):

    def _backend(self, pipe=None):
        if pipe is None:
            return self.db()
        else:
            return RedisPipelineWrapper(instance=self, pipe=pipe)

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
        redis_pk = self.db_key(self.primary_key())

        # apply add to the record
        if state['add']:
            backend.hmset(redis_pk, state['add'])

        # apply remove to the record
        if state['remove']:
            backend.hdel(redis_pk, *state['remove'])

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
        models = [model for model in models if model]

        if single:
            return models[0] if models else None
        return models

    @classmethod
    def ids(cls):
        if rediscluster and \
                isinstance(cls.db(), rediscluster.StrictRedisCluster):
            conns = [redis.StrictRedis(host=node['host'], port=node['port'])
                     for node in cls.db().connection_pool.nodes.nodes.values()
                     if node.get('server_type', None) == 'master']
        else:
            conns = [cls.db()]
        cursor = 0
        keyspace = cls._ks()
        redis_pattern = "%s{*}" % keyspace
        pattern = re.compile(r'^%s\{(.*)\}$' % keyspace)
        for conn in conns:
            while True:
                cursor, keys = conn.scan(
                    cursor=cursor,
                    match=redis_pattern,
                    count=500)
                for key in keys:
                    res = pattern.match(key)
                    if not res:
                        continue
                    yield res.group(1)
                if cursor == 0:
                    break

    @classmethod
    def patch(cls, _pk, pipe=None, **kwargs):
        p = Pipeline() if pipe is None else pipe
        backend = RedisPipelineWrapper(instance=cls.ref(_pk), pipe=p)
        redis_pk = cls.db_key(_pk)

        if not _pk:
            raise FieldError("Missing primary key value")

        add = {}
        rem = []

        fields = getattr(cls, '_fields')

        for k, v in kwargs.items():
            col = fields[k]

            # if the value is empty flag the field to be deleted
            # otherwise, we write the data.
            if v is None:
                rem.append(k)
            else:
                persist = col.to_persistence
                _v = persist(v)
                if _v is None:
                    rem.append(k)
                else:
                    add[k] = _v

        if add:
            backend.hmset(redis_pk, add)

        # apply remove to the record
        if rem:
            backend.hdel(redis_pk, *rem)

        if pipe is None:
            p.execute()

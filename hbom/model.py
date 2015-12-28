import json
from .exceptions import FieldError
from .fields import Field
try:
    # noinspection PyPackageRequirements
    import rediscluster
except ImportError:
    rediscluster = None


__all__ = ['BaseModel']


class AbstractError(RuntimeError):
    pass


class _BaseMeta(type):
    def __new__(mcs, name, bases, d):
        if name in ['BaseModel', 'RedisModel']:
            return type.__new__(mcs, name, bases, d)

        d['_required'] = required = set()
        d['_fields'] = fields = {}
        d['_pkey'] = None

        # load all fields from any base classes to allow for validation
        odict = {}

        for ocls in reversed(bases):
            f = getattr(ocls, '_fields', None)
            if f is not None:
                odict.update(f)
        odict.update(d)

        d = odict

        # validate all of our fields to ensure that they fulfill our
        # expectations
        for attr, col in d.iteritems():
            if isinstance(col, Field):
                col.attr = attr
                col.model = name
                fields[attr] = col
                if getattr(col, '_required', False):
                    required.add(attr)
                if getattr(col, '_primary', False):
                    if d['_pkey']:
                        raise FieldError(
                            "One primary field allowed, you have: %s %s" % (
                                attr, d['_pkey'])
                        )
                    d['_pkey'] = attr

        if not d['_pkey']:
            raise FieldError('No primary field specified in %s' % name)

        model = type.__new__(mcs, name, bases, d)
        return model


class BaseModel(object):
    """
    This is the base class for all models. You subclass from this base Model
    in order to create a model with fields. As an example::

        class User(Model):
            email_address = StringField(required=True)
            salt = StringField(default='')
            hash = StringField(default='')
            created_at = FloatField(default=time.time)

    Which can then be used like::

        user = User(email_address='user@domain.com')
        user.save()
        user = User.get(email_address='user@domain.com')
        user = User.get(5)
        users = User.get([2, 6, 1, 7])

    """
    __metaclass__ = _BaseMeta

    def __init__(self, **kwargs):

        self._new = not kwargs.pop('_loading', False)
        self._data = {}
        self._init = False
        self._loaded = False
        self._last = {}
        ref = kwargs.pop('_ref', False)
        if ref:
            attr = getattr(self.__class__, '_pkey')
            setattr(self, attr, ref)
            self._new = False
            self._pre_load()
            return

        for attr in getattr(self, '_fields'):
            setattr(self, attr, kwargs.get(attr, None))

        if not self._new:
            self._last = self.to_dict()

        self._init = True
        self._loaded = True

    def load(self, data):
        if isinstance(data, list):
            if data.count(None) == len(data):
                data = None
            else:
                fields = getattr(self, '_fields')
                data = {field: data[i] for i, field in enumerate(fields)}
        if data:
            self.__init__(_loading=True, **data)
        self._loaded = True
        self._post_load()

    def _pre_load(self):
        pass

    def _post_load(self):
        pass

    def primary_key(self):
        return getattr(self, getattr(self, '_pkey'))

    def initialized(self):
        return self._init

    def loaded(self):
        return self._loaded

    def exists(self):
        return True if self._last else False

    @classmethod
    def ref(cls, primary_key, pipe=None):
        obj = cls(_ref=primary_key)
        if pipe is not None:
            pipe.attach(obj)
        return obj

    @property
    def _pk(self):
        return '%s:%s' % (self.__class__.__name__,
                          getattr(self, getattr(self, '_pkey')))

    def _apply_changes(self, old, new, pipe=None):
        raise AbstractError("extend the class to implement persistence")

    @classmethod
    def _calc_changes(cls, old, new):
        """
        figure out which fields have changed.
        """

        pk = old.get(getattr(cls, '_pkey')) or new.get(getattr(cls, '_pkey'))
        if not pk:
            raise FieldError("Missing primary key value")

        response = {}
        response['changes'] = changes = 0
        response['primary_key'] = pk
        response['add'] = add = {}
        response['remove'] = rem = []

        # first figure out what data needs to be persisted
        fields = getattr(cls, '_fields')
        for attr in fields:
            col = fields[attr]

            # get old and new values for this field
            ov = old.get(attr)
            nv = new.get(attr)

            # not doing a full write, and old and new values are the same
            if ov == nv:
                continue

            # looks like there are some changes.
            changes += 1

            # if the new value is empty, just flag the field to be deleted
            # otherwise, we write the data.
            if nv is not None:
                persist = getattr(col, '_to_persistence')
                _v = persist(nv)
                if _v is None:
                    rem.append(attr)
                else:
                    add[attr] = _v
            elif ov is not None:
                rem.append(attr)

        response['changes'] = changes
        return response

    def to_dict(self):
        """
        Returns a copy of all data assigned to fields in this entity. Useful
        for returning items to JSON-enabled APIs. If you want to copy an
        entity, you should look at the ``.copy()`` method.
        """
        return dict(self._data)

    def save(self, full=False, pipe=None):
        """
        Saves the current entity to Redis. Will only save changed data by
        default, but you can force a full save by passing ``full=True``.
        :param pipe:
        :param full:
        """
        new = self.to_dict()
        last = self._last
        if full or self._new:
            last = {}
        ret = self._apply_changes(last, new, pipe=pipe)
        self._new = False
        self._last = new
        return ret

    def delete(self, pipe=None):
        """
        Deletes the entity immediately.
        :param pipe:
        """
        self._apply_changes(self._last, {}, pipe=pipe)

    def copy(self):
        """
        Creates a shallow copy of the given entity (any entities that can be
        retrieved from a OneToMany relationship will not be copied).
        """
        x = self.to_dict()
        x.pop(getattr(self, '_pkey'))
        return self.__class__(**x)

    @classmethod
    def get(cls, ids):
        """
        Will fetch one or more entities of this type from the persistent store.

        Used like::

            MyModel.get(5)
            MyModel.get([1, 6, 2, 4])
            MyModel.get(email='testuser1@yahoo.com')
            MyModel.get(email=['testuser1@yahoo.com', 'testuser2@gmail.com'])

        Passing a list or a tuple will return multiple entities, in the same
        order that the ids were passed.
        :param ids:
        """
        raise AbstractError("extend the class to implement persistence")

    def __str__(self):
        return "<%s:%s>" % (self.__class__.__name__, self.primary_key())

    def __repr__(self):
        return json.dumps({self.__class__.__name__: self._data})

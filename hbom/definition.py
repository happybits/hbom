import json
from .exceptions import FieldError
from .fields import Field

__all__ = ['Definition']


class DefinitionMeta(type):
    def __new__(mcs, name, bases, d):
        d['_fields'] = fields = {}
        d['_pkey'] = None
        d['__slots__'] = {'_new', '_init', '_data', '_dirty'}

        if name in ['Definition', 'RedisModel']:
            return type.__new__(mcs, name, bases, d)

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
                if col.primary:
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


class Definition(object):
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
    __metaclass__ = DefinitionMeta

    def __init__(self, **kwargs):

        self._new = not kwargs.pop('_loading', False)
        self._init = False
        self._dirty = set()
        self._data = {}

        ref = kwargs.pop('_ref', False)
        if ref:
            attr = getattr(self.__class__, '_pkey')
            setattr(self, attr, ref)
            self._new = False
            return

        for attr in self._fields:
            setattr(self, attr, kwargs.get(attr, None))

        if not self._new:
            self._dirty = set()

        self._init = True

    def load(self, data):
        if isinstance(data, list):
            if data.count(None) == len(data):
                data = None
            else:
                data = {field: data[i] for i, field in enumerate(self._fields)}
        if data:
            self.__init__(_loading=True, **data)

    def primary_key(self):
        return getattr(self, getattr(self.__class__, '_pkey'))

    def exists(self):
        return True if self._data and self._init and not self._new else False

    @property
    def _pk(self):
        return '%s:%s' % (self.__class__.__name__,
                          getattr(self, getattr(self, '_pkey')))

    def changes_(self, full=False, delete=False):
        """
        figure out which fields have changed.
        """
        cls = self.__class__
        data = self._data
        pk = data.get(getattr(cls, '_pkey'))
        if not pk:
            raise FieldError("Missing primary key value")

        response = {}

        # first figure out what data needs to be persisted
        fields = getattr(cls, '_fields')

        for attr in fields.keys() if full or delete else self._dirty:
            col = fields[attr]

            # get old and new values for this field
            nv = data.get(attr)

            # if the new value is empty, just flag the field to be deleted
            # otherwise, we write the data.
            if delete or nv is None:
                response[attr] = None
            else:
                response[attr] = col.to_persistence(nv)

        return response

    def persisted_(self):
        self._new = False
        self._dirty = set()

    def to_dict(self):
        """
        Returns a copy of all data assigned to fields in this entity. Useful
        for returning items to JSON-enabled APIs.
        """
        return dict(self._data)

    @property
    def __dict__(self):
        return dict(self._data)

    def __str__(self):
        return "<%s:%s>" % (self.__class__.__name__, self.primary_key())

    def __repr__(self):
        return json.dumps({self.__class__.__name__: self._data})

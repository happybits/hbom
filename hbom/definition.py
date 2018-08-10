import json
from .exceptions import FieldError, MissingField
from .fields import Field
from future.utils import with_metaclass

__all__ = ['Definition']


class DefinitionMeta(type):
    def __new__(mcs, name, bases, d):
        d['_fields'] = fields = {}
        d['_pkey'] = None
        d['__slots__'] = {'_new', '_init', '_data', '_dirty', '_parent'}

        if name in ['Definition']:
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
        for attr, col in d.items():
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


class Definition(with_metaclass(DefinitionMeta, object)):
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

    def __init__(self, **kwargs):

        self._new = not kwargs.pop('_loading', False)
        self._init = False
        self._dirty = set()
        self._data = {}
        self._parent = kwargs.pop('_parent', None)
        ref = kwargs.pop('_ref', False)

        if ref:
            setattr(self, self.__class__._pkey, ref)
            self._new = False
            return

        for attr in self._fields:
            setattr(self, attr, kwargs.get(attr, None))

        if not self._new:
            self._dirty = set()

        self._init = True

    def load_(self, data):
        if isinstance(data, list):
            if data.count(None) == len(data):
                data = None
            else:
                data = {field: data[i] if data[i] is None
                        else self._fields[field].from_persistence(data[i])
                        for i, field in enumerate(self._fields)}
        if data:
            self.__init__(_loading=True, **data)

    def primary_key(self):
        return getattr(self, self.__class__._pkey)

    def attach(self, pipe):
        if self._parent is None:
            return

        if self._init or self.exists():
            return

        self._parent.prepare(self, pipe=pipe)

    def exists(self):
        return True if self._data and self._init and not self._new else False

    def changes_(self, full=False, delete=False):
        """
        figure out which fields have changed.
        """
        cls = self.__class__
        data = self._data
        pk = data.get(cls._pkey)
        if not pk:
            raise FieldError("Missing primary key value")

        response = {}

        # first figure out what data needs to be persisted
        fields = cls._fields

        for attr in fields.keys():
            col = fields[attr]

            # get old and new values for this field
            nv = data.get(attr)

            if not delete:
                # don't allow save if missing a required field
                if col.required and nv is None:
                    raise MissingField(
                        "%s.%s cannot be missing" % (cls.__name__, attr)
                    )
                col.validate(nv)

            # if the field is not dirty, no need to include in change set
            # unless we are deleting the record or passed the full flag.
            if not full and not delete and attr not in self._dirty:
                continue

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

    def __iter__(self):
        """
        supports coercing the object into a dictionary.
        Returns: generator
        """
        for k, v in self._data.items():
            yield k, v

    @property
    def __dict__(self):
        return dict(self._data)

    def __str__(self):
        return "<%s:%s>" % (self.__class__.__name__, self.primary_key())

    def __repr__(self):
        return json.dumps({self.__class__.__name__: self._data})

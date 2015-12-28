import json
from decimal import Decimal
from .exceptions import *  # noqa

__all__ = '''
Field
IntegerField
FloatField
DecimalField
StringField
StringListField
TextField
JsonField
ListField
BooleanField
'''.split()

_NUMERIC = (0, 0.0, Decimal('0'))

NULL = object()

_SCALAR = [str, unicode]


class Field(object):
    """
    Field objects handle data conversion to/from strings, store metadata
    about indices, etc. Note that these are "heavy" fields, in that whenever
    data is read/written, it must go through descriptor processing. This is
    primarily so that (for example) if you try to write a Decimal to a Float
    field, you get an error the moment you try to do it, not some time later
    when you try to save the object (though saving can still cause an error
    during the conversion process).

    Standard Arguments:

        * *required* - determines whether this field is required on
          creation
        * *default* - a default value (either a callable or a simple value)
          when this field is not provided
        * *unique* - can only be enabled on ``String`` fields, allows for
          required distinct field values (like an email address on a User
          model)

    Notes:

        * Fields with 'unique' set to True can only be string fields
        * You can only have one unique field on any model
        * If you set required to True, then you must have the field set
          during object construction: ``MyModel(col=val)``
    """
    _allowed = ()

    __slots__ = '_primary _required _default _init _model _attr'.split()

    def __init__(self, required=False, default=NULL, primary=False):
        self._primary = primary
        self._required = required
        self._default = default
        self._init = False
        self._model = None
        self._attr = None

        if primary:
            if not any(isinstance(i, self._allowed) for i in _NUMERIC):
                if self._allowed not in (str, unicode):
                    raise FieldError(
                        "this field type cannot be primary"
                    )

    def _from_persistence(self, value):
        convert = self._allowed[0] if \
            isinstance(self._allowed, (tuple, list)) else self._allowed
        return convert(value)

    def _to_persistence(self, value):
        if isinstance(value, long):
            return str(value)
        return repr(value)

    def _validate(self, value):
        if value is not None:
            if isinstance(value, self._allowed):
                return
        elif not self._required:
            return
        raise InvalidFieldValue("%s.%s has type %r but must be of type %r" % (
            self._model, self._attr, type(value), self._allowed))

    def _init_(self, obj, model, attr, value, loading):
        # You shouldn't be calling this directly, but this is what sets up all
        # of the necessary pieces when creating an entity from scratch, or
        # loading the entity from Redis
        self._model = model
        self._attr = attr

        if value is None:
            default = self._default
            if default is NULL:
                if self._required:
                    raise MissingField(
                        "%s.%s cannot be missing" % (model, attr)
                    )
            elif callable(default):
                # noinspection PyCallingNonCallable
                value = default()
            else:
                value = self._default
        elif not isinstance(value, self._allowed):
            try:
                value = self._from_persistence(value)
            except (ValueError, TypeError) as e:
                raise InvalidFieldValue(*e.args)

        if not loading:
            self._validate(value)
        getattr(obj, '_data')[attr] = value

    def __set__(self, obj, value):
        initialized = getattr(obj, '_init', False)
        if not initialized:
            self._init_(obj, *value)
            return

        if self._primary:
            raise InvalidOperation("Cannot update primary key value")

        if value is None:
            return self.__delete__(obj)

        try:
            value = self._from_persistence(value)
        except (ValueError, TypeError):
            raise InvalidFieldValue(
                "Cannot convert %r into type %s" % (value, self._allowed)
            )
        self._validate(value)
        getattr(obj, '_data')[self._attr] = value

    def __get__(self, obj, _):
        try:
            return getattr(obj, '_data')[self._attr]
        except KeyError:
            AttributeError("%s.%s does not exist" % (self._model, self._attr))

    def __delete__(self, obj):
        if self._required:
            raise InvalidOperation(
                "%s.%s cannot be null" % (self._model, self._attr)
            )
        try:
            getattr(obj, '_data').pop(self._attr)
        except KeyError:
            raise AttributeError(
                "%s.%s does not exist" % (self._model, self._attr)
            )


class BooleanField(Field):
    """
    Used for boolean fields.

    All standard arguments supported.

    All values passed in on creation are casted via bool(), with the exception
    of None (which behaves as though the value was missing), and any existing
    data in persistence layer is considered ``False`` if empty, and ``True``
    otherwise.

    Used via::

        class MyModel(Model):
            col = Boolean()

    Queries via ``MyModel.get_by(...)`` and ``MyModel.query.filter(...)`` work
    as expected when passed ``True`` or ``False``.

    .. note: these fields are not sortable by default.
    """
    _allowed = bool

    def _to_persistence(self, obj):
        return '1' if obj else None

    def _from_persistence(self, obj):
        return bool(obj)


class DecimalField(Field):
    """
    A Decimal-only numeric field (converts ints/longs into Decimals
    automatically). Attempts to assign Python float will fail.

    All standard arguments supported.

    Used via::

        class MyModel(Model):
            col = Decimal()
    """
    _allowed = Decimal

    def _from_persistence(self, value):
        return Decimal(value)

    def _to_persistence(self, value):
        return str(value)


class FloatField(Field):
    """
    Numeric field that supports integers and floats (values are turned into
    floats on load from persistence).

    All standard arguments supported.

    Used via::

        class MyModel(Model):
            col = Float()
    """
    _allowed = (float, int, long)


class IntegerField(Field):
    """
    Used for integer numeric fields.

    All standard arguments supported.

    Used via::

        class MyModel(Model):
            col = Integer()
    """
    _allowed = (int, long)


class JsonField(Field):
    """
    Allows for more complicated nested structures as attributes.

    Used via::

        class MyModel(Model):
            col = Json()
    """
    _allowed = (dict, list, tuple, set)

    def _to_persistence(self, value):
        return json.dumps(value)

    def _from_persistence(self, value):
        if isinstance(value, self._allowed):
            return value
        return json.loads(value)


class ListField(JsonField):
    _allowed = list

    def _from_persistence(self, value):
        if isinstance(value, self._allowed):
            return value
        try:
            return json.loads(value)
        except (ValueError, TypeError) as e:
            if isinstance(value, str) and len(value) > 0:
                return value.split(',')
            raise InvalidFieldValue(*e.args)


class StringListField(Field):
    _allowed = list

    def _from_persistence(self, value):
        if isinstance(value, self._allowed):
            return value
        try:
            return json.loads(value)
        except (ValueError, TypeError) as e:
            if isinstance(value, str) and len(value) > 0:
                return value.split(',')
            raise InvalidFieldValue(*e.args)

    def _to_persistence(self, value):
        return ",".join(value)


class StringField(Field):
    """
    A plain string field. Trying to save unicode strings will probably result
    in an error, if not bad data.

    All standard arguments supported.

    Used via::

        class MyModel(Model):
            col = String()
    """
    _allowed = str

    def _to_persistence(self, value):
        return value


class TextField(Field):
    """
    A unicode string field.

    All standard arguments supported, except for ``unique``.

    Aside from not supporting ``unique`` indices, will generally have the same
    behavior as a ``String`` field, only supporting unicode strings. Data is
    encoded via utf-8 before writing to persistence. If you would like to
    create your own field to encode/decode differently, examine the source
    find out how to do it.

    Used via::

        class MyModel(Model):
            col = Text()
    """
    _allowed = unicode

    def _to_persistence(self, value):
        return value.encode('utf-8')

    def _from_persistence(self, value):
        if isinstance(value, str):
            return value.decode('utf-8')
        return value

from .compat import json
from .exceptions import InvalidFieldValue, \
    MissingField, InvalidOperation
import future.builtins
import redpipe
import six

__all__ = '''
Field
IntegerField
FloatField
StringField
StringListField
TextField
DictField
ListField
BooleanField
'''.split()


NULL = object()

_SCALAR = (str, six.text_type, future.builtins.str)


class Field(object):
    """
    Field objects handle data conversion to/from strings, store metadata
    about indices, etc. Note that these are "heavy" fields, in that whenever
    data is read/written, it must go through descriptor processing. This is
    primarily so that (for example) if you try to write a Dictionary to a Float
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
    _parser = redpipe.TextField

    __slots__ = 'primary required default model convert attr'.split()

    def __init__(self, required=False, default=NULL, primary=False):
        self.primary = primary
        self.required = required
        self.default = default
        self.model = None
        self.attr = None

    @property
    def _allowed_types(self):
        return self._allowed if isinstance(self._allowed, (tuple, list)) else [self._allowed]

    def _is_allowed(self, value):
        if value is None:
            return True

        for a in self._allowed_types:
            if isinstance(value, a):
                return True

        try:
            self._parser.encode(value)
            return True
        except redpipe.InvalidValue:
            return False

    def validate(self, value):
        if value is None:
            if self.required:
                raise InvalidFieldValue('%s.%s is required' %
                                        (self.model, self.attr))
            return

        if self._is_allowed(value):
            return

        raise InvalidFieldValue("%s.%s has type %r but must be of type %r" % (
            self.model, self.attr, type(value), self._allowed_types))

    def _init_(self, obj, value, loading):
        # You shouldn't be calling this directly, but this is what sets up all
        # of the necessary pieces when creating an entity from scratch, or
        # loading the entity from persistence layer.
        model = self.model
        attr = self.attr
        if value is None:
            default = self.default
            if default is NULL:
                if self.required:
                    raise MissingField(
                        "%s.%s cannot be missing" % (model, attr)
                    )
            elif callable(default):
                # noinspection PyCallingNonCallable
                value = default()
            else:
                value = self.default
        elif not self._is_allowed(value):
            raise InvalidFieldValue('value invalid')

        if not loading:
            self.validate(value)
            if value is not None:
                obj._dirty.add(attr)

        if isinstance(value, (dict, list)):
            value = json.loads(json.dumps(value))

        obj._data[attr] = value

    def __set__(self, obj, value):
        initialized = getattr(obj, '_init', False)

        if not initialized:
            loading = not getattr(obj, '_new', False)
            self._init_(obj, value, loading)
            return

        if self.primary:
            raise InvalidOperation("Cannot update primary key value")

        if value is None:
            return self.__delete__(obj)

        self.validate(value)
        data = obj._data
        attr = self.attr
        if data.get(attr, None) != value:
            obj._dirty.add(attr)
        data[attr] = value

    def __get__(self, obj, _):
        try:
            return obj._data[self.attr]
        except KeyError:
            AttributeError("%s.%s does not exist" % (self.model, self.attr))

    def __delete__(self, obj):
        attr = self.attr
        if self.required:
            raise InvalidOperation(
                "%s.%s cannot be null" % (self.model, attr)
            )
        obj._data[attr] = None
        obj._dirty.add(attr)


class BooleanField(Field):
    _allowed = bool
    _parser = redpipe.BooleanField

    def __init__(self):
        super(BooleanField, self).__init__(default=False)


class FloatField(Field):
    _allowed = (float, int)
    _parser = redpipe.FloatField


class IntegerField(Field):
    _allowed = int
    _parser = redpipe.IntegerField


class DictField(Field):
    _allowed = (dict, list, tuple, set)
    _parser = redpipe.DictField


class ListField(Field):
    _allowed = list
    _parser = redpipe.ListField


class StringListField(ListField):
    _allowed = list
    _parser = redpipe.StringListField

    def __set__(self, obj, value):
        if value is not None:
            try:
                value[:] = [v for v in value if v is not None]
            except TypeError:
                raise InvalidFieldValue('invalid list')

            if not value:
                value = None
        return super(StringListField, self).__set__(obj, value)


class StringField(Field):
    _allowed = _SCALAR
    _parser = redpipe.AsciiField


class TextField(Field):
    _allowed = [six.text_type, str]
    _parser = redpipe.TextField

import threading
import sys
from json.encoder import JSONEncoder
from functools import wraps


__all__ = ['Pipeline']


class Pipeline(object):

    __slots__ = ['pipes', 'refs', 'callbacks']

    def __init__(self):
        self.pipes = {}
        self.refs = {}
        self.callbacks = []

    def attach(self, model, force=False):
        """
        pass in a model object that hasn't been hydrated yet.
        We set up a pipeline callback handler that will read the data from the
        database and populate it into the object.
        the call will be pipelined on hydrate or execute.
        :param force:
        :param model:
        """
        if not force and getattr(model, '_init', False):
            return False

        parent = getattr(model, '_parent', None)
        if parent is not None:
            parent.prepare(model, pipe=self)
            return True

        prepare = getattr(model, 'prepare', None)
        if prepare is None:
            return False

        pipe, refs = self._pipe_refs(model)
        refs.append(prepare(pipe))

        return True

    def hydrate(self, models, force=False):
        if not isinstance(models, list):
            models = [models]
        if any([self.attach(model, force=force) for model in models]):
            self.execute()
            return True
        return False

    def on_execute(self, callback):
        self.callbacks.append(callback)

    def execute(self):
        callbacks = self.callbacks
        self.callbacks = []
        pipes = self.pipes
        self.pipes = {}
        refs = self.refs
        self.refs = {}
        # only need to use threads if we have more than one connection
        if len(pipes) > 1:
            threads = []
            # kick off all the threads
            for conn_id, pipe in pipes.items():
                t = ExecThread(pipe, refs[conn_id])
                t.start()
                threads.append(t)

            # wait for all the threads to finish executing
            for t in threads:
                t.join()

            # did any of them have problems?
            # if so raise the first one you find.
            for t in threads:
                if t.exc_info:
                    raise t.exc_info[0], t.exc_info[1], t.exc_info[2]
        else:
            # only one connection, no threads needed.
            # keep it simple.
            for conn_id, pipe in pipes.items():
                for i, result in enumerate(pipe.execute()):
                    refs[conn_id][i](result)

        for callback in callbacks:
            callback()

    def allocate_callback(self, instance, callback):
        pipe, refs = self._pipe_refs(instance)
        refs.append(callback)
        return pipe

    def allocate_response(self, instance):
        response = PipelineResponse()

        def set_data(data):
            response.set(data)

        pipe = self.allocate_callback(instance, set_data)
        return response, pipe

    def _pipe_refs(self, instance):
        conn = instance.db()

        conn_id = id(conn)
        try:
            return self.pipes[conn_id], self.refs[conn_id]
        except KeyError:
            pipe = self.pipes[conn_id] = instance.db_pipeline()
            refs = self.refs[conn_id] = []
            return pipe, refs

    def __getattr__(self, command):
        def fn(*args, **kwargs):
            db_args = [a for a in args]
            if command in ['eval', 'object']:
                ref = args[2]
                db_args[2] = ref.db_key(ref.primary_key())
            else:
                ref = args[0]
                db_args[0] = ref.db_key(ref.primary_key())
            response, pipe = self.allocate_response(ref)
            getattr(pipe, command)(*db_args, **kwargs)
            return response
        return fn


class ExecThread(threading.Thread):
    def __init__(self, pipe, refs):
        threading.Thread.__init__(self)
        self.pipe = pipe
        self.refs = refs
        self.exc_info = None

    def run(self):
        # noinspection PyBroadException
        try:
            for i, result in enumerate(self.pipe.execute()):
                self.refs[i](result)
        except Exception:
            self.exc_info = sys.exc_info()


def IS(instance, other):  # noqa
    """
    Support the `future is other` use-case.
    Can't override the language so we built a function.
    Will work on non-future objects too.

    :param instance: future or any python object
    :param other: object to compare.
    :return:
    """
    try:
        instance = instance._hbom_pipeline_result  # noqa
    except AttributeError:
        pass

    try:
        other = other._hbom_pipeline_result
    except AttributeError:
        pass

    return instance is other


def ISINSTANCE(instance, A_tuple):  # noqa
    """
    Allows you to do isinstance checks on futures.
    Really, I discourage this because duck-typing is usually better.
    But this can provide you with a way to use isinstance with futures.
    Works with other objects too.

    :param instance:
    :param A_tuple:
    :return:
    """
    try:
        instance = instance._hbom_pipeline_result
    except AttributeError:
        pass

    return isinstance(instance, A_tuple)


class PipelineResponse(object):
    """
    An object returned from all our Pipeline calls.
    """
    __slots__ = ['_result']

    def __init__(self, data=None):
        self._result = data

    def set(self, data):
        """
        :param data: any python object
        :return: None
        """
        self._result = data

    @property
    def result(self):
        """
        Get the underlying result.
        Usually one of the data types returned by redis-py.

        :return: None, str, int, list, set, dict
        """
        try:
            return self._result
        except AttributeError:
            return None

    data = result

    def IS(self, other):
        """
        Allows you to do identity comparisons on the underlying object.

        :param other: Mixed
        :return: bool
        """
        return self.result is other

    def isinstance(self, other):
        """
        allows you to check the instance type of the underlying result.

        :param other:
        :return:
        """
        return isinstance(self.result, other)

    def id(self):
        """
        Get the object id of the underlying result.
        """
        return id(self.result)

    def __repr__(self):
        """
        Magic method in python used to override the behavor of repr(future)

        :return: str
        """
        return repr(self.result)

    def __str__(self):
        """
        Magic method in python used to override the behavor of str(future)

        :return:
        """
        return str(self.result)

    def __lt__(self, other):
        """
        Magic method in python used to override the behavor of future < other

        :param other: Any python object, usually numeric
        :return: bool
        """
        return self.result < other

    def __le__(self, other):
        """
        Magic method in python used to override the behavor of future <= other


        :param other: Any python object, usually numeric
        :return: bool
        """
        return self.result <= other

    def __gt__(self, other):
        """
        Magic method in python used to override the behavor of future > other

        :param other: Any python object, usually numeric
        :return: bool
        """
        return self.result > other

    def __ge__(self, other):
        """
        Magic method in python used to override the behavor of future >= other

        :param other: Any python object, usually numeric
        :return: bool
        """
        return self.result >= other

    def __hash__(self):
        """
        Magic method in python used to override the behavor of hash(future)

        :return: int
        """
        return hash(self.result)

    def __eq__(self, other):
        """
        Magic method in python used to override the behavor of future == other

        :param other: Any python object
        :return: bool
        """
        return self.result == other

    def __ne__(self, other):
        """
        Magic method in python used to override the behavor of future != other

        :param other: Any python object
        :return: bool
        """
        return self.result != other

    def __nonzero__(self):
        """
        Magic method in python used to override the behavor of bool(future)

        :return: bool
        """
        return bool(self.result)

    def __bytes__(self):
        """
        Magic method in python used to coerce object: bytes(future)

        :return: bytes
        """
        return bytes(self.result)

    def __bool__(self):
        """
        Magic method in python used to coerce object: bool(future)

        :return: bool
        """
        return bool(self.result)

    def __call__(self, *args, **kwargs):
        """
        Magic method in python used to invoke a future:
        future(*args, **kwargs)

        :param args: tuple
        :param kwargs: dict
        :return: Unknown, defined by object
        """
        return self.result(*args, **kwargs)

    def __len__(self):
        """
        Magic method in python used to determine length: len(future)

        :return: int
        """
        return len(self.result)

    def __iter__(self):
        """
        Magic method in python to support iteration.
        Example:

        .. code-block:: python

            future = Future()
            future.set([1, 2, 3])
            for row in future:
                print(row)

        :return: iterable generator
        """
        for item in self.result:
            yield item

    def __contains__(self, item):
        """
        Magic python method supporting: `item in future`

        :param item: any python object
        :return: bool
        """
        return item in self.result

    def __reversed__(self):
        """
        Magic python method to emulate: reversed(future)

        :return: list
        """
        return reversed(self.result)

    def __getitem__(self, item):
        """
        Used to emulate dictionary access of an element: future[key]

        :param item: usually str, key name of dict.

        :return: element, type unknown
        """
        return self.result[item]

    def __int__(self):
        """
        Magic method in python to coerce to int:  int(future)

        :return:
        """
        return int(self.result)

    def __float__(self):
        """
        Magic method in python to coerce to float: float(future)

        :return: float
        """
        return float(self.result)

    def __round__(self, ndigits=0):
        """
        Magic method in python to round: round(future, 1)

        :param ndigits: int
        :return: float, int
        """
        return round(self.result, ndigits=ndigits)

    def __add__(self, other):
        """
        support addition:  result = future + 1

        :param other: int, float, str, list

        :return: int, float, str, list
        """
        return self.result + other

    def __sub__(self, other):
        """
        support subtraction: result = future - 1

        :param other: int, float, str, list
        :return: int, float, str, list
        """
        return self.result - other

    def __mul__(self, other):
        """
        support multiplication: result = future * 2

        :param other: int, float, str, list
        :return: int, float, str, list
        """
        return self.result * other

    def __mod__(self, other):
        """
        support modulo: result = future % 2

        :param other: int, float, str, list
        :return: int, float, str, list
        """
        return self.result % other

    def __div__(self, other):
        """
        support division: result = future / 2
        for python 2

        :param other: int, float
        :return: int, float
        """
        return self.result / other

    def __truediv__(self, other):
        """
        support division: result = future / 2
        for python 3

        :param other: int, float
        :return: int, float
        """
        return self.result / other

    def __floordiv__(self, other):
        """
        support floor division: result = future // 2

        :param other: int, float
        :return: int, float
        """
        return self.result // other

    def __pow__(self, power, modulo=None):
        """
        supports raising to a power: result = pow(future, 3)

        :param power: int
        :param modulo:
        :return: int, float
        """
        return pow(self.result, power, modulo)

    def __lshift__(self, other):
        """
        bitwise operation: result = future << other
        """
        return self.result << other

    def __rshift__(self, other):
        """
        bitwise operation: result = future >> other
        """
        return self.result >> other

    def __and__(self, other):
        """
        bitwise operation: result = future & other
        """
        return self.result & other

    def __xor__(self, other):
        """
        bitwise operation: result = future ^ other
        """
        return self.result ^ other

    def __or__(self, other):
        """
        bitwise operation: result = future | other
        """
        return self.result | other

    def __radd__(self, other):
        """
        addition operation: result = other + future
        """
        return other + self.result

    def __rsub__(self, other):
        """
        subtraction operation: result = other - future
        """
        return other - self.result

    def __rmul__(self, other):
        """
        multiplication operation: result = other * future
        """
        return self.result * other

    def __rmod__(self, other):
        """
        use as modulo: result = other * future
        """
        return other % self.result

    def __rdiv__(self, other):
        """
        use as divisor: result = other / future

        python 2
        """
        return other / self.result

    def __rtruediv__(self, other):
        """
        use as divisor: result = other / future

        python 3
        """
        return other / self.result

    def __rfloordiv__(self, other):
        """
        floor divisor: result other // future
        """
        return other // self.result

    def __rpow__(self, other):
        """
        reverse power: other ** future
        """
        return other ** self.result

    def __rlshift__(self, other):
        """
        result = other << future
        """
        return other << self.result

    def __rrshift__(self, other):
        """
        result = other >> future
        """
        return other >> self.result

    def __rand__(self, other):
        """
        result = other & future
        """
        return other & self.result

    def __rxor__(self, other):
        """
        result = other ^ future
        """
        return other ^ self.result

    def __ror__(self, other):
        """
        result = other | future
        """
        return other | self.result

    def __getattr__(self, name, default=None):
        """
        access an attribute of the future:  future.some_attribute
        or getattr(future, name, default)

        :param name: attribute name
        :param default: a value to be used if no attribute is found
        :return:
        """
        if name[0] == '_':
            raise AttributeError(name)

        return getattr(self.result, name, default)

    def __getstate__(self):
        """
        used for getting object state to serialize when pickling
        :return: object
        """
        return self.result

    def __setstate__(self, state):
        """
        used for restoring object state when pickling
        :param state: object
        :return: None
        """
        self._result = state

    # this helps with duck-typing.
    # when grabbing the property for json encoder,
    # we can look for this unique attribute which is an alias for result
    # and we can be reasonably sure it is not accidentally grabbing
    # some other type of object.
    _hbom_pipeline_result = result


def _json_default_encoder(func):
    """
    Monkey-Patch the core json encoder library.
    This isn't as bad as it sounds.
    We override the default method so that if an object
    falls through and can't be encoded normally, we see if it is
    a Future object and return the result to be encoded.

    I set a special attribute on the Future object so I can tell
    that's what it is, and can grab the result.

    If that doesn't work, I fall back to the earlier behavior.
    The nice thing about patching the library this way is that it
    won't inerfere with existing code and it can itself be wrapped
    by other methods.

    So it's very extensible.

    :param func: the JSONEncoder.default method.
    :return: an object that can be json serialized.
    """
    @wraps(func)
    def inner(self, o):
        try:
            return o._hbom_pipeline_result  # noqa
        except AttributeError:
            pass
        return func(self, o)

    return inner


JSONEncoder.default = _json_default_encoder(JSONEncoder.default)

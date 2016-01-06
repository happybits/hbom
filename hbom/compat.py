# try to use ujson
try:
    import ujson as json  # noqa
except ImportError:
    import json  # noqa

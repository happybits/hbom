import redis

__all__ = ['default_connection', 'set_default_connection']

CONNECTION = redis.StrictRedis()


def default_connection():
    return CONNECTION


def set_default_connection(r):
    global CONNECTION
    CONNECTION = r

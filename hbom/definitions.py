from recordclass import recordclass

__all__ = ['Definition']

def Definition(typename, field_names):
    cls = recordclass(typename, field_names)
    cls.__new__.__defaults__ = (None,) * (len(cls._fields) - 1)
    return cls

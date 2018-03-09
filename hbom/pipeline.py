from redpipe import pipeline as Pipeline

__all__ = ['Pipeline', 'hydrate']


def hydrate(objects, pipe=None):
    with Pipeline(pipe=pipe, autoexec=True) as p:
        for o in objects:
            o.attach(p)

import threading
import sys

__all__ = ['Pipeline']


class Pipeline(object):
    def __init__(self):
        self.pipes = {}
        self.refs = {}

    def attach(self, model, force=False):
        """
        pass in a model object that hasn't been hydrated yet.
        We set up a pipeline callback handler that will read the data from the
        database and populate it into the object.
        the redis call will be pipelined on hydrate or execute.
        :param force:
        :param model:
        """
        if not force and model.loaded():
            return False

        pipe, refs = self._pipe_refs(model.db())
        refs.append(model.prepare(pipe))
        return True

    def hydrate(self, models, force=False):
        if not isinstance(models, list):
            models = [models]
        if any([self.attach(model, force=force) for model in models]):
            self.execute()
            return True
        return False

    def execute(self):
        # only need to use threads if we have more than one redis connection
        if len(self.pipes) > 1:
            threads = []
            # kick off all the threads
            for conn_id, pipe in self.pipes.items():
                t = ExecThread(pipe, self.refs[conn_id])
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
            # only one redis connection, no threads needed.
            # keep it simple.
            for conn_id, pipe in self.pipes.items():
                for i, result in enumerate(pipe.execute()):
                    self.refs[conn_id][i](result)

    def __getattr__(self, command):
        def fn(*args, **kwargs):
            redis_args = [a for a in args]
            if command == 'eval':
                ref = args[2]
                redis_args[2] = ref.redis_key(ref.primary_key())
            else:
                ref = args[0]
                redis_args[0] = ref.redis_key(ref.primary_key())
            response = PipelineResponse(ref.primary_key())
            pipe, refs = self._pipe_refs(ref.db())
            getattr(pipe, command)(*redis_args, **kwargs)

            def set_data(data):
                response.data = data

            refs.append(set_data)
            return response

        return fn

    def _pipe_refs(self, conn):
        conn_id = id(conn)
        try:
            return self.pipes[conn_id], self.refs[conn_id]
        except KeyError:
            pipe = self.pipes[conn_id] = conn.pipeline(transaction=False)
            refs = self.refs[conn_id] = []
            return pipe, refs


class PipelineResponse(object):
    def __init__(self, key, data=None):
        self.key = key
        self.data = data

    def to_dict(self):
        return {'key': self.key, 'data': self.data}


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

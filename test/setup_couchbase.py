import os
import unittest
import subprocess
import socket
import time

try:
    import couchbase
    from couchbase.bucket import Bucket as CouchbaseBucket
    from couchbase.exceptions import CouchbaseNetworkError, CouchbaseTransientError
except ImportError:
    couchbase = None


from setup import hbom, TEST_DIR  # noqa

_couchbase_jar = os.path.join(os.path.dirname(__file__), "bin", "couchbasemock.jar")


skip_if_couchbase_disabled = unittest.skipIf(
    couchbase is None, "no couchbase package installed")

class CouchbaseLite(object):

    __slots__ = ['port', 'process', 'bucket']


    def __init__(self):
        """
        spin up the mock
        :param process: subprocess.Popen
        :return:
        """
        if not couchbase:
            return
        self.port = self._allocate_port()
        self.process = subprocess.Popen(['/usr/bin/env', 'java', '-jar',
                                         _couchbase_jar, '--port', "%d" % self.port])

        port = self.port
        e = None
        for i in xrange(0, 10):
            try:
                self.bucket = CouchbaseBucket('http://127.0.0.1:%d/default' % port)
                return

            except (CouchbaseNetworkError, CouchbaseTransientError) as e:
                pass
            time.sleep(0.1)

        raise e

    def _allocate_port(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("", 0))
        port = s.getsockname()[1]
        s.close()
        return port

    def kill(self):
        res =  self.process.terminate()
        self.process = None
        return res

    def is_alive(self):
        return True if self.process and self.process.poll() is None else False

    def __del__(self):
        try:
            if self.process:
                self.process.terminate()
        except AttributeError:
            pass



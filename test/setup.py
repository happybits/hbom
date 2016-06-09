# std lib
import os
import sys
import time
import uuid

# put our path in front so we can be sure we are testing locally
# not against the global package
TEST_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(TEST_DIR)
sys.path.insert(1, ROOT_DIR)

# our package
import hbom  # noqa


def generate_uuid():
    return str(uuid.uuid4())

StubModelChanges = []


class StubModel(hbom.Definition):

    id = hbom.StringField(primary=True, default=generate_uuid)


class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        if self.verbose:
            print 'elapsed time: %f ms' % self.msecs

#!/usr/bin/env python

# std-lib
import uuid
import time
import line_profiler

# test harness
from setup import Timer
from setup_redis import hbom


def xid():
    return uuid.uuid4().get_hex()


class Device(hbom.RedisObject):
    class storage(hbom.RedisHash):
        _keyspace = 'D'

    class definition(hbom.Definition):
        device_id = hbom.StringField(primary=True, default=xid)
        user_id = hbom.StringField()
        secret = hbom.StringField(required=True)
        app_type = hbom.StringField()
        app_version = hbom.StringField()
        app_build = hbom.StringField()
        platform_type = hbom.StringField()
        platform_version = hbom.StringField()
        flavor = hbom.StringField()
        push_token = hbom.StringField()
        voip_token = hbom.StringField()
        locale = hbom.StringField()
        timezone = hbom.StringField(default='UTC')
        created_at = hbom.FloatField(default=time.time)
        last_login = hbom.FloatField(default=time.time)
        experiment = hbom.StringField()
        cohort = hbom.StringField()
        experiment_active = hbom.BooleanField()


def bench(iterations=1000):
    secret = xid()
    user_id = xid()
    push_token = xid() + xid()
    device_args = {
        'secret': secret,
        'user_id': user_id,
        'app_type': 'foo',
        'app_version': '0.1.1',
        'platform_type': 'android',
        'platform_version': '0.0.1',
        'flavor': 'debug',
        'push_token': push_token,
        'locale': 'en_US',
        'timezone': 'UTC',
        'experiment': 'test1',
        'cohort': 'A',
        'experiment_active': True
    }

    device_ids = []
    for _ in xrange(iterations):

        d = Device.new(**device_args)
        Device.save(d)
        device_ids.append(d.device_id)

    for device_id in device_ids:
        Device.get(device_id)

    Device.get_multi(device_ids)

    pipe = hbom.Pipeline()
    devices = []
    for device_id in device_ids:
        devices.append(Device.get(device_id, pipe=pipe))

    pipe.execute()

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p', '--profile',
        help='profile the script', action='store_true', default=False)

    parser.add_argument(
        '-i', '--iterations', type=int,
        help='number of objects to iterate through', default=1000)

    args = parser.parse_args()
    if args.profile:
        profile = line_profiler.LineProfiler(bench)
        profile.add_module(hbom.redis_backend)
        profile.run('bench(%s)' % args.iterations)
        profile.print_stats()
    else:
        with Timer(verbose=True) as t:
            bench(args.iterations)

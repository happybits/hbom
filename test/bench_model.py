#!/usr/bin/env python

from setup import hbom, Timer
import uuid
import time
import line_profiler


def xid():
    return uuid.uuid4().get_hex()


class Device(hbom.RedisModel):
    _keyspace = 'D'
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

        d = Device(**device_args)
        d.save()
        device_ids.append(d.device_id)

    for device_id in device_ids:
        d = Device.get(device_id)

    Device.get(device_ids)

    pipe = hbom.Pipeline()
    devices = []
    for device_id in device_ids:
        d = Device.ref(device_id, pipe=pipe)
        devices.append(d)

    pipe.execute()

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--profile',
                        help='profile the script', action='store_true', default=False)
    parser.add_argument('-i', '--iterations', type=int,
                        help='number of objects to iterate through', default=1000)

    args = parser.parse_args()
    if args.profile:
        profile = line_profiler.LineProfiler(bench)
        profile.add_module(hbom.model)
        profile.run('bench(%s)' % args.iterations)
        profile.print_stats()
    else:
        with Timer(verbose=True) as t:
            bench(args.iterations)
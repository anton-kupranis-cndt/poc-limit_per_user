import time
from functools import wraps
from contextlib import contextmanager

from celery import Celery
from redis import StrictRedis

BROKER_URL = 'redis://localhost:6379/0'
BACKEND_URL = 'redis://localhost:6379/1'
KEY_LIMIT = 2
RETRY_SEC = 8

celery = Celery("tasks", broker=BROKER_URL, backend=BACKEND_URL)
redis_client = StrictRedis(host="localhost", port=6379, decode_responses=True)

_LUA_SHA = None
_LUA_SCRIPT = '''
    local e = redis.call('GET', KEYS[1])
    if not e then
        redis.call('SET', KEYS[1], 1, 'EX', ARGV[2])
        return 1
    elseif e < ARGV[1] then
        redis.call('SET', KEYS[1], e + 1, 'EX', ARGV[2])
        return e + 1
    end
    return 0   
'''
_LUA_SCRIPT_ = '''
    local e = redis.call('GET', KEYS[1])
    if not e or e < 1 then
        redis.call('SET', KEYS[1], 0, 'EX', ARGV[2])
        return 0
    else
        redis.call('SET', KEYS[1], e - 1, 'EX', ARGV[2])
        return e
    end
'''


@contextmanager
def key_release(key):
    try:
        yield
    finally:
        global _LUA_SHA
        if _LUA_SHA is None:
            _LUA_SHA = redis_client.register_script(_LUA_SCRIPT_)

        r = _LUA_SHA(keys=[key], args=[KEY_LIMIT, 60])
        redis_client.decr(key, 1)


def task_id_limiter(func):
    @wraps(func)
    def inner(*args, **kwargs):
        key = args[0]
        global _LUA_SHA
        if _LUA_SHA is None:
            _LUA_SHA = redis_client.register_script(_LUA_SCRIPT)

        check_key = _LUA_SHA(keys=[key], args=[KEY_LIMIT, 60])
        if check_key:
            with key_release(key):
                return func(*args, **kwargs)
        else:
            dummy_task.apply_async(args=args, countdown=RETRY_SEC)
    return inner


@celery.task
@task_id_limiter
def dummy_task(task_key, sec_to_sleep):
    time.sleep(sec_to_sleep)
    return sec_to_sleep



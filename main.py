import time
import inspect
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

_LUA_SHA_INCREASE = None
_LUA_SCRIPT_INCREASE = '''
    local e = redis.call('GET', KEYS[1]) or 0
    e = tonumber(e) + 1
    if e <= tonumber(ARGV[1]) then redis.call('SET', KEYS[1], e, 'EX', tonumber(ARGV[2])) end
    return e
'''
_LUA_SHA_DECREASE = None
_LUA_SCRIPT_DECREASE = '''
    local e = redis.call('GET', KEYS[1]) or 0
    e = tonumber(e)
    if e < 1 then e = 1 end
    e = e - 1
    redis.call('SET', KEYS[1], e, 'EX', tonumber(ARGV[1]))
    return e
'''


@contextmanager
def key_releaser(key):
    try:
        yield
    finally:
        global _LUA_SHA_DECREASE
        if _LUA_SHA_DECREASE is None:
            _LUA_SHA_DECREASE = redis_client.register_script(_LUA_SCRIPT_DECREASE)

        r = _LUA_SHA_DECREASE(keys=[key], args=[600])


def bind_arguments(func, *args, **kwds):
    signature = inspect.signature(func)
    bound = signature.bind(*args, **kwds)
    bound.apply_defaults()
    return bound.arguments.copy()


def task_limiter(tkey):

    def key_factory(func, *args, **kwargs):
        if isinstance(tkey, str):
            key_params = dict(bind_arguments(func, *args, **kwargs))
            key_params["task_name"] = func.__name__
            return tkey.format_map(key_params)

        return tkey(*args, **kwargs)

    def retry(func, *args, **kwargs):
        globals()[func.__name__].apply_async(args=args, countdown=RETRY_SEC)

    def decor(func):
        @wraps(func)
        def inner(*args, **kwargs):
            key = key_factory(func, *args, **kwargs)
            global _LUA_SHA_INCREASE
            if _LUA_SHA_INCREASE is None:
                _LUA_SHA_INCREASE = redis_client.register_script(_LUA_SCRIPT_INCREASE)
            check_key = _LUA_SHA_INCREASE(keys=[key], args=[KEY_LIMIT, 600])

            if check_key <= KEY_LIMIT:
                with key_releaser(key):
                    return func(*args, **kwargs)
            else:
                retry(func, *args, **kwargs)
        return inner
    return decor


@celery.task(acks_late=True, reject_on_worker_lost=True)
@task_limiter(tkey="task_limit:{task_name}:{task_key}")
def dummy_task(task_key, sec_to_sleep):
    time.sleep(sec_to_sleep)
    return sec_to_sleep



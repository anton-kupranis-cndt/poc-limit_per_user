import time
from functools import wraps

from celery import Celery, Task
from redis import StrictRedis

BROKER_URL = 'redis://localhost:6379/0'
BACKEND_URL = 'redis://localhost:6379/1'
KEY_LIMIT = 2
RETRY_SEC = 8

celery = Celery("tasks", broker=BROKER_URL, backend=BACKEND_URL)
redis_client = StrictRedis(host="localhost", port=6379, decode_responses=True)


class LimitedTask(Task):

    def __init__(self, *args, **kwargs):
        self.redis = redis_client
        super().__init__(*args, **kwargs)

    def on_success(self, retval, task_id, args, kwargs):
        self._release_key(args[0])

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        self._release_key(args[0])

    def _release_key(self, task_key):
        r = self.redis.decr(task_key, 1)


def decor(func):
    @wraps(func)
    def inner(self, *args, **kwargs):
        key = args[0]
        check_key = int(self.redis.incr(key, 1))
        if check_key <= KEY_LIMIT:
            return func(self, *args, **kwargs)
        else:
            self._release_key(key)
            self.retry(countdown=RETRY_SEC)
    return inner


@celery.task(bind=True, base=LimitedTask, max_retries=200)
@decor
def dummy_task(self, task_key, sec_to_sleep):
    time.sleep(sec_to_sleep)
    return sec_to_sleep



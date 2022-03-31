import time
from main import dummy_task
from redis import Redis

redis_cli = Redis()


def send_tasks(task_list):
    for task in task_list:
        dummy_task.apply_async(args=(task, 10))
        print(f"{time.time()} sending task {task}")


tasks_1 = ["user_1" for _ in range(10)]
tasks_2 = ["user_2" for _ in range(6)] + [f"user_{x}" for x in range(3, 8)]

send_tasks(tasks_1)
time.sleep(5)
send_tasks(tasks_2)

for task in tasks_1:
    dummy_task.apply_async(args=(task, 10))
time.sleep(5)
for task in tasks_2:
    dummy_task.apply_async(args=(task, 10))

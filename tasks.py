import time
from celery import Celery

from background_task import send_to_rabbitMQ

app = Celery('tasks', broker='redis://localhost:6379/0',backend='redis://localhost:6379/0')

@app.task
def add(x, y):
    return x + y

@app.task
def chunkFile():
    file = "/Users/bsudharshan/PycharmProjects/flask_web/words.txt"
    chunk_size = 8
    time.sleep(10)
    words = []
    count = 0
    with open(file=file) as inputfile:
        for line in inputfile:
            words.append(line)
            count = count + 1
            if count == chunk_size:
                print("chunking...")
                count = 0
                print(words)
                send_to_rabbitMQ(str(words), "mapper")
                words = []

    if len(words) > 0:
        send_to_rabbitMQ(str(words), "mapper")

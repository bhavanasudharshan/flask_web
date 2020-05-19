from concurrent.futures.thread import ThreadPoolExecutor

import pika
from celery import Celery
from flask import json
from pymongo import MongoClient

try:
    from .rediscli import get_cache
except ImportError:
    from rediscli import get_cache

try:
    from .background_task import send_to_rabbitMQ
except ImportError:
    from background_task import send_to_rabbitMQ



celery = Celery('tasks', broker='amqp://guest@rabbitmq//', backend='amqp://guest@rabbitmq//')
db_client = MongoClient(host="mongodb")
executor = ThreadPoolExecutor(max_workers=3)


@celery.task
def add(x, y):
    return x + y


@celery.task
def createMapperJobs():
    file = "/app/words.txt"
    for i in range(0, 1000):
        lines = []
        with open(file=file) as inputfile:
            for line in inputfile:
                lines.append(line)
            print("creating a mapper job {}")
            print(json.dumps(lines))
            get_cache().incr("COUNT")
            send_to_rabbitMQ(json.dumps(lines), "mapper")

    # start the mapper consumer threads once the total jobs are tracked
    executor.submit(threaded_rmq_mapper_task)
    executor.submit(threaded_rmq_mapper_task)
    executor.submit(threaded_rmq_mapper_task)


@celery.task
def reducer(key):
    print("starting reducer job {0}".format(key))
    mydb = db_client["crm"]
    results_collection = mydb["results"]
    reducer_entry = mydb["reducerKeys"].find_one({'key': key})

    count = 0

    for c in reducer_entry['value']:
        print("count incrementing for reducer {0}".format(key))
        count = count + c
    results_collection.update({"key": key}, {'$set': {"value": count}}, upsert=True)
    print("reducer finishing for key {0} output {1}".format(key, c))


@celery.task
def shuffle():
    reducer_job_keys = {}

    mydb = db_client["crm"]
    reducer_collection = mydb["reducerKeys"]

    mapper_output = mydb["mapperOutput"]
    for item in mapper_output.find():
        print(item)
        if item['key'] not in reducer_job_keys:
            reducer_job_keys[(item['key'])] = True
        reducer_collection.update_one({"key": item['key']}, {"$push": {"value": item['value']}}, upsert=True)

    for key in reducer_job_keys:
        reducer.delay(key)


def wordMapper(arr):
    print("in word mapper")
    mydb = db_client["crm"]
    mapperOutput = mydb["mapperOutput"]
    for item in arr:
        item = item.strip()
        mapperOutput.insert_one({'key': item, 'value': 1})
        print('inserting into mapperOutput {0}'.format(item))

    get_cache().decr("COUNT")
    count = get_cache().get("COUNT")

    if count == "0":
        print("finished processing all mapper jobs, shuffler task starting")
        shuffle.delay()
    else:
        print("remaining mapper jobs to be processed {0} {1} {2}".format(count, count == "0", count == 0))


def mapper_callback(ch, method, properties, body):
    print(" [x] Received mapper %r" % body)
    print(body.decode('utf8'))
    wordMapper(json.loads(body.decode('utf8')))


def threaded_rmq_mapper_task():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='mapper')

    channel.basic_consume(
        queue='mapper', on_message_callback=mapper_callback, auto_ack=True)

    print(' [*] Waiting for mapper messages. To exit press CTRL+C')
    channel.start_consuming()

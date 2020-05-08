import threading
import time

import pika
from celery import Celery
from flask import json
from pymongo import MongoClient

from background_task import send_to_rabbitMQ

celery = Celery('tasks', broker='redis://localhost:6379/0',backend='redis://localhost:6379/0')
db_client = MongoClient(host="mongodb")
@celery.task
def add(x, y):
    return x + y

@celery.task
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
                print(json.dumps(words))
                send_to_rabbitMQ(json.dumps(words), "mapper")
                words = []

    if len(words) > 0:
        send_to_rabbitMQ(str(words), "mapper")

    thread = threading.Thread(target=threaded_reducer)
    thread.daemon = True
    thread.start()

def threaded_reducer():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    status = channel.queue_declare(queue="mapper")
    if status.method.message_count == 0:
        print("mapper queue empty")
        mydb = db_client["crm"]
        for doc in mydb.words.find({}):
            print(doc)
            reducer.delay(doc['key'])
            # print(doc['key'])
    else:
        print("mapper queue not empty")

    channel.close()

@celery.task
def reducer(key):
    print('reducer starting for {0}'.format(key))
    mydb = db_client["crm"]
    count=0
    document = mydb.words.find_one({'key':key})
    for v in document['value']:
        count=count+v
    print("result of reducer for key {0}={1}".format(key,count))
    mydb.results.update_one({"key": key},{"$set": {"count": count}},upsert=True)
    mydb.words.delete_one({'key':key})

@celery.task
def shuffle(input):
    result={}
    print("in shuffler\n")
    sortedKeys=sorted(input)
    for k in sortedKeys:
        result[k]=input[k]
        myquery = {"key": k}
        newvalues = {"$push": {"value": result[k]}}
        mydb = db_client["crm"]
        mycol = mydb["words"]
        print("inserting into mongodo")
        mycol.update_one(myquery, newvalues,upsert=True)

    print(result)


@celery.task
def wordMapper(arr):
    result={}
    for item in arr:
        item=item.strip()
        print(item)
        print("\n")
        if item in result:
            result[item]=result[item]+1
        else:
            result[item]=1
    print(result)
    print("mapresult")
    shuffle.delay(result)


def mapper_callback(ch, method, properties, body):
    print(" [x] Received mapper %r" % body)
    print("Task Executed {}".format(threading.current_thread()))
    print(body.decode('utf8'))
    result=wordMapper.delay(json.loads(body))
    # result = add.delay(23, 42)
    print(result)
    print("mapping started...")

def threaded_rmq_mapper_task():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='mapper')

    channel.basic_consume(
        queue='mapper', on_message_callback=mapper_callback, auto_ack=True)

    print(' [*] Waiting for mapper messages. To exit press CTRL+C')
    channel.start_consuming()

def send_to_reduceMQ(req_data, queue_name):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))

    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='',
                              routing_key=queue_name, body=req_data)
    connection.close()


# def threaded_rmq_reducer_task(queue_name):
#     connection = pika.BlockingConnection(
#         pika.ConnectionParameters(host='rabbitmq'))
#     channel = connection.channel()
#
#     channel.queue_declare(queue=queue_name)
#
#     channel.basic_consume(
#         queue=queue_name, on_message_callback=mapper_callback, auto_ack=True)
#
#     print(' [*] Waiting for reducer messages. To exit press CTRL+C')
#     channel.start_consuming()
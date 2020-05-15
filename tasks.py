import threading

import pika
import time
from celery import Celery
from flask import json
from pymongo import MongoClient
from rediscli import get_cache

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
                get_cache().incr("COUNT")
                send_to_rabbitMQ(json.dumps(words), "mapper")
                words = []

    if len(words) > 0:
        get_cache().incr("COUNT")
        send_to_rabbitMQ(json.dumps(words), "mapper")

    thread = threading.Thread(target=threaded_reducer)
    thread.daemon = True
    thread.start()

def threaded_reducer():
    while True:
        c=get_cache().get("COUNT")
        if int(c)<=0:
            mydb = db_client["crm"]
            for doc in mydb.words.find({}):
                print(doc["key"])
                reducer.delay(doc['key'])
            break
        else:
            print("reducer waiting to be kicked off")
            time.sleep(10)

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
        print('shuffler inserting into mongodb  {0}'.format(k))
        mycol.update_one(myquery, newvalues,upsert=True)

    print(result)
    get_cache().decr("COUNT")

# @celery.task
def wordMapper(arr):
    print("in word mapper")
    result={}
    for item in arr:
        item=item.strip()
        print(item)
        # print("\n")
        if item in result:
            result[item]=result[item]+1
        else:
            result[item]=1
    print(result)
    print("mapresult")
    shuffle.delay(result)


def mapper_callback(ch, method, properties, body):
    print(" [x] Received mapper %r" % body)
    print(body.decode('utf8'))
    wordMapper(json.loads(body.decode('utf8')))
    # wordMapper(json.loads(body.decode('utf8'))).delay()

def threaded_rmq_mapper_task():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='mapper')

    channel.basic_consume(
        queue='mapper', on_message_callback=mapper_callback, auto_ack=True)

    print(' [*] Waiting for mapper messages. To exit press CTRL+C')
    channel.start_consuming()

# def send_to_reduceMQ(req_data, queue_name):
#     connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
#
#     channel = connection.channel()
#     channel.queue_declare(queue=queue_name)
#     channel.basic_publish(exchange='',
#                               routing_key=queue_name, body=req_data)
#     connection.close()
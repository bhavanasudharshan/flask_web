# flask_web/app.py
from concurrent.futures.thread import ThreadPoolExecutor

from pymongo import MongoClient
from flask import Flask,request
import pika
from threading import Thread
import json

import redis
from rediscli import get_cache
from background_task import threaded_rmq_consumer_task
from tasks import add, chunkFile,threaded_rmq_mapper_task

app = Flask(__name__)
app.config['enable-threads']=True
# app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
# app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
#
# app = Celery('tasks', broker='redis://localhost:6379/0',backend='redis://localhost:6379/0')
# db_client = MongoClient(host="mongodb")
# thread = Thread(target=threaded_rmq_consumer_task)
# thread.daemon = True
# thread.start()
#
# executor = ThreadPoolExecutor(max_workers=3)
# executor.submit(threaded_rmq_mapper_task)
# executor.submit(threaded_rmq_mapper_task)
# executor.submit(threaded_rmq_mapper_task)
# thread = Thread(target=threaded_rmq_mapper_task)
# thread.daemon = True
# thread.start()


db_client = MongoClient(host="mongodb")


@app.route('/')
def hello_world():
    return 'Hey, we have Flask in a Docker container!'


@app.route('/testredis')
def redis_test():
    try:
        get_cache().set("msg:hello", "Hello Redis!!!")
        msg = get_cache().get("msg:hello")
        return msg
    except redis.exceptions.ConnectionError as exc:
        raise exc


@app.route('/testconn')
def test_conn():
    db_client = MongoClient(host="mongodb")
    var = db_client["crm"]
    var.test.insert({'blah': 'blah'})
    db_client.close()
    return var.name

@app.route('/wordcount/<word>',methods=["GET"])
def word_count(word):
    cache_result=get_cache().get(word)
    if cache_result is not None:
        print("cache hit")
        return json.dumps({'word':word,'count':cache_result})
    else:
        doc=db_client.crm.results.find_one({'key': word})
        if doc is None:
            return json.dumps({'word':word,'count':0})

        get_cache().set(word,doc['count'],10)
        return json.dumps({'word':word,'count':doc['count']})


@app.route('/send',methods=["POST"])
def rmq_send():
    req_data = request.form
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='hello')
    channel.basic_publish(exchange='',
                          routing_key='hello', body=json.dumps(req_data))
    connection.close()


    return json.dumps({
                    'started. rabbitmq message sent': True})


@app.route('/recieve/<userId>',methods=["GET"])
def rmq_recieve(userId):
    if get_cache().get(userId) is None:
        return "found nothing in queue"
    else:
        return get_cache().get(userId)

@app.route('/wordcountstart',methods=["POST"])
def word_count_start():
    thread = Thread(target=threaded_rmq_consumer_task)
    thread.daemon = True
    thread.start()

    executor = ThreadPoolExecutor(max_workers=3)
    executor.submit(threaded_rmq_mapper_task)
    executor.submit(threaded_rmq_mapper_task)
    executor.submit(threaded_rmq_mapper_task)
    return json.dumps({'start':'success'})


if __name__ == '__main__':
    result = add.delay(23, 42)

    result  =chunkFile.delay()
    # print(result.ready())
    app.run(debug=False, host='0.0.0.0')


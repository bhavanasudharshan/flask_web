# flask_web/app.py
import time

from pymongo import MongoClient
from flask import Flask,request
import pika
from threading import Thread
import redis
import rediscli
from background_task import threaded_rmq_consumer_task,threaded_rmq_mapper_task
import json
from tasks import add, chunkFile

app = Flask(__name__)
app.config['enable-threads']=True
# app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
# app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'


thread = Thread(target=threaded_rmq_consumer_task)
thread.daemon = True
thread.start()

thread = Thread(target=threaded_rmq_mapper_task)
thread.daemon = True
thread.start()



@app.route('/')
def hello_world():
    return 'Hey, we have Flask in a Docker container!'


@app.route('/testredis')
def redis_test():
    try:
        rediscli.get_cache().set("msg:hello", "Hello Redis!!!")
        msg = rediscli.get_cache().get("msg:hello")
        return msg
    except redis.exceptions.ConnectionError as exc:
        raise exc


@app.route('/testconn')
def test_conn():
    db_client = MongoClient(host="mongodb")
    var = db_client["crm"]
    var.test.insert({'blah': 'blah'})
    return var.name


@app.route('/send',methods=["POST"])
def rmq_send():
    req_data = request.form
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='hello')
    channel.basic_publish(exchange='',
                          routing_key='hello', body=json.dumps(req_data))
    connection.close()


    return json.dumps({'thread_name': str(thread.name),
                    'started. rabbitmq message sent': True})


@app.route('/recieve/<userId>',methods=["GET"])
def rmq_recieve(userId):
    if rediscli.get_cache().get(userId) is None:
        return "found nothing in queue"
    else:
        return rediscli.get_cache().get(userId)


if __name__ == '__main__':
    result = add.delay(23, 42)
    result  =chunkFile.delay()
    print(result.ready())
    app.run(debug=False, host='0.0.0.0')


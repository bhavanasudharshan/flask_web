# flask_web/app.py
from pymongo import MongoClient
from flask import Flask,request
import pika
from threading import Thread
import redis

import rediscli
from background_task import threaded_rmq_consumer_task
import json

app = Flask(__name__)
app.config['enable-threads']=True
# cache = redis.Redis(host='redis', port=6379)
thread = Thread(target=threaded_rmq_consumer_task)
thread.daemon = True
thread.start()

@app.route('/')
def hello_world():
    return 'Hey, we have Flask in a Docker container!'


@app.route('/testredis')
def redis_test():
    try:
        rediscli.get_cache().set("msg:hello", "Hello Redis!!!")

        # step 5: Retrieve the hello message from Redis
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
    app.run(debug=True, host='0.0.0.0')

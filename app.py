# flask_web/app.py
import json
from concurrent.futures.thread import ThreadPoolExecutor
from time import strftime, gmtime

import pika
import redis
from celery import Celery
from flask import Flask, request
from pymongo import MongoClient

try:
    from .background_task import send_to_rabbitMQ
except ImportError:
    from background_task import send_to_rabbitMQ
try:
    from .rediscli import get_cache
except ImportError:
    from rediscli import get_cache


def make_celery(app):
    celery = Celery(
        'worker',
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery


flask_app = Flask(__name__)
flask_app.config['enable-threads'] = True
flask_app.config['CELERY_BROKER_URL'] = 'amqp://guest@rabbitmq//'
flask_app.config['CELERY_RESULT_BACKEND'] = 'amqp://guest@rabbitmq//'

celery = make_celery(flask_app)

db_client = MongoClient(host="mongodb")
executor = ThreadPoolExecutor(max_workers=3)


@flask_app.route('/')
def hello_world():
    return 'Hey, we have Flask in a Docker container!'


@flask_app.route('/testredis')
def redis_test():
    try:
        get_cache().set("msg:hello", "Hello Redis!!!")
        msg = get_cache().get("msg:hello")
        return msg
    except redis.exceptions.ConnectionError as exc:
        raise exc


@flask_app.route('/testconn')
def test_conn():
    db_client = MongoClient(host="mongodb")
    var = db_client["crm"]
    var.test.insert({'blah': 'blah'})
    db_client.close()
    return var.name


@flask_app.route('/wordcount/<word>', methods=["GET"])
def word_count(word):
    cache_result = get_cache().get(word)
    if cache_result is not None:
        print("cache hit")
        return json.dumps({'word': word, 'count': cache_result})
    else:
        doc = db_client.crm.results.find_one({'key': word})
        if doc is None:
            return json.dumps({'word': word, 'count': 0})

        get_cache().set(word, doc['value'], ex=5)
        return json.dumps({'word': word, 'count': doc['value']})


@flask_app.route('/send', methods=["POST"])
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


@flask_app.route('/recieve/<userId>', methods=["GET"])
def rmq_recieve(userId):
    if get_cache().get(userId) is None:
        return "found nothing in queue"
    else:
        return get_cache().get(userId)


@celery.task(name='app.createMapperJobs')
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


@celery.task(name='app.reducer')
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


@celery.task(name='app.shuffle')
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


@flask_app.route('/wordcountstart', methods=["POST"])
def word_count_start():
    if get_cache().get("COUNT") != "0":
        if 'force' not in request.form or request.form['force'] != "true":
            return json.dumps({"fail": "previous job still in progress. to force try set force=true"})
        else:
            print("forcing job")

    get_cache().set("COUNT", "0")
    db_client["crm"].drop_collection("mapperOutput")
    db_client["crm"].drop_collection("reducerKeys")
    db_client["crm"].drop_collection("results")

    createMapperJobs.delay()
    return json.dumps({'start': 'success', 'timestamp': strftime("%Y-%m-%d %H:%M:%S", gmtime())})


if __name__ == '__main__':
    flask_app.run(debug=False, host='0.0.0.0')

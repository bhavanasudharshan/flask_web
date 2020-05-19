import json

import pika

try:
    from .rediscli import get_cache
except ImportError:
    from rediscli import get_cache


def callback(ch, method, properties, body):
    b = json.loads(body)
    get_cache().set(b['key'], body)
    print(" [x] Received %r" % body)


def threaded_rmq_consumer_task():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='hello')

    channel.basic_consume(
        queue='hello', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for hello messages. To exit press CTRL+C')
    channel.start_consuming()


def send_to_rabbitMQ(req_data, queue_name):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))

    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='',
                          routing_key=queue_name, body=req_data)
    connection.close()

import pika
# from app import cache
import rediscli
import json

def callback(ch, method, properties, body):
    b=json.loads(body)
    rediscli.get_cache().set(b['key'], body)
    print(" [x] Received %r" % body)

def threaded_rmq_consumer_task():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='hello')

    channel.basic_consume(
        queue='hello', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
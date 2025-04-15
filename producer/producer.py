#!/usr/bin/env python3
import pika
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='hello')

for i in range(100):
    channel.basic_publish(exchange='', routing_key='hello', body='Hello World {}!'.format(i))
    print(" [x] Sent 'Hello World {}!'".format(i))
    time.sleep(1)

connection.close()
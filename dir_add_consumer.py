#!/usr/bin/env python

import json
import pika
import socket
import sys
import rabbitmq_config as rcfg

class DirAddMQ(object):

    USER = 'guest'
    PASSWORD = 'guest'
    HOST = 'localhost'
    PORT = 5672
    VHOST = '/'
    EXCHANGE = ''
    EXCHANGE_TYPE = 'direct'
    QUEUE = ''

    def __init__(self, config=None):
        print("things")
        if config:
           if 'exchange' in config:
               self.EXCHANGE = config['exchange']
           if 'queue' in config:
               self.QUEUE = config['queue']

        hostname = socket.gethostname().split(".", 1)[0]

        self.HOST = rcfg.Server if hostname != rcfg.Server else "localhost"
        self.USER = rcfg.User
        self.PASSWORD = rcfg.Password
        self.VHOST = rcfg.VHost
        self.PORT = rcfg.Port
        self._parameters = pika.ConnectionParameters(
                self.HOST,
                self.PORT,
                self.VHOST,
                pika.PlainCredentials(self.USER, self.PASSWORD))

    def connect(self):
        self._connection = pika.BlockingConnection(self._parameters)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
                exchange=self.EXCHANGE, 
                exchange_type=self.EXCHANGE_TYPE,
                durable=True)

        self._result = self._channel.queue_declare(
            queue=self.QUEUE,
            exclusive=True)
        self._queue_name = self._result.method.queue

    def disconnect(self):
        self._connection.close()

    def publish_msg(self, obj):
        self.connect()

        self._objects = sys.argv[1:]
        if not self._objects:
            sys.stderr.write("Usage: %s [ood] [ohpc] [manager]\n" % sys.argv[0])
            sys.exit(1)

        for obj in self._objects:
            channel.queue_bind(
            exchange='direct_logs', 
            queue=self._queue_name, 
            routing_key=obj['routing_key'])

        print(' [*] Waiting for logs. To exit press CTRL+C')

        def callback(ch, method, properties, body):
            print(" [x] %r:%r" % (method.routing_key, body))
            print('[%r]  User creation task is done.' % method.routing_key)


        self._channel.basic_consume(queue=self._queue_name, 
                                    on_message_callback=self.callback,
                                    auto_ack=True)

        self.channel.start_consuming()


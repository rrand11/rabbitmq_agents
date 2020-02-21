#!/usr/bin/env python

import json
import pika
import socket
import sys
import rabbit_config as rcfg

# first import the class
from usrreg_mq import UserRegMQ

# Initial with Exchange name:
userreg_mq = UserRegMQ({ 'exchange': 'direct_logs' })

# specify routing key and the message(JSON format)
userreg_mq.publish_msg({
        'routing_key': 'ohpc',
        'msg': {
                "username": 'user_name',
                "fullname": "Full Name",
                "reason": "Reason1, Reason2."
        }
})

connection = userreg_mq.connect()


result = userreg_mq._channel.queue_declare(
            queue='',
            exclusive=True)

queue_name = result.method.queue


if not queue_name:
    sys.stderr.write("Usage: %s [ood] [ohpc] [manager]\n" % sys.argv[0])
    sys.exit(1)

for routing_key in queue_name:
    userreg_mq._channel.queue_bind(
    exchange='direct_logs', 
    queue=queue_name, 
    routing_key= 'ohpc')

print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))
    print('[%r]  User creation task is done.' % method.routing_key)


userreg_mq._channel.basic_consume(queue=queue_name, 
                      on_message_callback=callback,
                      auto_ack=True)

userreg_mq._channel.start_consuming()

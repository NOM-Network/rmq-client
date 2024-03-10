import pika
import threading
from pika_messenger import PikaMessenger
from config import RMQ_ADDR, RMQ_PORT
from asyncio import *

# listener
def start_rmq_consumer():
    def rmq_callback(ch, method, properties, body):
        m = str(body).strip().replace('"', '').replace("'", '')
        m = m.lstrip('b').replace('\\n', '').strip()
        print('got chat: ', m)
        # do something with chat ...
    with PikaMessenger(RMQ_ADDR) as consumer:
        consumer.consume(keys=['melba'], callback=rmq_callback)

consumer_thread = threading.Thread(target=start_rmq_consumer)
consumer_thread.start()
print('listening')

rmq_creds = pika.credentials.PlainCredentials('melba', 'melba')
rmq = pika.BlockingConnection(
    pika.ConnectionParameters(RMQ_ADDR, RMQ_PORT, '/', rmq_creds)
)
rmq_melba = rmq.channel()
rmq_melba.queue_declare(queue='melba', exclusive=False, durable=True)

# send
rmq_melba.basic_publish(
    exchange = '',
    routing_key ='melba',
    body = 'hello world!'
)
print('sent')

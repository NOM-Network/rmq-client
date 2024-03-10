import pika
from config import RMQ_ADDR, RMQ_PORT, RMQ_USER, RMQ_PASS, RMQ_SEND

other_user = RMQ_SEND

class PikaMessenger():
    exchange_name = other_user
    def __init__(self, *args, **kwargs):
        rmq_creds = pika.credentials.PlainCredentials(RMQ_USER, RMQ_PASS)
        self.conn = pika.BlockingConnection(
            pika.ConnectionParameters(RMQ_ADDR, RMQ_PORT, '/', rmq_creds)
        )
        self.channel = self.conn.channel()
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            durable=True,
            exchange_type='direct')
    def consume(self, keys, callback):
        self.channel.queue_declare('', exclusive=False)
        self.channel.queue_bind(
            exchange=self.exchange_name,
            queue=other_user,
            routing_key=other_user)
        self.channel.basic_consume(
            queue=other_user,
            on_message_callback=callback,
            auto_ack=True)
        self.channel.start_consuming()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

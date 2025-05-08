import datetime
import json
import logging
import threading
from typing import Optional, Callable, Any

import pika

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')


class Subscriber(threading.Thread):
    def __init__(self, exchange_name: str, message_handler: Optional[Callable[[dict], Any]] = None):
        super().__init__()
        self.message_handler = message_handler
        self.exchange_name = exchange_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='fanout')
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        self.channel.queue_bind(exchange=exchange_name, queue=self.queue_name)

        logger.info(f"✅ Connected to fanout queue: {self.exchange_name}")

    def _on_message(self, channel, method, properties, body):
        try:
            timestamp = get_timestamp()
            #logger.info(f"📥 Message received. Suscriber {self.queue_name} Timestamp: {timestamp}--------------")
            message = json.loads(body)
            self.message_handler(message)
            #logger.info(f"📥 Message acked. Queue {self.queue_name} Timestamp: {timestamp} ---------------")
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error on queue consumer {self.exchange_name}: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error(f"❌ Error processing message on queue consumer {self.exchange_name}: {e}. body: {body}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def close(self):
        try:
            if self.connection and not self.connection.is_closed:
                # self._connection.close()
                # TODO revisar la liberacion de recursos. El connection close cierra la cola si esta en otro thread?
                # TODO los threads daemon se cierran solos al cerrar el worker?
                # self.channel_thread.stop()
                logger.info("Connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")

    def run(self):
        logger.info(f"🟢 Starting fanout consumer '{self.queue_name}'")
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self._on_message, auto_ack=True)
        self.channel.start_consuming()

def get_timestamp():
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
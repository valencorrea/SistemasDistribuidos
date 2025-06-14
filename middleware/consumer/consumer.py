#!/usr/bin/env python3

import json
import logging
import threading
import uuid
from typing import Callable, Any, Optional, Literal

import pika
import datetime

logger = logging.getLogger(__name__)

class Consumer(threading.Thread):
    def __init__(self, queue_name: str,
                 _message_handler: Optional[Callable[[dict], Any]] = None,
                 queue_type: Literal['direct', 'fanout'] = 'direct'):
        super().__init__()
        self._queue_name = queue_name
        self._queue_type = queue_type
        self._message_handler = _message_handler or (lambda x: x)
        self._connection = None
        self._channel = None
        self._exchange_name = f'{queue_type}_exchange'
        self._consumer_id = str(uuid.uuid4())[:8]
        self._actual_queue_name = None
        self._delivery_tags = {}  # batchs_ids para rabbitmq

    def connect(self) -> bool:
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='rabbitmq',
                    connection_attempts=3,
                    retry_delay=5,
                    heartbeat=240,
                    socket_timeout=30
                )
            )
            self._channel = self._connection.channel()
            self._channel.exchange_declare(
                exchange=self._exchange_name,
                exchange_type=self._queue_type,
                durable=True
            )

            if self._queue_type == 'direct':
                self._channel.queue_declare(queue=self._queue_name, durable=True)
                queue_name = self._queue_name
            else:
                result = self._channel.queue_declare(queue='', exclusive=True, auto_delete=True)
                queue_name = result.method.queue

            self._channel.queue_bind(
                exchange=self._exchange_name,
                queue=queue_name,
                routing_key=self._queue_name if self._queue_type == 'direct' else ''
            )

            self._actual_queue_name = queue_name
            logger.info(f"✅ Connected to queue: {self._queue_name}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to configure consumer: {e}")
            return False

    def _on_message(self, channel, method, properties, body):
        try:
            timestamp = get_timestamp()
            logger.debug(f"📥 Message received. Queue {self._queue_name} Timestamp: {timestamp}--------------")
            message = json.loads(body)
            self._message_handler(message)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            logger.debug(f"📥 Message acked. Queue {self._queue_name} Timestamp: {timestamp} ---------------")

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _on_message_2(self, channel, method, properties, body):
        try:
            timestamp = get_timestamp()
            logger.debug(f"Message received. Queue {self._queue_name} Timestamp: {timestamp}--------------")
            message = json.loads(body)

            message_id = str(message.get("batch_id"))
            if message_id is None:
                raise ValueError("Message does not contain 'batch_id'")

            self._delivery_tags[message_id] = method.delivery_tag
            self._message_handler(message)
            logger.debug(f"📥 Message acked. Queue {self._queue_name} Timestamp: {timestamp} ---------------")

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_consuming(self):
        if not self._connection or self._connection.is_closed:
            if not self.connect():
                return

        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            queue=self._actual_queue_name,
            on_message_callback=self._on_message,
            auto_ack=False
        )

        logger.info("🟢 Waiting for messages...")
        try:
            self._channel.start_consuming()
        except Exception as e:
            logger.error(f"Error during consuming: {e}")
            self.close()

    def start_consuming_2(self):
        if not self._connection or self._connection.is_closed:
            if not self.connect():
                return

        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            queue=self._actual_queue_name,
            on_message_callback=self._on_message_2,
            auto_ack=False
        )

        logger.info("🟢 Waiting for messages...")
        try:
            self._channel.start_consuming()
        except Exception as e:
            logger.error(f"Error during consuming: {e}")
            self.close()

    def close(self):
        try:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
                logger.info("Connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
    
    def run(self):
        logger.debug(f"🟢 Starting direct consumer '{self._queue_name}'")
        self.start_consuming()

    def ack(self, message_id: str):
        delivery_tag = self._delivery_tags.pop(message_id, None)
        if delivery_tag is not None:
            try:
                self._channel.basic_ack(delivery_tag=delivery_tag)
                logger.debug(f"ACK sent for message_id {message_id}")
            except Exception as e:
                logger.error(f"Failed to ack message_id {message_id}: {e}")
        else:
            logger.warning(f"No delivery tag found for message_id {message_id}")


def get_timestamp():
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
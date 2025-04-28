#!/usr/bin/env python3

import json
import logging
import uuid
from typing import Callable, Any, Optional, Literal

import pika


logger = logging.getLogger(__name__)


class Consumer:
    def __init__(self, queue_name: str,
                 _message_handler: Optional[Callable[[dict], Any]] = None,
                 queue_type: Literal['direct', 'fanout'] = 'direct'):
        self._queue_name = queue_name
        self._queue_type = queue_type
        self._message_handler = _message_handler or (lambda x: x)
        self._connection = None
        self._channel = None
        self._exchange_name = f'{queue_type}_exchange'
        self._consumer_id = str(uuid.uuid4())[:8]
        self._actual_queue_name = None

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
            message = json.loads(body)
            logger.info(f"📥 Message received")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            self._message_handler(message)

        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
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

    def close(self):
        try:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
                logger.info("Connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")

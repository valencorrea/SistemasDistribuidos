import json
import logging
import time
from typing import Any, Literal, Optional

import pika

logger = logging.getLogger(__name__)

class Producer:
    def __init__(self, queue_name: str = 'default', queue_type: Literal['direct', 'fanout'] = 'direct',
                 dlx:Optional[str] = None, ttl: Optional[int] = None):
        self._queue_name = queue_name
        self._queue_type = queue_type
        self._connection = None
        self._channel = None
        self._closing = False
        self._exchange_name = f'{queue_type}_exchange'
        self._dlx = dlx
        self._ttl = ttl

    def connect(self) -> bool:
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='rabbitmq',
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
            self._channel = self._connection.channel()

            self._channel.exchange_declare(
                exchange=self._exchange_name,
                exchange_type=self._queue_type,
                durable=True
            )

            if self._queue_type == 'direct':
                queue_args = {}
                if self._ttl:
                    queue_args['x-message-ttl'] = self._ttl
                if self._dlx:
                    queue_args['x-dead-letter-exchange'] = self._dlx

                self._channel.queue_declare(
                    queue=self._queue_name,
                    durable=True,
                    arguments=queue_args or None
                )
                self._channel.queue_bind(
                    exchange=self._exchange_name,
                    queue=self._queue_name,
                    routing_key=self._queue_name
                )

            logger.info(f"✅ Productor conectado a la cola: {self._queue_name} exchange_name: {self._exchange_name} queue_type: {self._queue_type}")
            return True

        except Exception:
            logger.error(f"❌ Error al configurar productor")
            return False

    def enqueue(self, message, routing_key_override: Optional[str] = None, backoff = 0.0) -> bool:
        logger.debug(f"Intentando enviar mensaje a la cola: {self._queue_name}")
        try:
            if not self._connection or self._connection.is_closed:
                if not self.connect():
                    logger.error(f"Se intento enviar un mensaje por la cola: {self._queue_name}, pero no esta conectada")
                    return False

            routing_key = routing_key_override or (self._queue_name if self._queue_type == 'direct' else '')

            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=routing_key,
                body=json.dumps(message).encode(),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                )
            )
            logger.debug(f"✅ Mensaje enviado a la cola: {self._queue_name}")
            return True

        except Exception as e:
            logger.error(f"❌ Error al enviar mensaje")
            time.sleep(backoff)
            return self.enqueue(message, routing_key_override, backoff=backoff + 0.5)

    def getname(self):
        return self._queue_name

    def close(self):
        try:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
                logger.debug("✅ Conexión cerrada correctamente")
        except Exception as e:
            logger.error(f"❌ Error al cerrar conexión: {e}")
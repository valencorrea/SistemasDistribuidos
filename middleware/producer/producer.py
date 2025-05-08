import json
import logging
from typing import Any, Literal
import time
from typing import Any, Literal, Optional

import pika


logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')

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

            # Declarar exchange según el tipo especificado
            self._channel.exchange_declare(
                exchange=self._exchange_name,
                exchange_type=self._queue_type,
                durable=True
            )

            # Solo declarar y vincular cola si es tipo direct
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

    def enqueue(self, message: Any, routing_key_override: Optional[str] = None) -> bool:
        logger.debug(f"Intentando enviar mensaje a la cola: {self._queue_name}")
        try:
            if not self._connection or self._connection.is_closed:
                if not self.connect():
                    return False

            # Para fanout, el routing_key se ignora pero lo mantenemos por consistencia
            routing_key = routing_key_override or (self._queue_name if self._queue_type == 'direct' else '')

            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=routing_key,
                body=json.dumps(message).encode(),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # hace el mensaje persistente
                )
            )
            logger.debug(f"✅ Mensaje enviado a la cola: {self._queue_name}")
            return True

        except Exception as e:
            logger.error(f"❌ Error al enviar mensaje: {e}")
            return False

    def close(self):
        try:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
                logger.debug("✅ Conexión cerrada correctamente")
        except Exception as e:
            logger.error(f"❌ Error al cerrar conexión: {e}")
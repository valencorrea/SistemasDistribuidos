#!/usr/bin/env python3
import pika
import signal
import sys
import time
import logging
import json
from typing import Any, Literal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Producer:
    def __init__(self, queue_name: str = 'default', queue_type: Literal['direct', 'fanout'] = 'direct'):
        self._queue_name = queue_name
        self._queue_type = queue_type
        self._connection = None
        self._channel = None
        self._closing = False
        self._exchange_name = f'{queue_type}_exchange'
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logger.info('SIGTERM recibido - Iniciando graceful shutdown')
        self._closing = True
        if self._connection:
            self._notify_shutdown()
            self._connection.close()

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
                self._channel.queue_declare(
                    queue=self._queue_name,
                    durable=True
                )
                self._channel.queue_bind(
                    exchange=self._exchange_name,
                    queue=self._queue_name,
                    routing_key=self._queue_name
                )

            logger.info(f"✅ Productor conectado a la cola: {self._queue_name} exchange_name: {self._exchange_name} queue_type: {self._queue_type}")
            return True

        except Exception as e:
            logger.error(f"❌ Error al configurar productor: {e}")
            return False

    def enqueue(self, message: Any) -> bool:
        try:
            if not self._connection or self._connection.is_closed:
                if not self.connect():
                    return False

            # Para fanout, el routing_key se ignora pero lo mantenemos por consistencia
            routing_key = self._queue_name if self._queue_type == 'direct' else ''

            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=routing_key,
                body=json.dumps(message).encode(),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # hace el mensaje persistente
                )
            )

            logger.info(f"📤 Mensaje enviado")
            return True

        except Exception as e:
            logger.error(f"❌ Error al enviar mensaje: {e}")
            return False

    # def _notify_shutdown(self):
    #     """Notifica a los consumidores que el productor se está cerrando"""
    #     shutdown_message = {
    #         "type": "shutdown",
    #         "timestamp": time.time(),
    #         "message": "Producer se está cerrando"
    #     }
    #     self.enqueue(shutdown_message)
    #     logger.info("Notificación de shutdown enviada a los consumers")

    def close(self):
        try:
            if self._connection and not self._connection.is_closed:
                # self._notify_shutdown()
                self._connection.close()
                logger.info("✅ Conexión cerrada correctamente")
        except Exception as e:
            logger.error(f"❌ Error al cerrar conexión: {e}")
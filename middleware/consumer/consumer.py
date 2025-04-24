#!/usr/bin/env python3
from time import sleep

import pika
import signal
import sys
import time
import logging
import uuid
import json
from typing import Callable, Any, Optional, Literal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Consumer:
    def __init__(self, queue_name: str,
                 message_factory: Callable = None,
                 queue_type: Literal['direct', 'fanout'] = 'direct'):
        self._queue_name = queue_name
        self._queue_type = queue_type
        self._message_factory = message_factory or (lambda x: x)
        self._connection = None
        self._channel = None
        self._closing = False
        self._producer_active = True
        self._consumer_id = str(uuid.uuid4())[:8]
        self._exchange_name = f'{queue_type}_exchange'
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logger.info('SIGTERM recibido - Iniciando graceful shutdown')
        self._closing = True
        if self._connection:
            self._connection.close()

    def _handle_shutdown_message(self, message: dict):
        """Maneja el mensaje de shutdown del producer"""
        if isinstance(message, dict) and message.get("type") == "shutdown":
            self._producer_active = False
            logger.info(f"Producer se está cerrando: {message.get('message')}")
            logger.info(f"Timestamp del cierre: {message.get('timestamp')}")
            return True
        return False

    def connect(self) -> bool:
        """Establece conexión con RabbitMQ y configura la cola"""
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='rabbitmq',
                    connection_attempts=3,
                    retry_delay=5,
                    heartbeat=30,
                    socket_timeout=30
                ))
            self._channel = self._connection.channel()

            # Declarar exchange según el tipo
            self._channel.exchange_declare(
                exchange=self._exchange_name,
                exchange_type=self._queue_type,
                durable=True
            )

            if self._queue_type == 'direct':
                # Para colas direct, usar la cola compartida
                queue_name = self._queue_name
                self._channel.queue_declare(
                    queue=queue_name,
                    durable=True
                )
            else:
                # Para fanout, crear una cola exclusiva y temporal
                result = self._channel.queue_declare(
                    queue='',  # Cola con nombre aleatorio
                    exclusive=True,  # Se eliminará al desconectarse
                    auto_delete=True  # Se eliminará cuando no haya consumidores
                )
                queue_name = result.method.queue

            # Vincular la cola al exchange
            self._channel.queue_bind(
                exchange=self._exchange_name,
                queue=queue_name,
                routing_key=self._queue_name if self._queue_type == 'direct' else ''
            )

            # Guardar el nombre de la cola para uso en dequeue
            self._actual_queue_name = queue_name

            self._producer_active = True
            logger.info(f"✅ Consumidor conectado a la cola: {self._queue_name}")
            return True

        except Exception as e:
            logger.error(f"❌ Error al configurar consumidor: {e}")
            return False

    def dequeue(self, timeout: int = 1) -> Optional[Any]:
        """Recibe un mensaje de la cola de forma síncrona"""
        try:
            sleep(0.05)
            if not self._connection or self._connection.is_closed:
                if not self.connect():
                    return None

            # Configurar QoS
            self._channel.basic_qos(prefetch_count=1)

            # Obtener mensaje de la cola actual
            method, properties, body = self._channel.basic_get(
                queue=self._actual_queue_name,
                auto_ack=True
            )

            if method:
                try:
                    message = json.loads(body)

                    # Manejar mensaje de shutdown
                    if self._handle_shutdown_message(message):
                        logger.info("👋 Recibido mensaje de shutdown")
                        return None

                    # Procesar mensaje normal
                    result = self._message_factory(message)
                    #logger.info(f"📥  Mensaje recibido: {message}")
                    if message.get("total_batches") and message.get("total_batches") > 0:
                        logger.info(f"📥  Resultado: {result}")
                    return result
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Error decodificando mensaje JSON: {e}")
                    return None
                except Exception as e:
                    logger.error(f"❌ Error procesando mensaje: {e}")
                    return None

            return None

        except Exception as e:
            logger.error(f"❌ Error al recibir mensaje: {e}")
            return None

    def close(self):
        """Cierra la conexión con RabbitMQ"""
        try:
            if self._connection and not self._connection.is_closed:
                self._connection.close()
                logger.info("✅ Conexión cerrada correctamente")
        except Exception as e:
            logger.error(f"❌ Error al cerrar conexión: {e}")
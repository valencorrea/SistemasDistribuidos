#!/usr/bin/env python3
import pika
import signal
import sys
import time
import logging
import uuid
from typing import Callable, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Consumer:
    def __init__(self, queue_name: str = 'hello', message_factory: Callable[[bytes], Any] = None):
        self._connection = None
        self._channel = None
        self._closing = False
        self._queue_name = queue_name
        self._message_factory = message_factory or (lambda x: x)
        self._producer_active = True
        self._consumer_id = str(uuid.uuid4())[:8]  # ID único para cada consumidor
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logger.info('SIGTERM recibido - Iniciando graceful shutdown')
        self._closing = True
        if self._connection:
            self._connection.close()

    def _handle_shutdown_message(self, message: dict):
        """Maneja el mensaje de shutdown del producer"""
        if message.get("type") == "shutdown":
            self._producer_active = False
            logger.info(f"Producer se está cerrando: {message.get('message')}")
            logger.info(f"Timestamp del cierre: {message.get('timestamp')}")
            return True
        return False

    def connect(self):
        while not self._closing:
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

                # Declarar el exchange de tipo fanout
                self._channel.exchange_declare(
                    exchange='fanout_exchange',
                    exchange_type='fanout',
                    durable=True
                )

                # Crear una cola exclusiva para este consumidor
                result = self._channel.queue_declare(
                    queue=f'{self._queue_name}_{self._consumer_id}',
                    exclusive=True
                )

                # Vincular la cola al exchange
                self._channel.queue_bind(
                    exchange='fanout_exchange',
                    queue=result.method.queue
                )

                self._producer_active = True
                logger.info(f"Conexion establecida exitosamente con RabbitMQ. ID del consumidor: {self._consumer_id}")
                return True
            except Exception as e:
                logger.error(f"Error inesperado: {e}")
                return False
        return False

    def dequeue(self, timeout: int = 1) -> Any:
        """
        Obtiene un mensaje de la cola de forma síncrona.

        Args:
            timeout: Tiempo máximo de espera en segundos para recibir un mensaje

        Returns:
            El mensaje procesado por message_factory o None si no hay mensajes
        """
        if not self._connection or self._connection.is_closed:
            if not self.connect():
                return None

        try:
            method_frame, properties, body = self._channel.basic_get(
                queue=f'{self._queue_name}_{self._consumer_id}',
                auto_ack=True
            )

            if method_frame:
                try:
                    processed_message = self._message_factory(body)

                    if isinstance(processed_message, dict) and self._handle_shutdown_message(processed_message):
                        logger.info(f" [x] Consumidor {self._consumer_id} recibió mensaje de shutdown")
                        return None

                    logger.info(f" [x] Consumidor {self._consumer_id} recibió: {processed_message}")
                    return processed_message
                except Exception as e:
                    logger.error(f"Error procesando mensaje: {e}")
                    return None
            return None

        except Exception as e:
            logger.error(f"Error al obtener mensaje: {e}")
            return None

    def close(self):
        """
        Cierra la conexión con RabbitMQ de forma ordenada.
        """
        logger.info('Iniciando cierre del consumidor...')
        self._closing = True

        if self._connection and not self._connection.is_closed:
            try:
                self._connection.close()
                logger.info('Conexión cerrada exitosamente')
            except Exception as e:
                logger.error(f"Error al cerrar la conexión: {e}")
        else:
            logger.info('No había conexión activa para cerrar')


if __name__ == '__main__':
    def json_message_factory(message: bytes) -> dict:
        import json
        return json.loads(message.decode('utf-8'))

    consumer = Consumer(
        queue_name='hello',
        message_factory=json_message_factory
    )

    try:
        while True:
            message = consumer.dequeue()
            if message:
                print(f"Mensaje recibido: {message}")
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info('Interrumpido por el usuario')
    finally:
        consumer.close()
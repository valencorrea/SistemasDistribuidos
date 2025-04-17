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

    def start(self):
        while not self._closing:
            try:
                if not self.connect():
                    continue
                
                def callback(ch, method, properties, body):
                    if self._closing:
                        return
                    try:
                        processed_message = self._message_factory(body)
                        
                        if isinstance(processed_message, dict) and self._handle_shutdown_message(processed_message):
                            return
                            
                        logger.info(f" [x] Consumidor {self._consumer_id} recibio: {processed_message}")
                    except Exception as e:
                        logger.error(f"Error procesando mensaje: {e}")

                self._channel.basic_consume(
                    queue=f'{self._queue_name}_{self._consumer_id}',
                    on_message_callback=callback,
                    auto_ack=True
                )

                logger.info(f' [*] Consumidor {self._consumer_id} esperando mensajes. Para salir presione CTRL+C')
                self._channel.start_consuming()
            except pika.exceptions.AMQPConnectionError:
                logger.error("Conexion perdida, intentando reconectar...")
                continue
            except KeyboardInterrupt:
                logger.info('Interrumpido por el usuario')
                break
            except Exception as e:
                logger.error(f"Error inesperado: {e}")
                break
            finally:
                if self._connection and not self._connection.is_closed:
                    self._connection.close()

if __name__ == '__main__':
    def json_message_factory(message: bytes) -> dict:
        import json
        return json.loads(message.decode('utf-8'))

    consumer = Consumer(
        queue_name='hello',
        message_factory=json_message_factory
    )
    consumer.start()
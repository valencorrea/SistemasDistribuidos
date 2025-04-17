#!/usr/bin/env python3
import pika
import signal
import sys
import time
import logging
import json
from typing import Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Producer:
    def __init__(self, queue_name: str = 'hello'):
        self._connection = None
        self._channel = None
        self._closing = False
        self._queue_name = queue_name
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logger.info('SIGTERM recibido - Iniciando graceful shutdown')
        self._closing = True
        if self._connection:
            self._notify_shutdown()
            self._connection.close()

    def connect(self):
        while not self._closing:
            try:
                self._connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host='rabbitmq',
                        connection_attempts=3,
                        retry_delay=5,
                        heartbeat=30
                    ))
                self._channel = self._connection.channel()
                
                self._channel.exchange_declare(
                    exchange='fanout_exchange',
                    exchange_type='fanout',
                    durable=True
                )
                
                logger.info("Conexion establecida exitosamente con RabbitMQ")
                return True
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"Error de conexion: {e}")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Error inesperado: {e}")
                return False
        return False

    def enqueue(self, message: Any):
        if not self._connection or self._connection.is_closed:
            if not self.connect():
                return False

        try:
            # Publicar en el exchange fanout en lugar de una cola específica
            self._channel.basic_publish(
                exchange='fanout_exchange',
                routing_key='',  # En fanout, el routing key se ignora
                body=json.dumps(message).encode(),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Hacer los mensajes persistentes
                )
            )
            logger.info(f" [x] Encolado: {message}")
            return True
        except Exception as e:
            logger.error(f"Error al encolar mensaje: {e}")
            return False

    def _notify_shutdown(self):
        """Notifica a los consumidores que el productor se está cerrando"""
        shutdown_message = {
            "type": "shutdown",
            "timestamp": time.time(),
            "message": "Producer se está cerrando"
        }
        self.enqueue(shutdown_message)
        logger.info("Notificación de shutdown enviada a los consumers")

    def close(self):
        if self._connection and not self._connection.is_closed:
            self._notify_shutdown()
            self._connection.close()

if __name__ == '__main__':
    producer = Producer()
    
    try:
        if not producer.connect():
            sys.exit(1)
            
        # Enviar un mensaje de prueba
        producer.enqueue({"mensaje": "Hola mundo"})
        
        # Enviar algunos mensajes numerados
        for i in range(5):
            producer.enqueue({
                "numero": i,
                "texto": f"Mensaje {i}"
            })
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info('Interrumpido por el usuario')
    finally:
        producer.close()
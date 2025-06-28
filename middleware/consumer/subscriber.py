import datetime
import json
import logging
import threading
import time
from typing import Optional, Callable, Any

import pika

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')

class Subscriber(threading.Thread):
    def __init__(self, exchange_name: str, message_handler: Optional[Callable[[dict], Any]] = None, 
                 subscriber_name: Optional[str] = None, max_retries: int = 5, retry_delay: float = 1.0):
        super().__init__()
        self.message_handler = message_handler
        self.exchange_name = exchange_name
        self.subscriber_name = subscriber_name or f"subscriber_{exchange_name}"
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection = None
        self.channel = None
        self.queue_name = None
        self.running = False
        
        # Configurar conexión inicial
        self._setup_connection()

    def _setup_connection(self):
        """Configura la conexión con RabbitMQ con reintentos automáticos"""
        retries = 0
        while retries < self.max_retries:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
                self.channel = self.connection.channel()
                self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='fanout')
                result = self.channel.queue_declare(queue='', exclusive=True)
                self.queue_name = result.method.queue
                self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name)
                
                logger.info(f"✅ {self.subscriber_name} conectado al exchange fanout: {self.exchange_name}")
                return
                
            except Exception as e:
                retries += 1
                logger.warning(f"⚠️ Intento {retries}/{self.max_retries} falló para {self.subscriber_name}: {e}")
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"❌ No se pudo conectar {self.subscriber_name} después de {self.max_retries} intentos")
                    raise

    def _reconnect(self):
        """Reconecta al exchange si se pierde la conexión"""
        logger.info(f"🔄 {self.subscriber_name} intentando reconectar...")
        try:
            self.close()
            time.sleep(self.retry_delay)
            self._setup_connection()
            logger.info(f"✅ {self.subscriber_name} reconectado exitosamente")
            return True
        except Exception as e:
            logger.error(f"❌ {self.subscriber_name} falló al reconectar: {e}")
            return False

    def _on_message(self, channel, method, properties, body):
        try:
            message = json.loads(body)
            if self.message_handler:
                self.message_handler(message)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error on queue consumer {self.exchange_name}: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception as e:
            logger.error(f"❌ Error processing message on queue consumer {self.exchange_name}: {e}. body: {body}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def close(self):
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.stop_consuming()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info("Connection closed successfully")
        except Exception as e:
            logger.error(f"Error cerrando conexión de {self.subscriber_name}: {e}")

    def stop(self):
        """Detiene el subscriber de forma segura"""
        self.running = False
        self.close()

    def run(self):
        """Ejecuta el subscriber con reconexión automática"""
        self.running = True
        logger.info(f"🟢 Iniciando {self.subscriber_name} en queue '{self.queue_name}'")
        
        while self.running:
            try:
                if not self.connection or self.connection.is_closed:
                    if not self._reconnect():
                        logger.error(f"❌ {self.subscriber_name} no pudo reconectar, terminando...")
                        break
                
                self.channel.basic_consume(
                    queue=self.queue_name, 
                    on_message_callback=self._on_message, 
                    auto_ack=False
                )
                self.channel.start_consuming()
                
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"⚠️ {self.subscriber_name} perdió conexión: {e}")
                if not self.running:
                    break
                time.sleep(self.retry_delay)
                
            except Exception as e:
                logger.error(f"❌ Error inesperado en {self.subscriber_name}: {e}")
                if not self.running:
                    break
                time.sleep(self.retry_delay)
        
        logger.info(f"🔴 {self.subscriber_name} detenido")

def get_timestamp():
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
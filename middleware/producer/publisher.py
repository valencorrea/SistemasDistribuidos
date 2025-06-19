import json
import logging

import pika

logger = logging.getLogger(__name__)

class Publisher:
    def __init__(self, exchange_name: str = 'default'):
        self.exchange_name = exchange_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='fanout')

    def getname(self):
        return self.exchange_name

    def enqueue(self, message):
        logger.debug(f"Enviando mensaje por cola fanout {self.exchange_name}")
        self.channel.basic_publish(exchange=self.exchange_name, routing_key='', body=json.dumps(message).encode())
        return True

    def close(self):
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.debug("✅ Conexión cerrada correctamente")
        except Exception as e:
            logger.error(f"❌ Error al cerrar conexión: {e}")

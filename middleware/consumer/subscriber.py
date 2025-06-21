import datetime
import json
import logging
import threading
from typing import Optional, Callable, Any

import pika

logger = logging.getLogger(__name__)

class Subscriber(threading.Thread):
    def __init__(self, exchange_name: str, message_handler: Optional[Callable[[dict], Any]] = None, 
                 durable: bool = False, auto_ack: bool = True):
        super().__init__()
        self.message_handler = message_handler
        self.exchange_name = exchange_name
        self.durable = durable
        self.auto_ack = auto_ack
        
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        
        # ✅ SOLUCIÓN: Declarar exchange sin cambiar configuración existente
        try:
            self.channel.exchange_declare(
                exchange=exchange_name, 
                exchange_type='fanout',
                durable=durable,
                passive=False  # Intentar crear si no existe
            )
        except pika.exceptions.ChannelClosedByBroker as e:
            if "PRECONDITION_FAILED" in str(e) and "durable" in str(e):
                # ✅ SOLUCIÓN: Si el exchange ya existe con diferente configuración, usar passive=True
                logger.warning(f"⚠️ Exchange {exchange_name} ya existe con diferente configuración. Usando configuración existente.")
                self.channel.exchange_declare(
                    exchange=exchange_name, 
                    exchange_type='fanout',
                    passive=True  # Solo verificar que existe, no cambiar configuración
                )
            else:
                raise e
        
        # ✅ PARAMETRIZABLE: Cola durable y no exclusiva solo si se especifica
        if durable:
            result = self.channel.queue_declare(queue='', durable=True, auto_delete=False)
        else:
            result = self.channel.queue_declare(queue='', exclusive=True)
            
        self.queue_name = result.method.queue
        self.channel.queue_bind(exchange=exchange_name, queue=self.queue_name)

        logger.info(f"✅ Connected to fanout queue: {self.exchange_name} (durable={durable}, auto_ack={auto_ack})")

    def _on_message(self, channel, method, properties, body):
        try:
            timestamp = get_timestamp()
            logger.debug(f"📥 Message received. Subscriber {self.queue_name} Timestamp: {timestamp}")
            message = json.loads(body)
            
            # ✅ PARAMETRIZABLE: Procesar mensaje
            self.message_handler(message)
            
            # ✅ PARAMETRIZABLE: ACK manual solo si no es auto_ack
            if not self.auto_ack:
                channel.basic_ack(delivery_tag=method.delivery_tag)
                logger.debug(f"📥 Message acked. Queue {self.queue_name} Timestamp: {timestamp}")
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error on queue consumer {self.exchange_name}: {e}")
            if not self.auto_ack:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error(f"❌ Error processing message on queue consumer {self.exchange_name}: {e}. body: {body}")
            if not self.auto_ack:
                # ✅ PARAMETRIZABLE: Requeue en caso de error solo si no es auto_ack
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def close(self):
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info("✅ Connection closed successfully")
        except Exception as e:
            logger.error(f"❌ Error closing connection: {e}")

    def run(self):
        logger.info(f"🟢 Starting fanout consumer '{self.queue_name}'")
        # ✅ PARAMETRIZABLE: auto_ack según configuración
        self.channel.basic_consume(
            queue=self.queue_name, 
            on_message_callback=self._on_message, 
            auto_ack=self.auto_ack
        )
        self.channel.start_consuming()

def get_timestamp():
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%dT%H:%M:%S') + ('-%02d' % (now.microsecond / 10000))
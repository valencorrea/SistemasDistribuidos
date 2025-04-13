import pika
import json
import signal
import sys
from abc import ABC, abstractmethod

class BaseWorker(ABC):
    def __init__(self, queue_name, host='rabbitmq'):
        self._queue_name = queue_name
        self._host = host
        self._connection = None
        self._channel = None
        self._closing = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        print('SIGTERM recibido - Iniciando graceful shutdown')
        self._closing = True
        if self._connection:
            self._connection.close()

    def _connect(self):
        if not self._connection or self._connection.is_closed:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self._host)
            )
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self._queue_name, durable=True)
            self._channel.basic_qos(prefetch_count=1)

    def start(self):
        try:
            self._connect()
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=self._process_message
            )
            print(f'Worker iniciado. Esperando mensajes en cola {self._queue_name}')
            self._channel.start_consuming()
        except KeyboardInterrupt:
            print('Interrumpido por el usuario')
        finally:
            if self._connection and not self._connection.is_closed:
                self._connection.close()

    def _process_message(self, ch, method, properties, body):
        if self._closing:
            return
        
        try:
            message = json.loads(body)
            result = self.callback(message)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            if result and properties.reply_to:
                self._channel.basic_publish(
                    exchange='',
                    routing_key=properties.reply_to,
                    body=json.dumps(result),
                    properties=pika.BasicProperties(
                        correlation_id=properties.correlation_id
                    )
                )
        except Exception as e:
            print(f"Error procesando mensaje: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag)

    @abstractmethod
    def callback(self, message):
        pass 
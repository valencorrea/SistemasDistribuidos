import pika
import json
import uuid

class BasePublisher:
    def __init__(self, host='rabbitmq'):
        self._host = host
        self._connection = None
        self._channel = None
        self._response = None
        self._corr_id = None
        
    def _connect(self):
        if not self._connection or self._connection.is_closed:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self._host)
            )
            self._channel = self._connection.channel()
            
    def setup_queue(self, queue_name):
        self._connect()
        self._channel.queue_declare(queue=queue_name, durable=True)
        
    def publish(self, queue_name, message, wait_response=True):
        self._connect()
        
        if wait_response:
            callback_queue = self._channel.queue_declare(queue='', exclusive=True).method.queue
            self._channel.basic_consume(
                queue=callback_queue,
                on_message_callback=self._on_response,
                auto_ack=True
            )
            
            self._response = None
            self._corr_id = str(uuid.uuid4())
            
            self._channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                properties=pika.BasicProperties(
                    reply_to=callback_queue,
                    correlation_id=self._corr_id,
                    delivery_mode=2,  # mensaje persistente
                ),
                body=json.dumps(message)
            )
            
            while self._response is None:
                self._connection.process_data_events(time_limit=30)
            return json.loads(self._response)
        else:
            self._channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # mensaje persistente
                ),
                body=json.dumps(message)
            )
    
    def _on_response(self, ch, method, props, body):
        if self._corr_id == props.correlation_id:
            self._response = body
            
    def close(self):
        if self._connection and not self._connection.is_closed:
            self._connection.close() 
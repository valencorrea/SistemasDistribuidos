import logging
import signal
import threading
import time
import os
from abc import ABC, abstractmethod
from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from middleware.heartbeat.heartbeat_sender import HeartbeatSender

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')

class Worker(ABC):
    def __init__(self):
        logging.basicConfig(
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=logging.INFO,
            datefmt='%H:%M:%S')
        logging.getLogger("pika").setLevel(logging.WARNING)
        self.shutdown_event = threading.Event()
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # Inicializar el heartbeat sender
        service_name = os.getenv('SERVICE_NAME', self.__class__.__name__.lower())
        self.heartbeat_sender = HeartbeatSender(service_name)
        self.heartbeat_sender.start()  # Iniciar el heartbeat sender
        
        if not self.wait_for_rabbitmq():
            logger.error("Error al intentar conectar con rabbitMQ. No se va a iniciar el worker")
            self._close()
            return
            
        self.shutdown_consumer = Consumer(
            queue_name="shutdown",
            _message_handler=self.handle_shutdown,
            queue_type="fanout"
        )
        logger.info("Se va a escuchar por mensajes de shutdown")
        self.shutdown_thread = threading.Thread(target=self.shutdown_consumer.start_consuming)
        self.shutdown_thread.start()
        logger.info("Escuchando por mensaje de shutdown...")

    def signal_handler(self, signum, frame):
        logger.info(f"Señal del sistema recibida ({signum}). Cerrando worker...")
        self._close()

    def handle_shutdown(self, message):
        logger.info("Mensaje de shutdown recibido.")
        self._close()

    def _close(self):
        self.shutdown_event.set()
        self.heartbeat_sender.stop()  # Detener el heartbeat sender
        self.close()

    @staticmethod
    def wait_for_rabbitmq(max_retries: int = 10, retry_interval: float = 10.0) -> bool:
        for i in range(max_retries):
            time.sleep(retry_interval)
            try:
                logger.info(f"Intento {i + 1} de {max_retries} de conectar a RabbitMQ...")
                producer = Producer("test")
                if producer.connect():
                    logger.info(f"Conectado a RabbitMQ")
                    producer.close()
                    return True
            except Exception:
                pass
        return False

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def close(self):
        pass

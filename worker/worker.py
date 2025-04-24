import logging
import signal
import threading
from abc import ABC, abstractmethod
from middleware.consumer.consumer import Consumer

logger = logging.getLogger(__name__)

class Worker(ABC):
    def __init__(self):
        self.shutdown_event = threading.Event()
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.shutdown_consumer = Consumer(
            queue_name="shutdown",
            message_factory=self.handle_shutdown,
            queue_type="fanout"
        )
        threading.Thread(
            target=self.listen_for_shutdown,
            args=(self.shutdown_consumer,),
            daemon=True
        ).start()

    def signal_handler(self, signum, frame):
        logger.info(f"Señal del sistema recibida ({signum}). Cerrando worker...")
        self.shutdown_event.set()

    def handle_shutdown(self, message):
        logger.info("Mensaje de shutdown recibido.")
        self.shutdown_event.set()
        return message

    def listen_for_shutdown(self, shutdown_consumer):
        logger.info("Escuchando por mensaje de shutdown.")
        while not self.shutdown_event.is_set():
            shutdown_consumer.dequeue()

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def close(self):
        pass

import json
import logging
import threading
import signal

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TwentiethCenturyArgProductionFilter:
    def __init__(self):
        self.consumer = Consumer("movie", message_factory=self.handle_message)
        self.esp_production_producer = Producer("arg_españa_production")
        self.rating_joiner_producer = Producer("rating_joiner")
        self.partial_aggregator_producer = Producer("credits_joiner")
        self.shutdown_consumer = Consumer(
            queue_name="shutdown",
            message_factory=self._handle_shutdown,
            queue_type="fanout"
        )

        # 🔁 Evento de shutdown para cortar loops
        self.shutdown_event = threading.Event()

        # 🛑 Manejo de señales del sistema directamente desde esta clase
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info(f"Señal del sistema recibida ({signum}). Cerrando worker...")
        self.shutdown_event.set()

    def _handle_shutdown(self, message):
        logger.info("Mensaje de shutdown recibido desde la cola shutdown.")
        self.shutdown_event.set()
        return message

    def handle_message(self, message):
        if message.get("type") == "shutdown":
            self.shutdown_event.set()
            return message

        movies = convert_data(message)
        filtered_movies = apply_filter(movies)

        return {
            "movies": [movie.to_dict() for movie in filtered_movies],
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result"
        }

    def start(self):
        logger.info("Iniciando filtro de películas del siglo XXI")

        try:
            while not self.shutdown_event.is_set():
                message = self.consumer.dequeue()
                if isinstance(message, dict) and message.get("type") == "shutdown":
                    logger.info("Shutdown recibido desde la cola movie")
                    self.shutdown_event.set()
                    break

                if not message:
                    continue

                self.esp_production_producer.enqueue(message)
                self.partial_aggregator_producer.enqueue(message)
                self.rating_joiner_producer.enqueue(message)

        except Exception as e:
            logger.error(f"Error durante el procesamiento: {e}")
        finally:
            self.close()

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.esp_production_producer.close()
            self.partial_aggregator_producer.close()
            self.rating_joiner_producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

def apply_filter(movies):
    return [movie for movie in movies if movie.released_in_or_after_2000_argentina()]

if __name__ == '__main__':
    filter = TwentiethCenturyArgProductionFilter()
    filter.start()

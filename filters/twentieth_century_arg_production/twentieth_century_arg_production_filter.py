import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data
from worker.worker import Worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TwentiethCenturyArgProductionFilter(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("movie", message_factory=self.handle_message)
        self.esp_production_producer = Producer("arg_españa_production")
        self.rating_joiner_producer = Producer("rating_joiner")
        self.partial_aggregator_producer = Producer("credits_joiner")

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

    def start(self):
        logger.info("Iniciando filtro de películas del siglo XXI")

        try:
            while not self.shutdown_event.is_set():
                message = self.consumer.dequeue()
                if not message:
                    continue
                self.esp_production_producer.enqueue(message)
                self.partial_aggregator_producer.enqueue(message)
                self.rating_joiner_producer.enqueue(message)

        except Exception as e:
            logger.error(f"Error durante el procesamiento: {e}")
        finally:
            self.close()

    def handle_message(self, message):

        movies = convert_data(message)
        filtered_movies = self.apply_filter(movies)

        return {
            "movies": [movie.to_dict() for movie in filtered_movies],
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result"
        }

    @staticmethod
    def apply_filter(movies):
        return [movie for movie in movies if movie.released_in_or_after_2000_argentina()]

if __name__ == '__main__':
    worker = TwentiethCenturyArgProductionFilter()
    worker.start()

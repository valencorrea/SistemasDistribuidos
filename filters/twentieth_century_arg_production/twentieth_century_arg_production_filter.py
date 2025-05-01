import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data
from worker.worker import Worker


logger = logging.getLogger(__name__)


class TwentiethCenturyArgProductionFilter(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("movie", _message_handler=self.handle_message)
        self.esp_production_producer = Producer("arg_españa_production")
        self.partial_aggregator_producer = Producer("20_century_batch_results")

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.esp_production_producer.close()
            self.partial_aggregator_producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

    def start(self):
        logger.info("Iniciando filtro de películas del siglo XXI")
        self.consumer.start_consuming()

    def handle_message(self, message):

        movies = convert_data(message)
        filtered_movies = self.apply_filter(movies)

        result = {
            "movies": [movie.to_dict() for movie in filtered_movies],
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result"
        }

        self.esp_production_producer.enqueue(result)
        self.partial_aggregator_producer.enqueue(result)

    @staticmethod
    def apply_filter(movies):
        return [movie for movie in movies if movie.released_in_or_after_2000_argentina()]


if __name__ == '__main__':
    worker = TwentiethCenturyArgProductionFilter()
    worker.start()

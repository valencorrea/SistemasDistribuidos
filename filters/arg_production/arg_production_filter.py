import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data
from worker.worker import Worker


logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')


class ArgProductionFilter(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("twentieth_century", _message_handler=self.handle_message)
        self.esp_production_producer = Producer("arg_production")
        self.batch_results_producer = Producer("20_century_batch_results")
        self.rating_joiner_producer = Producer("rating_joiner")

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.esp_production_producer.close()
            self.batch_results_producer.close()
            self.rating_joiner_producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

    def start(self):
        logger.info("Iniciando filtro de películas de produccion argentina")
        self.consumer.start_consuming()

    def handle_message(self, message):

        movies = convert_data(message)
        filtered_movies = self.apply_filter(movies)

        result = {
            "movies": [movie.to_dict() for movie in filtered_movies],
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result",
            "client_id": message.get("client_id")
        }

        self.esp_production_producer.enqueue(result)
        self.batch_results_producer.enqueue(result)
        self.rating_joiner_producer.enqueue(result)

    @staticmethod
    def apply_filter(movies):
        return [movie for movie in movies if movie.angentinian_production()]


if __name__ == '__main__':
    worker = ArgProductionFilter()
    worker.start()

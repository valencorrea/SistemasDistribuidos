import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import parse_genres
from worker.worker import Worker


logger = logging.getLogger(__name__)


class ArgEspProductionFilter(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer(queue_name="arg_españa_production",  _message_handler=self.handle_message)
        self.producer = Producer("aggregate_consulta_1")

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

    def handle_message(self, message):
        filtered_movies = self.apply_filter(message.get("movies"))
        batch_message = {
            "movies": filtered_movies,
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result",
            "client_id": message.get("client_id")
        }
        self.producer.enqueue(batch_message)

    def start(self):
        logger.info("Iniciando filtro de películas españolas")
        try:
            self.consumer.start_consuming()
        finally:
            self.close()

    @staticmethod
    def apply_filter(movies):
        result = []
        for movie in movies:
            if int(movie.get("release_date")) < 2010 and "ES" in movie.get("production_countries"):
                result.append({"title": movie.get("title"), "genres": parse_genres(movie.get("genres"))})
        return result


if __name__ == '__main__':
    worker = ArgEspProductionFilter()
    worker.start()

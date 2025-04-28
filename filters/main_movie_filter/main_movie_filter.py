import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data_for_main_movie_filter
from worker.worker import Worker


logger = logging.getLogger(__name__)


class MainMovieFilter(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("movie_main_filter", _message_handler=self.handle_message)  # Lee de la cola de movies
        self.movie_producer = Producer("movie")
        self.movie_2_producer = Producer("movie_2")
        self.movie_3_producer = Producer("movie_1")

    def close(self):
        try:
            self.consumer.close()
            self.movie_producer.close()
            self.movie_2_producer.close()
            self.movie_3_producer.close()
        except Exception as e:
            logger.error(f"Error al cerrar las conexiones: {e}")

    def handle_message(self, message):
        movies = convert_data_for_main_movie_filter(message)

        print(f"[MAIN] Peliculas: {len(movies)}")

        batch_message = {
            "movies": [movie.to_dict() for movie in movies],
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result"
        }

        self.movie_producer.enqueue(batch_message)
        self.movie_2_producer.enqueue(batch_message)
        self.movie_3_producer.enqueue(batch_message)

        return batch_message

    def start(self):
        logger.info("Iniciando filtro de entradas de error")
        try:
            self.consumer.start_consuming()
        finally:
            self.close()


if __name__ == '__main__':
    worker = MainMovieFilter()
    worker.start()

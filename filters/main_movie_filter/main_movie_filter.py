import json
import logging
from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data_for_main_movie_filter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MainMovieFilter:
    def __init__(self):
        self.consumer = Consumer("movie_main_filter",message_factory=self.handle_message)  # Lee de la cola de movies
        self.movie_producer = Producer("movie")
        self.movie_2_producer = Producer("movie_2")
        self.movie_3_producer = Producer("movie_1")

    def handle_message(self, message):
        if message.get("type") == "shutdown":
            return message
        movies = convert_data_for_main_movie_filter(message)

        print(f"[MAIN] Peliculas: {len(movies)}")
        # Crear un mensaje con la información del batch
        batch_message = {
            "movies": [movie.to_dict() for movie in movies],
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result"
        }
        
        return batch_message

    def start(self):
        logger.info("Iniciando filtro de películas del siglo XXI")
        
        try:
            while True:
                message = self.consumer.dequeue()
                if type(message) == dict and message.get("type") == "shutdown":
                    print("Shutting down filter")
                    break
                if not message:
                    continue
                self.movie_producer.enqueue(message)
                self.movie_2_producer.enqueue(message)
                self.movie_3_producer.enqueue(message)
        except KeyboardInterrupt:
            logger.info("Deteniendo filtro...")
        finally:
            self.close()

    def close(self):
        try:
            self.consumer.close()
            self.movie_producer.close()
            self.movie_2_producer.close()
            self.movie_3_producer.close()
        except Exception as e:
            logger.error(f"Error al cerrar las conexiones: {e}")

if __name__ == '__main__':
    filter = MainMovieFilter()
    filter.start()
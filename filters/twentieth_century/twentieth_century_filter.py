import json
import logging
from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TwentiethCenturyFilter:
    def __init__(self):
        self.consumer = Consumer("movie",message_factory=self.handle_message)  # Lee de la cola de movies
        self.producer = Producer("aggregate_consulta_1")  # Envía resultados a aggregate_consulta_1

    def handle_message(self, message):
        print("message_bytes" + str(message))
        print("batch" + str(message))
        movies = convert_data(message)
        print("HOLAAAAAAAAAA" + str(message))

        filtered_movies = apply_filter(movies)
        for movie in filtered_movies:
            self.movies_filtered.append(movie)

        return movies

    def start(self):
        """Inicia el procesamiento de películas"""
        logger.info("Iniciando filtro de películas del siglo XXI")
        
        try:
            while True:
                movies = self.consumer.dequeue()
                if not movies or len(movies) == 0:
                    continue
                message = [movie.to_dict() for movie in movies]
                self.producer.enqueue(message)
        except KeyboardInterrupt:
            logger.info("Deteniendo filtro...")
        finally:
            self.close()

    def close(self):
        """Cierra las conexiones"""
        self.consumer.close()
        self.producer.close()
def apply_filter(movies):
    return [movie for movie in movies if movie.released_in_or_after_2000()]
if __name__ == '__main__':
    filter = TwentiethCenturyFilter()
    filter.start()
import logging
import threading

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data_for_second_filter
from worker.worker import Worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Top5LocalFilter(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("movie_1",message_factory=self.handle_message)  # Lee de la cola de movies
        self.producer = Producer("aggregate_consulta_2")  # Envía resultados a aggregate_consulta_1

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.error(f"Error al cerrar las conexiones: {e}")

    def handle_message(self, message):
        movies = convert_data_for_second_filter(message)
        filtered_movies = self.apply_filter(movies)

        batch_message = {
            "movies": filtered_movies,
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result"
        }

        return batch_message

    def start(self):
        logger.info("Iniciando filtro de películas top 5 local")

        shutdown_thread = threading.Thread(target=self.listen_for_shutdown, daemon=True)
        shutdown_thread.start()

        try:
            while not self.shutdown_event.is_set():
                message = self.consumer.dequeue()
                if not message:
                    continue
                self.producer.enqueue(message)
        except Exception as e:
            logger.error(f"Error durante el procesamiento: {e}")
        finally:
            self.close()

    @staticmethod
    def apply_filter(movies):
        result = []
        for movie in movies:
            if movie.get("production_countries") and len(movie.get("production_countries")) == 1:
                result.append({"country": movie.get("production_countries")[0], "budget": movie.get("budget")})
        return result

if __name__ == '__main__':
    worker = Top5LocalFilter()
    worker.start()
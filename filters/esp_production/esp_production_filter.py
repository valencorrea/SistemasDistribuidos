import logging
import threading

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ArgEspProductionFilter(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer(
            queue_name="arg_españa_production",
            message_factory=self.handle_message
        )
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
            "type": "batch_result"
        }
        return batch_message

    def start(self):
        logger.info("Iniciando filtro de películas del siglo XXI")
        shutdown_thread = threading.Thread(target=self.listen_for_shutdown, daemon=True)
        shutdown_thread.start()

        try:
            while not self.shutdown_event.is_set():
                message = self.consumer.dequeue()
                if not message:
                    continue
                self.producer.enqueue(message)
        except KeyboardInterrupt:
            logger.info("[CONSUMER_CLIENT] Interrumpido por el usuario")
        finally:
            self.close()

    @staticmethod
    def apply_filter(movies):
        result = []
        for movie in movies:
            if int(movie.get("release_date")) < 2010 and "ES" in movie.get("production_countries"):
                result.append({"title": movie.get("title"), "genres": movie.get("genres")})
        return result

if __name__ == '__main__':
    worker = ArgEspProductionFilter()
    worker.start()
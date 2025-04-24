import time
import json
import logging
from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ArgEspProductionFilter:
    def __init__(self):
        self.consumer = Consumer(
            queue_name="arg_españa_production",
            message_factory=self.handle_message
        )
        self.producer = Producer("aggregate_consulta_1")
        self.shutdown_consumer= Consumer(
            queue_name="shutdown",
            message_factory=self.close,
            type="fanout"
        )

    def close(self):
        self.consumer.close()
        self.producer.close()
        self.shutdown_consumer.close()

    def handle_message(self, message):
        if message.get("type") == "shutdown":
            return message


        filtered_movies = apply_filter(message.get("movies"))

        batch_message = {
            "movies": filtered_movies,
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result"
        }
        
        return batch_message

    def start(self):
        """Inicia el procesamiento de películas"""
        logger.info("Iniciando filtro de películas del siglo XXI")
        
        try:
            while True:
                message = self.consumer.dequeue()
                if type(message) == dict and message.get("type") == "shutdown":
                    print("Shutting down filter")
                    break
                if not message:
                    continue
                self.producer.enqueue(message)
        except KeyboardInterrupt:
            logger.info("[CONSUMER_CLIENT] Interrumpido por el usuario")
        finally:
            self.close()

def apply_filter(movies):
    result = []
    for movie in movies:
        if int(movie.get("release_date")) < 2010 and "ES" in movie.get("production_countries"):
            result.append({"title": movie.get("title"), "genres": movie.get("genres")})
    return result

if __name__ == '__main__':
    filter = ArgEspProductionFilter()
    filter.start()
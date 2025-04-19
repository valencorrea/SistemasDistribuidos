import time
import json
from venv import logger

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data


class ArgEspProductionFilter:
    def __init__(self):
        self.consumer = Consumer(
            queue_name="arg_españa_production",
            message_factory=self.handle_message
        )
        self.producer = Producer("aggregate_consulta_1")

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
            logger.info("Deteniendo filtro...")
        finally:
            self.close()

    def close(self):
        """Cierra las conexiones"""
        self.consumer.close()
        self.producer.close()

def apply_filter(movies):
    return [movie for movie in movies if len(movie.get("production_countries")) == 2 and "ES" in movie.get("production_countries")]

if __name__ == '__main__':
    filter = ArgEspProductionFilter()
    filter.start()
import json
import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data
from utils.parsers.ratings_parser import convert_data_for_rating_joiner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RatingsJoiner:
    def __init__(self):
        self.partial_aggregator_consumer = Consumer("rating_joiner", message_factory=self.handle_partial_aggregator_message)  # Lee de la cola de movies
        self.ratings_consumer = Consumer("credits", message_factory=self.handle_ratings_message)  # Lee de la cola de movies
        self.ratings_producer = Producer("result")

        self.movies_ratings = {}
        self.receive_movie_batches = 0
        self.total_movie_batches = None
        self.receive_ratings_batches = 0
        self.total_ratings_batches = None
        #self.partial_aggregator_producer = Producer("aggregator_4")

    def handle_ratings_message(self, message):
        if message.get("type") == "shutdown":
            return message

        ratings = convert_data_for_rating_joiner(message)

        # Crear un mensaje con la información del batch
        batch_message = {
            "ratings": ratings,
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result"
        }

        return batch_message


    def handle_partial_aggregator_message(self, message):
        if message.get("type") == "shutdown":
            return message

        # Crear un mensaje con la información del batch
        batch_message = {
            "movies": message.get("movies", []),
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
                message = self.partial_aggregator_consumer.dequeue()
                if not message:
                    continue

                if type(message) == dict and message.get("type") == "shutdown":
                    break

                if message.get("type") == "batch_result":
                    
                    for movie in message.get("movies", []):
                        self.movies_ratings[movie["id"]] = {"title":movie["title"], "rating":0, "votes":0}

                    self.receive_movie_batches += message.get("batch_size", 0)

                if message.get("total_batches"):
                    self.total_movie_batches = message.get("total_batches")
                    # Si hemos recibido todos los batches, enviar el resultado final

                if self.total_movie_batches and self.total_movie_batches > 0 and self.receive_movie_batches >= self.total_movie_batches:
                    break


            while True:
                message = self.ratings_consumer.dequeue()
                if not message:
                    continue

                if type(message) == dict and message.get("type") == "shutdown":
                    break

                if message.get("type") == "actor":
                    ratings = (message.get("ratings", []))

                    for rating in ratings:
                        if self.movies_ratings.get(rating["movieId"]):
                            self.movies_ratings[rating["movieId"]]["rating"] += rating["rating"]
                            self.movies_ratings[rating["movieId"]]["votes"] += 1
                self.receive_ratings_batches += message.get("batch_size", 0)

                if message.get("total_batches"):
                    self.total_ratings_batches = message.get("total_batches")
                    # Si hemos recibido todos los batches, enviar el resultado final
                if self.total_ratings_batches and self.total_ratings_batches > 0 and self.receive_ratings_batches >= self.total_ratings_batches:
                    break
            self.ratings_producer.produce(self.obtain_result())

        except KeyboardInterrupt:
            logger.info("Deteniendo filtro...")
        finally:
            self.close()

    def obtain_result(self):
        result = []
        for id in self.movies_ratings.keys():
            rating = self.movies_ratings[id]["rating"] / self.movies_ratings[id]["votes"]
            title = self.movies_ratings[id]["title"]
            result.append((id, title, rating))
        return result

    def close(self):
        """Cierra las conexiones"""
        self.partial_aggregator_consumer.close()
        self.ratings_consumer.close()
        self.ratings_producer.close()

if __name__ == '__main__':
    filter = RatingsJoiner()
    filter.start()
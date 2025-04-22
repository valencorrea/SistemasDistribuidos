import json
import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.credits_parser import convert_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CreditsJoiner:
    def __init__(self):
        self.movies_consumer = Consumer("credits_joiner", message_factory=self.handle_partial_aggregator_message)  # Lee de la cola de movies
        self.credits_consumer = Consumer("credits", message_factory=self.handle_credits_message)  # Lee de la cola de movies
        self.movies = [] # se usa?
        self.actors = []
        self.movie_ids = set()
        self.receive_movie_batches = 0
        self.total_movie_batches = None
        self.actor_counts = {}
        #self.partial_aggregator_producer = Producer("aggregator_4")

    def handle_credits_message(self, message):
        '''if message.get("type") == "shutdown":
            return message
        movies = convert_data(message)

        # Crear un mensaje con la información del batch
        batch_message = {
            "movies": [movie.to_dict() for movie in movies],
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result"
        }

        return batch_message'''
        return message


    def handle_partial_aggregator_message(self, message):
        '''if message.get("type") == "shutdown":
            return message
        movies = convert_data(message)

        # Crear un mensaje con la información del batch
        batch_message = {
            "movies": [movie.to_dict() for movie in movies],
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result"
        }

        return batch_message'''
        return message

    def start(self):
        """Inicia el procesamiento de películas"""
        logger.info("Iniciando filtro de películas del siglo XXI")
        
        try:
            while True:
                message = self.movies_consumer.dequeue()
                if not message:
                    continue

                if type(message) == dict and message.get("type") == "shutdown":
                    break

                if message.get("type") == "batch_result":
                    # Acumular las películas del batch
                    self.movies.extend(message.get("movies", []))
                    for movie in message.get("movies", []):
                        self.movie_ids.add(movie["id"])

                    self.receive_movie_batches += message.get("batch_size", 0)

                if message.get("total_batches"):
                    self.total_movie_batches = message.get("total_batches")

                if self.total_movie_batches and self.total_movie_batches > 0 and self.receive_movie_batches >= self.total_movie_batches:
                    logger.info("MOVIES RECEIVED: " + str(self.movie_ids))
                    break

            while True:
                message = self.credits_consumer.dequeue()
                if not message:
                    continue

                if type(message) == dict and message.get("type") == "shutdown":
                    break

                if message.get("type") == "actor":
                    actors = convert_data(message)
                    for actor in actors:
                        if actor.id not in self.actor_counts:
                            self.actor_counts[actor.id] = {
                                "name": actor.name,
                                "count": 1
                            }
                        else:
                            self.actor_counts[actor.id]["count"] += 1

                self.receive_movie_batches += message.get("batch_size", 0)

                if message.get("total_batches"):
                    self.total_movie_batches = message.get("total_batches")
                    # Si hemos recibido todos los batches, enviar el resultado final
                if self.total_movie_batches and self.total_movie_batches > 0 and self.receive_movie_batches >= self.total_movie_batches:
                    top_10 = sorted(self.actor_counts.items(), key=lambda item: item[1]["count"], reverse=True)[:10]

                    # Devolvemos lista con (id, nombre, cantidad)
                    logger.info("TOP 10 ACTORS: " + str([(info["name"], info["count"]) for actor_id, info in top_10]))


            # Enviar el mensaje a la cola de producción
        except KeyboardInterrupt:
            logger.info("Deteniendo filtro...")
        finally:
            self.close()

    def close(self):
        """Cierra las conexiones"""
        self.movies_consumer.close()
        self.credits_consumer.close()


def apply_filter(movies):
    return [movie for movie in movies if movie.released_in_or_after_2000_argentina()]

if __name__ == '__main__':
    filter = CreditsJoiner()
    filter.start()
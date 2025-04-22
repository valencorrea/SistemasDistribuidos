import json
import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.credits_parser import convert_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CreditsJoiner:
    def __init__(self):
        self.movies_consumer = Consumer("credits_joiner", message_factory=self.handle_partial_aggregator_message)
        self.credits_consumer = Consumer("credits", message_factory=self.handle_credits_message)
        self.producer = Producer("result")
        self.movies = [] # se usa?
        self.actors = []
        self.movie_ids = set()
        self.receive_movie_batches = 0
        self.total_movie_batches = None
        self.actor_counts = {}
        #self.partial_aggregator_producer = Producer("aggregator_4")

    def handle_credits_message(self, message):
        return message


    def handle_partial_aggregator_message(self, message):
        return message

    def start(self):
        try:
            while True:
                message = self.movies_consumer.dequeue()
                if not message:
                    continue

                if type(message) == dict and message.get("type") == "shutdown":
                    break

                if message.get("type") == "batch_result":
                    self.movies.extend(message.get("movies", []))
                    for movie in message.get("movies", []):
                        self.movie_ids.add(movie["id"])

                    self.receive_movie_batches += message.get("batch_size", 0)

                if message.get("total_batches"):
                    self.total_movie_batches = message.get("total_batches")

                if self.total_movie_batches and self.total_movie_batches > 0 and self.receive_movie_batches >= self.total_movie_batches:
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

                if self.total_movie_batches and self.total_movie_batches > 0 and self.receive_movie_batches >= self.total_movie_batches:
                    top_10 = sorted(self.actor_counts.items(), key=lambda item: item[1]["count"], reverse=True)[:10]

                    logger.info("TOP 10 ACTORS: " + str([(info["name"], info["count"]) for actor_id, info in top_10]))

                    result_message = {
                        "type": "result",
                        "actors": top_10,
                    }
                    if self.producer.enqueue(result_message):
                        logger.info(f"Resultado final enviado.")
                    break


        except KeyboardInterrupt:
            logger.info("Deteniendo filtro...")
        finally:
            self.close()

    def close(self):
        self.movies_consumer.close()
        self.credits_consumer.close()
        self.producer.close()

if __name__ == '__main__':
    filter = CreditsJoiner()
    filter.start()
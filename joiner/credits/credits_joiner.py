import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.credits_parser import convert_data
from worker.worker import Worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CreditsJoiner(Worker):
    def __init__(self):
        super().__init__()
        self.movies_consumer = Consumer("credits_joiner", message_factory=self.handle_partial_aggregator_message)
        self.credits_consumer = Consumer("credits", message_factory=self.handle_credits_message)
        self.producer = Producer("result")

        self.movie_ids = set()
        self.actor_counts = {}

        self.receive_movie_batches = 0
        self.receive_credits_batches = 0
        self.total_movie_batches = None
        self.total_credits_batches = None

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.movies_consumer.close()
            self.credits_consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

    @staticmethod
    def handle_credits_message( message):
        return message

    @staticmethod
    def handle_partial_aggregator_message(message):
        return message

    def start(self):
        try:
            while not self.shutdown_event.is_set():
                message = self.movies_consumer.dequeue()
                if not message:
                    continue

                if message.get("type") == "batch_result":
                    self.process_movie_batch(message)

                if message.get("total_batches"):
                    self.total_movie_batches = message.get("total_batches")

                if self.total_movie_batches and 0 < self.total_movie_batches <= self.receive_movie_batches:
                    break

            while not self.shutdown_event.is_set():
                message = self.credits_consumer.dequeue()
                if not message:
                    continue

                if message.get("type") == "actor":
                    self.process_credits_batch(message)

                if message.get("total_batches"):
                    self.total_credits_batches = message.get("total_batches")

                if self.total_movie_batches and 0 < self.total_movie_batches <= self.receive_movie_batches:
                    self.answer_client()
                    break

        except Exception as e:
            logger.error(f"Error durante el procesamiento: {e}")
        finally:
            self.close()

    def process_movie_batch(self, message):
        for movie in message.get("movies", []):
            self.movie_ids.add(movie["id"])

        self.receive_movie_batches += message.get("batch_size", 0)

    def process_credits_batch(self, message):
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

    def answer_client(self):
        top_10 = sorted(self.actor_counts.items(), key=lambda item: item[1]["count"], reverse=True)[:10]
        logger.info("Top 10 actors: " + str([(info["name"], info["count"]) for actor_id, info in top_10]))

        result_message = {
            "type": "result",
            "actors": top_10,
        }

        print(top_10)
        if self.producer.enqueue(result_message):
            logger.info(f"Resultado final enviado.")

if __name__ == '__main__':
    worker = CreditsJoiner()
    worker.start()
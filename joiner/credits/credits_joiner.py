import threading
from collections import defaultdict
import logging
from fileinput import close

from middleware.consumer.consumer import Consumer
from middleware.consumer.subscriber import Subscriber
from middleware.producer.producer import Producer
from utils.parsers.credits_parser import convert_data
from worker.worker import Worker

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')


class CreditsJoiner(Worker):
    def __init__(self):
        super().__init__()
        self.shutdown_event = threading.Event()

        self.movies_per_client = defaultdict(set)
        # TODO consumir el result de 20th century aggregator
        self.movies_consumer = Subscriber("20_century_arg_result",
                                        message_handler=self.handle_movies_message)
        self.credits_consumer = Consumer("credits",
                                        _message_handler=self.handle_credits_message)
        self.producer = Producer("top_10_actors_from_batch")
        self.pending_producer = Producer(
            queue_name="credits_pending",
            queue_type="direct",
            dlx="direct_exchange",
            ttl=2000
        )

        self.movies = {}
        self.client_id = "client_id"


    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.movies_consumer.close()
            self.credits_consumer.close()
            self.producer.close()
            self.pending_producer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

    def handle_credits_message(self, message):
        try:
            logger.info(f"Mensaje de credits recibido - cliente: " + str(message.get("client_id")))
            client_id = message.get("client_id")

            if client_id not in self.movies:
                logger.info("client id " + "not ready for credits file")
                self.pending_producer.enqueue(message, routing_key_override="credits")
                return

            actor_counts = {}
            actors = convert_data(message)
            movies_per_client = self.movies.get(client_id, set())
            for actor in actors:
                if actor.movie_id in movies_per_client:
                    logger.info(f"Actor encontrado para una pelicula argentina.")
                    actor_id = actor.id
                    actor_name = actor.name
                    if actor_id not in actor_counts:
                        actor_counts[actor_id] = {"name": actor_name, "count": 1}
                    else:
                        actor_counts[actor_id]["count"] += 1

            top_10 = sorted(actor_counts.items(), key=lambda item: item[1]["count"], reverse=True)[:10]

            logger.info("Top 10 actores con más contribuciones:")
            for actor_id, info in top_10:
                logger.info(f"{info['name']}: {info['count']} contribuciones")

            result_message = {
                "type": "query_4_top_10_actores_credits",
                "actors": top_10,
                "client_id": client_id
            }
            if message.get("total_batches") != 0: # Mensaje que contiene el total. Uno por cliente.
                result_message["total_batches"] = message.get("total_batches")
                logger.info(f"Se envia la cantidad total de batches: {result_message['total_batches']}.")

            if message.get("batch_size") != 0:
                result_message["batch_size"] = message.get("batch_size")
                logger.info(f"Se envia la cantidad de este batch: {result_message['batch_size']}.")

            self.producer.enqueue(result_message)
            logger.info(f"Resultado enviado {result_message}.")
        except Exception as e:
            logger.error(f"Error al procesar credits: {e}")
            self.close()

    def handle_movies_message(self, message):
        logger.info(f"Mensaje de movies recibido - cliente: " + str(message.get("client_id")))
        if message.get("type") == "20_century_arg_total_result":
            self.process_movie_message(message)
        else:
            logger.error(f"Tipo de mensaje no esperado. Tipo recibido: {message.get('type')}")

    def process_movie_message(self, message):
        self.movies[message.get("client_id")] = {movie["id"] for movie in message.get("movies")}
        logger.info(f"Obtenidas {message.get('total_movies')} películas")
        self.credits_consumer.start_consuming()

    def start(self):
        logger.info("Iniciando filtro de películas españolas")
        try:
            thread = threading.Thread(target=self.movies_consumer.start)
            thread.daemon = True
            thread.start()
            self.shutdown_event.wait()
        finally:
            self.close()


if __name__ == '__main__':
    worker = CreditsJoiner()
    worker.start()

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
        self.movies_per_client = defaultdict(set)
        # TODO consumir el result de 20th century aggregator
        self.movies_consumer = Subscriber("20_century_arg_result",
                                        message_handler=self.handle_movies_result_message)
        self.credits_consumer = Consumer("credits",
                                        _message_handler=self.handle_credits_message)
        self.producer = Producer("top_10_actors_from_batch")

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.movies_consumer.close()
            self.credits_consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

    def handle_credits_message(self, message):
        try:
            logger.info(f"Mensaje de credits recibido")
            actor_counts = {}
            actors = convert_data(message)
            for actor in actors:
                if actor.movie_id in self.movies:
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

    def handle_movies_result_message(self, message):
        logger.info(f"Mensaje de movies recibido")
        if message.get("type") == "20_century_arg_total_result":
            self.movies = {movie["id"] for movie in message.get("movies")}
            logger.info(f"Obtenidas {message.get('total_movies')} películas")
            self.credits_consumer.start_consuming()
        else:
            logger.error(f"Tipo de mensaje no esperado. Tipo recibido: {message.get('type')}")

    def start(self):
        logger.info("Iniciando filtro de películas españolas")
        try:
            self.movies_consumer.start()
        finally:
            self.close()


if __name__ == '__main__':
    worker = CreditsJoiner()
    worker.start()

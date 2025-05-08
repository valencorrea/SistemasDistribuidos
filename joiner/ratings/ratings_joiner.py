import logging
from collections import defaultdict
import json
import os

from middleware.consumer.consumer import Consumer
from middleware.consumer.subscriber import Subscriber
from middleware.producer.producer import Producer
from middleware.producer.publisher import Publisher
from utils.parsers.ratings_parser import convert_data_for_rating_joiner
from worker.worker import Worker

PENDING_MESSAGES = "/root/files/ratings_pending.jsonl"

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')

class RatingsJoiner(Worker):
    def __init__(self):
        super().__init__()
        self.movies_ratings = defaultdict(dict)
        self.received_ratings_batches_per_client = defaultdict(int)
        self.total_ratings_batches_per_client = defaultdict(int)

        self.processed_rating_batches_per_client = defaultdict(int)
        self.ratings_consumer = Consumer("ratings",
                                        _message_handler=self.handle_ratings_message)
        self.movies_consumer = Subscriber("20_century_arg_result",
                                        message_handler=self.handle_movies_result_message)
        self.producer = Producer("best_and_worst_ratings_partial_result")

        self.ratings_producer = Producer(
            queue_name="ratings",
            queue_type="direct")

        self.amounts_control_producer = Publisher("ratings_amounts")
        self.amounts_control_consumer = Subscriber("ratings_amounts",
                                                   message_handler=self.handle_ratings_amounts)

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.ratings_consumer.close()
            self.movies_consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.exception(f"❌ Error al cerrar conexiones: {e}")

    def handle_ratings_amounts(self, message):
        try:
            logger.debug(f"Mensaje de control de ratings recibido")
            message_type = message.get("type")
            client_id = message.get("client_id")
            amount = int(message.get("amount", 0))
            if message_type == "total_batches":
                self.total_ratings_batches_per_client[client_id] = amount
                logger.debug(f"Actualizado total_batches {amount} a {self.total_ratings_batches_per_client[client_id] }")
            elif message_type == "batch_size":
                self.received_ratings_batches_per_client[client_id] = self.received_ratings_batches_per_client.get(client_id, 0) + amount
                logger.debug(f"Actualizado batch_size {amount} a {self.received_ratings_batches_per_client[client_id] }")
            else:
                logger.error(f"Tipo de mensaje no esperado. Tipo recibido: {message_type}")
                return

            total = self.total_ratings_batches_per_client.get(client_id, None)
            received = self.received_ratings_batches_per_client.get(client_id, 0)
            logger.debug(f"Control de ratings total: {total} received: {received}")
            if total is not None and 0 < total <= received:
                logger.info(
                    f"Ya fueron procesados todos los batches ({received}/{total}) para el cliente {client_id}. Enviando el acumulado.")
                self.producer.enqueue({
                    "type": "query_3_arg_2000_ratings",
                    "ratings": self.get_movies_with_votes_for_client(client_id),
                    "batch_size": received,
                    "total_batches": total,
                    "processed_batches": self.processed_rating_batches_per_client.get(client_id, 0),
                    "client_id": client_id
                })
                self.movies_ratings.pop(client_id)
                self.total_ratings_batches_per_client.pop(client_id)
                self.received_ratings_batches_per_client.pop(client_id)

        except Exception as e:
            logger.error(f"Error al procesar mensaje <{message}> de control de cantidades: {e}")

    def get_movies_with_votes_for_client(self, client_id):
        movies_with_ratings = {}
        for movie_id, data in self.movies_ratings[client_id].items():
            if data["votes"] > 0:
                movies_with_ratings[movie_id] = data
        logger.info(f"Obtenidas {len(movies_with_ratings.keys())} peliculas con al menos un rating")
        return movies_with_ratings

    def handle_movies_result_message(self, message):
        logger.info(f"Mensaje de movies recibido - cliente: " + str(message.get("client_id")))

        if message.get("type") != "20_century_arg_total_result":
            logger.error(f"Tipo de mensaje no esperado. Tipo recibido: {message.get('type')}")
            return

        client_id = message.get("client_id")
        movies = message.get("movies")

        if not client_id:
            logger.error("Mensaje sin client_id. Se descarta.")
            return

        if not isinstance(movies, list):
            logger.error("Campo 'movies' no es una lista válida.")
            return

        try:
            logger.info(f"Se van a guardar {len(movies)} películas para el cliente {client_id}")
            for movie in movies:
                if not isinstance(movie, dict):
                    logger.warning(f"Pelicula no es un diccionario. Pelicula: {movie}")
                    continue

                movie_id = movie.get("id")
                if not movie_id:
                    logger.warning(f"Pelicula sin ID: {movie}")
                    continue

                self.movies_ratings[client_id][movie_id] = {
                    "title": movie.get("title", ""),
                    "rating_sum": 0,
                    "votes": 0
                }

            if os.path.exists(PENDING_MESSAGES):
                temp_path = PENDING_MESSAGES + ".tmp"
                with open(PENDING_MESSAGES, "r") as infile, open(temp_path, "w") as outfile:
                    for line in infile:
                        try:
                            pending_msg = json.loads(line)
                        except json.JSONDecodeError:
                            logger.warning("Línea inválida en archivo pendiente.")
                            continue

                        if pending_msg.get("client_id") == client_id:
                            logger.info(f"Reprocesando mensaje pendiente para cliente {client_id}")
                            self.ratings_producer.enqueue(pending_msg)
                        else:
                            outfile.write(line)

                os.replace(temp_path, PENDING_MESSAGES)

            logger.info(f"{len(self.movies_ratings[client_id])} películas guardadas para {client_id}")
            if not self.ratings_consumer.is_alive():
                self.ratings_consumer.start()
                logger.info("Thread de consumo de ratings empezado")
            else:
                logger.info("Thread de consumo de ratings no empezado, ya existe uno")

        except Exception as e:
            logger.error(f"Error al procesar mensaje de películas: {e}", exc_info=True)
            self.close()


    def handle_ratings_message(self, message):
        try:
            logger.info(f"Mensaje de ratings recibido - cliente: " + str(message.get("client_id")))
            client_id = message.get("client_id")
            if client_id not in self.movies_ratings:
                os.makedirs(os.path.dirname(PENDING_MESSAGES), exist_ok=True)
                logger.info("client id " + client_id + " not ready for credits file. Saving locally")
                with open(PENDING_MESSAGES, "a") as f:
                    f.write(json.dumps(message) + "\n")
                return

            ratings = convert_data_for_rating_joiner(message)
            logger.debug(f"Ratings convertidos. Total recibido: {len(ratings)}")
            logger.debug(f"Se tienen {len(self.movies_ratings[client_id])} peliculas para el cliente {client_id}")
            self.processed_rating_batches_per_client[client_id] += message.get("batch_size", 0)
            for rating in ratings:
                if not isinstance(rating, dict):
                    continue
                movie_id = rating.get("movieId")
                if int(movie_id) in self.movies_ratings[client_id].keys() or str(movie_id) in self.movies_ratings[client_id].keys():
                    logger.debug(f"Se agrega rating a la pelicula {movie_id}")
                    self.movies_ratings[client_id][movie_id]["rating_sum"] += float(rating.get("rating", 0))
                    self.movies_ratings[client_id][movie_id]["votes"] += 1

            batch_size = int(message.get("batch_size", 0))
            total_batches = int(message.get("total_batches", 0))

            logger.debug(f"Ratings procesados. Total actual: {len(self.movies_ratings)} batch_size {batch_size} total_batches {total_batches}")

            if batch_size != 0:
                message = {"type": "batch_size", "amount": batch_size, "client_id": client_id}
                self.amounts_control_producer.enqueue(message)
            if total_batches != 0:
                message = {"type": "total_batches","amount": total_batches, "client_id": client_id}
                self.amounts_control_producer.enqueue(message)
                logger.info("message " + str(message) + " enqueued")
                logger.debug(f"Mensaje de control de cantidades de ratings enviado total_batches: {total_batches}")
        except Exception as e:
            logger.error(f"Error al procesar ratings: {e}")
            self.close()

    def start(self):
        logger.debug("Iniciando joiner de ratings")
        try:
            self.amounts_control_consumer.start()
            self.movies_consumer.start()
            self.shutdown_event.wait()
        finally:
            self.close()

if __name__ == '__main__':
    worker = RatingsJoiner()
    worker.start()

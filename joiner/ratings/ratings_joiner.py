from collections import defaultdict
import logging

from middleware.consumer.consumer import Consumer
from middleware.consumer.subscriber import Subscriber
from middleware.producer.publisher import Publisher
from middleware.producer.producer import Producer
from utils.parsers.ratings_parser import convert_data_for_rating_joiner
from worker.worker import Worker

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
        self.ratings_consumer = Consumer("ratings",
                                        _message_handler=self.handle_ratings_message)
        self.movies_consumer = Subscriber("20_century_arg_result",
                                        message_handler=self.handle_movies_result_message)
        self.producer = Producer("best_and_worst_ratings_partial_result")
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
            logger.error(f"Error al cerrar conexiones: {e}")

    def handle_ratings_amounts(self, message):
        message_type = message.get("type", None)
        client_id = message.get("client_id", None)
        if message_type == "total_batches":
            self.total_ratings_batches_per_client[client_id] = message.get("amount", 0)
        elif message_type == "batch_size":
            if client_id not in self.received_ratings_batches_per_client:
                self.received_ratings_batches_per_client[client_id] = 0
            self.received_ratings_batches_per_client[client_id] += message.get("amount", 0)
        else:
            logger.error(f"Tipo de mensaje no esperado. Tipo recibido: {message.get('type')}")

        if self.total_ratings_batches_per_client[client_id] and self.received_ratings_batches_per_client[client_id] >= self.total_ratings_batches_per_client[client_id]:
            logger.info("Ya fueron procesados todos los batches. Enviando el acumulado para este worker.")
            # result = self.obtain_result()
            # if result:
            self.producer.enqueue({
                "type": "query_3_arg_2000_ratings",
                "ratings": self.get_movies_with_votes_for_client(client_id),
                "batch_size": self.received_ratings_batches_per_client[client_id],
                "total_batches": self.total_ratings_batches_per_client[client_id],
                "client_id": client_id
            })

    def get_movies_with_votes_for_client(self, client_id):
        logger.info(f"Se tienen {len(self.movies_ratings[client_id])} peliculas: {self.movies_ratings[client_id]}")
        movies_with_ratings = {}
        for movie_id, data in self.movies_ratings[client_id].items():
            if data["votes"] > 0:
                movies_with_ratings[movie_id] = data
        logger.info(f"Obtenidas {len(movies_with_ratings.keys())} peliculas con al menos un rating")
        return movies_with_ratings

    def handle_movies_result_message(self, message):
        logger.info(f"Mensaje de movies recibido")
        if message.get("type") == "20_century_arg_total_result":
            client_id = message.get("client_id", None)
            try:
                logger.info(f"Se van a guardar  {len(message.get('movies'))} peliculas")
                for movie in message.get("movies"):
                    if not isinstance(movie, dict):
                        logger.info(f"Pelicula no es un diccionario. Pelicula: {movie}")
                        continue
                    movie_id = movie.get("id")
                    if movie_id:
                        print(f"Pelicula: {movie_id} {type(movie_id)}")
                        self.movies_ratings[client_id][movie_id] = {
                            "title": movie.get("title", ""),
                            "rating_sum": 0,
                            "votes": 0
                        }
                self.ratings_consumer.start_consuming()
            except Exception as e:
                logger.error(f"Error al procesar ratings: {e}")
        else:
            logger.error(f"Tipo de mensaje no esperado. Tipo recibido: {message.get('type')}")

    def handle_ratings_message(self, message):
        try:
            logger.info(f"Mensaje de ratings recibido")
            ratings = convert_data_for_rating_joiner(message)
            logger.info(f"Ratings convertidos. Total recibido: {len(ratings)}")
            for rating in ratings:
                if not isinstance(rating, dict):
                    continue

                movie_id = rating.get("movieId")
                client_id = rating.get("client_id", None)

                if int(movie_id) in self.movies_ratings[client_id].keys() or str(movie_id) in self.movies_ratings[client_id].keys():
                    logger.info(f"Se agrega rating a la pelicula {movie_id}")
                    self.movies_ratings[client_id][movie_id]["rating_sum"] += float(rating.get("rating", 0))
                    self.movies_ratings[client_id][movie_id]["votes"] += 1
            batch_size = int(message.get("batch_size", 0))
            total_batches = int(message.get("total_batches", 0))

            logger.info(f"Ratings procesados. Total actual: {len(self.movies_ratings)} batch_size {batch_size} total_batches {total_batches}")

            if batch_size != 0:
                self.amounts_control_producer.enqueue({"type": "batch_size","amount": batch_size, "client_id": message.get("client_id")})
            if total_batches != 0:
                self.amounts_control_producer.enqueue({"type": "total_batches","amount": total_batches, "client_id": message.get("client_id")})
                logger.info(f"Mensaje de control de cantidades de ratings enviado total_batches: {total_batches}")
        except Exception as e:
            logger.error(f"Error al procesar ratings: {e}")

    def start(self):
        logger.info("Iniciando joiner de ratings")
        self.amounts_control_consumer.start()
        self.movies_consumer.start()


if __name__ == '__main__':
    worker = RatingsJoiner()
    worker.start()

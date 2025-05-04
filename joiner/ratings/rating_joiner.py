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
        self.movies_ratings = {}
        self.received_ratings_batches = 0
        self.total_ratings_batches = None
        self.ratings_consumer = Consumer("ratings",
                                        _message_handler=self.handle_ratings_message)
        self.movies_consumer = Consumer("20_century_arg_result",
                                        _message_handler=self.handle_movies_result_message)
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
        logger.info(f"Mensaje de control de cantidades de ratings recibido {message}")
        message_type = message.get("type", None)
        if message_type == "total_batches":
            self.total_ratings_batches = message.get("amount", 0)
            logger.info(f"Cantidad total de batches actualizada: {self.total_ratings_batches}")
        elif message_type == "batch_size":
            self.received_ratings_batches = message.get("amount", 0)
            logger.info(f"Se agrega {message.get('amount', 0)} a la cantidad  de procesados por un total de "
                        f'{self.received_ratings_batches}')
        else:
            logger.error(f"Tipo de mensaje no esperado. Tipo recibido: {message.get('type')}")

        if self.total_ratings_batches and self.received_ratings_batches >= self.total_ratings_batches:
            logger.info("Ya fueron procesados todos los batches. Enviando el acumulado para este worker.")
            result = self.obtain_result()
            if result:
                self.producer.enqueue({
                    "type": "query_3_arg_2000_ratings",
                    "ratings": result
                })

    def handle_movies_result_message(self, message):
        logger.info(f"Mensaje de movies recibido")
        if message.get("type") == "20_century_arg_total_result":
            try:
                logger.info(f"Se van a guardar  {len(message.get('movies'))} peliculas")
                for movie in message.get("movies"):
                    if not isinstance(movie, dict):
                        continue
                    movie_id = movie.get("id")
                    if movie_id:
                        self.movies_ratings[movie_id] = {
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
                if movie_id in self.movies_ratings:
                    self.movies_ratings[movie_id]["rating_sum"] += float(rating.get("rating", 0))
                    self.movies_ratings[movie_id]["votes"] += 1
                else:
                    self.movies_ratings[movie_id] = {
                        "rating_sum": float(rating.get("rating", 0)),
                        "votes": 1
                    }
            batch_size = int(message.get("batch_size", 0))
            total_batches = int(message.get("total_batches", 0))

            logger.info(f"Ratings procesados. Total actual: {len(self.movies_ratings)} batch_size {batch_size} total_batches {total_batches}")

            if batch_size != 0:
                self.amounts_control_producer.enqueue({"type": "batch_size","amount": batch_size})
            if total_batches != 0:
                self.amounts_control_producer.enqueue({"type": "total_batches","amount": total_batches})
        except Exception as e:
            logger.error(f"Error al procesar ratings: {e}")

    def start(self):
        logger.info("Iniciando joiner de ratings")
        try:
            self.amounts_control_consumer.start()
            self.movies_consumer.start_consuming()
        finally:
            self.close()

    def obtain_result(self):
        max_rating = float('-inf')
        min_rating = float('inf')
        best_movie = None
        worst_movie = None

        for movie_id, data in self.movies_ratings.items():
            if data["votes"] > 0:
                avg_rating = data["rating_sum"] / data["votes"]
                movie_data = {
                    "id": movie_id,
                    "title": data["title"],
                    "rating": avg_rating,
                }

                # Actualizar el mejor rating
                if avg_rating > max_rating:
                    max_rating = avg_rating
                    best_movie = movie_data

                # Actualizar el peor rating
                if avg_rating < min_rating:
                    min_rating = avg_rating
                    worst_movie = movie_data

        return {
            "best": best_movie,
            "best_rating": max_rating,
            "worst": worst_movie,
            "min_rating": min_rating,
        }

if __name__ == '__main__':
    worker = RatingsJoiner()
    worker.start()

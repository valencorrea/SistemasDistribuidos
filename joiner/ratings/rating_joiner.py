import logging
from heapq import heappush, heappushpop, nlargest

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.ratings_parser import convert_data_for_rating_joiner
from worker.worker import Worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RatingsJoiner(Worker):
    def __init__(self):
        super().__init__()
        self.partial_aggregator_consumer = Consumer("rating_joiner", message_factory=self.handle_partial_aggregator_message)
        self.ratings_consumer = Consumer("ratings", message_factory=self.handle_ratings_message)
        self.ratings_producer = Producer("result")

        self.movies_ratings = {}
        self.receive_movie_batches = 0
        self.total_movie_batches = None
        self.receive_ratings_batches = 0
        self.total_ratings_batches = None

    def close(self):
        """Cierra las conexiones"""
        self.partial_aggregator_consumer.close()
        self.ratings_consumer.close()
        self.ratings_producer.close()

    @staticmethod
    def handle_ratings_message(self, message):
        if not message or not isinstance(message, dict):
            return None
            
        if message.get("type") == "shutdown":
            return message

        ratings = convert_data_for_rating_joiner(message)

        batch_message = {
            "ratings": ratings,
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result"
        }

        return batch_message

    @staticmethod
    def handle_partial_aggregator_message(message):
        if not message or not isinstance(message, dict):
            return None
            
        if message.get("type") == "shutdown":
            return message

        batch_message = {
            "movies": message.get("movies", []),
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result"
        }
        
        return batch_message

    def start(self):
        """Inicia el procesamiento de películas y ratings"""
        logger.info("Iniciando joiner de ratings")
        
        try:
            # Procesar películas primero
            while not self.shutdown_event.is_set():
                message = self.partial_aggregator_consumer.dequeue()
                if not message:
                    continue

                if message.get("type") == "batch_result":
                    movies = message.get("movies", [])
                    for movie in movies:
                        if not isinstance(movie, dict):
                            continue
                            
                        movie_id = movie.get("id")
                        if movie_id:
                            self.movies_ratings[movie_id] = {
                                "title": movie.get("title", ""),
                                "rating_sum": 0,
                                "votes": 0
                            }

                    self.receive_movie_batches += message.get("batch_size", 0)
                    if message.get("total_batches") and message.get("total_batches") > 0:
                        self.total_movie_batches = message.get("total_batches")

                    if self.total_movie_batches and self.receive_movie_batches >= self.total_movie_batches:
                        break

            # Procesar ratings
            while not self.shutdown_event.is_set():
                message = self.ratings_consumer.dequeue()
                if not message:
                    continue

                if message.get("type") == "batch_result":
                    ratings = message.get("ratings", [])
                    for rating in ratings:
                        if not isinstance(rating, dict):
                            continue
                            
                        movie_id = rating.get("movieId")
                        if movie_id in self.movies_ratings:
                            self.movies_ratings[movie_id]["rating_sum"] += float(rating.get("rating", 0))
                            self.movies_ratings[movie_id]["votes"] += 1

                    if message.get("total_batches") and message.get("total_batches") > 0:
                        self.total_ratings_batches = message.get("total_batches")
                    self.receive_ratings_batches += message.get("batch_size", 0)
                    if self.total_ratings_batches and self.total_ratings_batches > 0:
                        logger.info(f"Total de ratings procesados: {self.receive_ratings_batches}/{self.total_ratings_batches}")
                    if self.total_ratings_batches and self.receive_ratings_batches >= self.total_ratings_batches:
                        logger.info("Total de ratings procesados")
                        break

            # Enviar resultado final
            result = self.obtain_result()
            if result:
                self.ratings_producer.enqueue({
                    "type": "result",
                    "ratings": result
                })

        except Exception as e:
            logger.error(f"Error durante el procesamiento: {e}")
        finally:
            self.close()

    def obtain_result(self):
        # Usamos un min-heap para mantener los 5 mejores
        top_5 = []
        for movie_id, data in self.movies_ratings.items():
            if data["votes"] > 0:
                avg_rating = data["rating_sum"] / data["votes"]
                movie_data = {
                    "id": movie_id,
                    "title": data["title"],
                    "rating": avg_rating,
                }
                
                if len(top_5) < 5:
                    heappush(top_5, (avg_rating, movie_data))
                elif avg_rating > top_5[0][0]:
                    heappushpop(top_5, (avg_rating, movie_data))
        
        # Convertir el heap a la lista final de resultados
        return [item[1] for item in nlargest(5, top_5)]

if __name__ == '__main__':
    worker = RatingsJoiner()
    worker.start()
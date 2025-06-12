from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker


class Aggregator(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("best_and_worst_ratings_partial_result",
                                 _message_handler=self.handle_message)
        self.producer = Producer("result")
        self.total_batches_per_client = defaultdict(int)
        self.received_batches_per_client = defaultdict(int)
        self.movies_ratings = defaultdict(dict)

    def close(self):
        self.logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            self.logger.error(f"Error al cerrar conexiones: {e}")

    def handle_message(self, message):
        self.logger.info(f"Mensaje de ratings recibido: {message}")
        ratings = message.get("ratings")
        self.logger.info(f"Se obtuvieron {len(ratings)} ratings: {ratings}.")
        batch_size = int(message.get("processed_batches", 0))
        total_batches = int(message.get("total_batches", 0))
        client_id = message.get("client_id", None)
        if batch_size != 0:
            self.received_batches_per_client[client_id] += batch_size
            self.logger.info(f"Se actualiza la cantidad recibida: {batch_size}, actual: {self.received_batches_per_client[client_id]}.")

        if total_batches != 0:
            self.total_batches_per_client[client_id] = total_batches
            self.logger.info(f"Se actualiza la cantidad total de batches: {self.total_batches_per_client[client_id]}.")

        for movie_id, data in ratings.items():
            if movie_id in self.movies_ratings[client_id]:
                self.movies_ratings[client_id][movie_id]["rating_sum"] += float(data.get("rating_sum", 0))
                self.movies_ratings[client_id][movie_id]["votes"] += int(data.get("votes", 0))
            else:
                self.movies_ratings[client_id][movie_id] = {
                    "title": data.get("title", ""),
                    "rating_sum": float(data.get("rating_sum", 0)),
                    "votes": int(data.get("votes", 0))
                }

        if self.total_batches_per_client[client_id] and self.received_batches_per_client[client_id] >= self.total_batches_per_client[client_id]:
            result = self.obtain_result(client_id)
            self.producer.enqueue({
                "result_number": 3,
                "type": "best_and_worst_movies",
                "actors": result,
                "client_id": client_id
            })
            self.movies_ratings.pop(client_id)
            self.received_batches_per_client.pop(client_id)
            self.total_batches_per_client.pop(client_id)
            self.logger.info("Resultado de mejor y peor pelicula enviado.")

    def obtain_result(self, client_id):
        max_rating = float('-inf')
        min_rating = float('inf')
        best_movie = None
        worst_movie = None

        for movie_id, data in self.movies_ratings[client_id].items():
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

    def start(self):
        self.logger.info("Iniciando agregador")
        try:
            self.consumer.start_consuming()
        finally:
            self.close()


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()

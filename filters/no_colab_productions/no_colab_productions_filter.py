import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from utils.parsers.movie_parser import convert_data_for_second_filter
from worker.worker import Worker




class NoColabProductionsFilter(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("movie_1", _message_handler=self.handle_message)
        self.producer = Producer("aggregate_consulta_2")

    def close(self):
        self.logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            self.logger.error(f"Error al cerrar las conexiones: {e}")

    def handle_message(self, message):
        movies = convert_data_for_second_filter(message)
        filtered_movies = self.apply_filter(movies)

        batch_message = {
            "movies": filtered_movies,
            "batch_size": message.get("batch_size", 0),
            "total_batches": message.get("total_batches", 0),
            "type": "batch_result",
            "client_id": message.get("client_id"),
            "batch_id": message.get("batch_id"),
        }
        self.producer.enqueue(batch_message)

    def start(self):
        self.logger.info("Iniciando filtro de películas top 5 local")
        try:
            self.consumer.start_consuming()
        finally:
            self.close()

    @staticmethod
    def apply_filter(movies):
        result = []
        for movie in movies:
            if movie.get("production_countries") and len(movie.get("production_countries")) == 1:
                result.append({"country": movie.get("production_countries")[0], "budget": movie.get("budget")})
        return result


if __name__ == '__main__':
    worker = NoColabProductionsFilter()
    worker.start()

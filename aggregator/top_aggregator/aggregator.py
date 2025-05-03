import logging
from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker


logger = logging.getLogger(__name__)

class Aggregator(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("aggregate_consulta_2",
                                 _message_handler=self.handle_message)
        self.producer = Producer("result")
        self.results = {}
        self.control_batches_per_client = {}
        self.total_batches_per_client = {}

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

    def process_country_budget(self, movies, client_id):
        for movie in movies:
            if not movie:
                continue
            try:
                print(f"movie: {movie}")
                country = movie.get("country", "")
                budget = float(movie.get("budget", 0))
                
                if country:
                    if client_id not in self.results.keys():
                        print(f"self.results: {self.results}")
                        self.results[client_id] = defaultdict(float)
                        print(f"self.results: {self.results}")
                    self.results[client_id][country] += budget
                    print(f"self.results: {self.results}")
                if not country:
                    print(f"Pelicula sin pais: {movie}")
            except (ValueError, TypeError) as e:
                logger.warning(f"Error procesando película: {e}")
                continue

    def _get_top_5_countries(self, client_id):
        sorted_countries = sorted(
            self.results[client_id].items(),
            key=lambda x: x[1],
            reverse=True
        )
        top_5 = sorted_countries[:5]
        return [
            {
                "country": country,
                "total_budget": budget
            }
            for country, budget in top_5
        ]

    def handle_message(self, message):
        if message.get("type") == "batch_result":
            client_id = message.get("client_id")
            print(f"client_id: {client_id}")
            self.process_country_budget(message.get("movies", []),client_id)
            if client_id not in self.control_batches_per_client.keys():
                self.control_batches_per_client[client_id] = 0
            self.control_batches_per_client[client_id] += message.get("batch_size", 0)

            if message.get("total_batches"):
                self.total_batches_per_client[client_id] = message.get("total_batches")

            logger.info(f"Batch procesado. Países acumulados: {len(self.results[client_id])}")
            logger.info(f"Batches recibidos: {self.control_batches_per_client[client_id]}/{self.total_batches_per_client[client_id]}")

            if self.total_batches_per_client[client_id] and 0 < self.total_batches_per_client[client_id] <= self.control_batches_per_client[client_id]:
                top_5_countries = self._get_top_5_countries(client_id)
                result_message = {
                    "result_number": 2,
                    "type": "query_2_top_5",
                    "result": top_5_countries,
                    "client_id": message.get("client_id")
                }
                if self.producer.enqueue(result_message):
                    logger.info("Resultado final enviado con top 5 países")
                    self.results.pop(client_id)
                    self.control_batches_per_client.pop(client_id)
                    self.total_batches_per_client.pop(client_id)


    def start(self):
        logger.info("Iniciando agregador")
        try:
            self.consumer.start_consuming()
        finally:
            self.close()

if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start() 
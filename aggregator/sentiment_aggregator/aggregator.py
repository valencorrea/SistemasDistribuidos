import logging
from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker


logger = logging.getLogger(__name__)


class Aggregator(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("aggregate_consulta_5",
                                 _message_handler=self.handle_message)
        self.producer = Producer("result")
        self.results_revenue = {}
        self.results_budget = {}
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

    def process_sentiment_revenue_budget(self, movies, client_id):
        for movie in movies:
            if not movie:
                continue
            try:
                sentiment = movie.get("sentiment", "")
                revenue = float(movie.get("revenue", 0))
                budget = float(movie.get("budget", 0))

                if sentiment:
                    if client_id not in self.results_revenue.keys():
                        self.results_revenue[client_id] = defaultdict(float)
                    self.results_revenue[client_id][sentiment] += revenue
                    if client_id not in self.results_budget.keys():
                        self.results_budget[client_id] = defaultdict(float)
                    self.results_budget[client_id][sentiment] += budget
            except (ValueError, TypeError) as e:
                logger.warning(f"Error procesando película: {e}")
                continue

    def _get_sentiment_mean(self,client_id):
        sentiment_mean = {}
        for sentiment in self.results_budget[client_id]:
            sentiment_mean[sentiment] = {
                "revenue": self.results_revenue[client_id][sentiment] / self.results_budget[client_id][sentiment]
            }
        return sentiment_mean

    def handle_message(self, message):
        if message.get("type") == "batch_result":
            client_id = message.get("client_id")
            if client_id not in self.control_batches_per_client.keys():
                self.control_batches_per_client[client_id] = 0
            self.process_sentiment_revenue_budget(message.get("movies", []),client_id)
            self.control_batches_per_client[client_id] += message.get("batch_size", 0)

            if message.get("total_batches"):
                self.total_batches_per_client[client_id] = message.get("total_batches")


            if self.total_batches_per_client[client_id] and 0 < self.total_batches_per_client[client_id] <= self.control_batches_per_client[client_id]:
                rate_revenue_budget = self._get_sentiment_mean(client_id)
                result_message = {
                    "result_number": 5,
                    "type": "query_5_sentiments",
                    "result": rate_revenue_budget,
                    "client_id": message.get("client_id")
                }
                if self.producer.enqueue(result_message):
                    logger.info(f"Resultado final enviado para el cliente {client_id}")
                    self.results_revenue.pop(client_id)
                    self.results_budget.pop(client_id)
                    self.control_batches_per_client.pop(client_id)
                    self.total_batches_per_client.pop(client_id)

    def start(self):
        logger.info("Iniciando agregador")
        self.consumer.start_consuming()


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()

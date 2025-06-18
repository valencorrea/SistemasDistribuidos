import logging
from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker




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
        self.results_ratios = defaultdict()

    def close(self):
        self.self.logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            self.self.logger.error(f"Error al cerrar conexiones: {e}")

    def process_sentiment_revenue_budget(self, movies, client_id):
        for movie in movies:
            if not movie:
                continue
            try:
                sentiment = movie.get("sentiment", "")
                revenue = float(movie.get("revenue", 0))
                budget = float(movie.get("budget", 0))

                if sentiment and budget > 0:
                    if client_id not in self.results_ratios:
                        self.results_ratios[client_id] = defaultdict(list)
                    ratio = revenue / budget
                    self.results_ratios[client_id][sentiment].append(ratio)
            except (ValueError, TypeError) as e:
                self.logger.warning(f"Error procesando película: {e}")
                continue

    def _get_sentiment_mean(self, client_id):
        sentiment_mean = {}
        for sentiment, ratios in self.results_ratios[client_id].items():
            if ratios:
                sentiment_mean[sentiment] = {
                    "revenue": sum(ratios) / len(ratios)
                }
        return sentiment_mean

    def handle_message(self, message):
        if message.get("type") == "batch_result":
            client_id = message.get("client_id")
            batch_id = message.get("batch_id")

            if client_id not in self.control_batches_per_client:
                self.control_batches_per_client[client_id] = 0
            self.process_sentiment_revenue_budget(message.get("movies", []), client_id)
            self.control_batches_per_client[client_id] += message.get("batch_size", 0)

            if message.get("total_batches"):
                self.logger.info(f"Se recibio el mensaje de totales para el cliente {client_id}: {message.get('total_batches')}")
                self.total_batches_per_client[client_id] = message.get("total_batches")

            if client_id in self.total_batches_per_client and 0 < self.total_batches_per_client[client_id] <= self.control_batches_per_client[client_id]:
                self.logger.info(f"Se tiene el total de batches para el cliente {client_id}. Enviando resultado.")

                rate_revenue_budget = self._get_sentiment_mean(client_id)
                result_message = {
                    "result_number": 5,
                    "type": "query_5_sentiments",
                    "result": rate_revenue_budget,
                    "client_id": message.get("client_id"),
                    "batch_id": batch_id
                }
                self.logger.info(f"4")
                if self.producer.enqueue(result_message):
                    self.logger.info(f"Resultado final enviado para el cliente {client_id}")
                    self.results_ratios.pop(client_id)
                    self.control_batches_per_client.pop(client_id)
                    self.total_batches_per_client.pop(client_id)

    def start(self):
        self.logger.info("Iniciando agregador")
        self.consumer.start_consuming()


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()

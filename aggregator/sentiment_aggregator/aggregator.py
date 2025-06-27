from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.abstractaggregator.abstractaggregator import AbstractAggregator


class Aggregator(AbstractAggregator):
    def __init__(self):
        super().__init__()

    def create_consumer(self):
        return Consumer("aggregate_consulta_5", _message_handler=self.handle_message)

    def create_producer(self):
        return Producer("result")

    def create_final_result(self, client_id):
        rate_revenue_budget = self._get_sentiment_mean(client_id)
        return {
            "result_number": 5,
            "type": "query_5_sentiments",
            "result": rate_revenue_budget,
            "client_id": client_id
        }

    def process_message(self, client_id, message):
        movies = message.get("movies", [])
        partial_results = defaultdict(list)
        for movie in movies:
            if movie:
                try:
                    sentiment = movie.get("sentiment", "")
                    revenue = float(movie.get("revenue", 0))
                    budget = float(movie.get("budget", 0))

                    if sentiment and budget > 0:
                        ratio = revenue / budget
                        partial_results[sentiment].append(ratio)
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"Error procesando película: {e}")
                    continue
        return partial_results

    def aggregate_message(self, client_id, result):
        self.logger.info(f"Clientes {self.results.keys()}")
        if client_id not in self.results.keys():
            self.logger.info(f"Nuevo cliente agregado: {client_id}")
            self.results[client_id] = result
        else:
            self.logger.info(f"Cliente {client_id} ya existe, agregando resultados parciales")
            for sentiment in result.keys():
                self.results[client_id][sentiment].extend(result[sentiment])

    def _get_sentiment_mean(self, client_id):
        sentiment_mean = {}
        for sentiment, ratios in self.results[client_id].items():
            if ratios:
                sentiment_mean[sentiment] = {
                    "revenue": sum(ratios) / len(ratios)
                }
        return sentiment_mean


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()

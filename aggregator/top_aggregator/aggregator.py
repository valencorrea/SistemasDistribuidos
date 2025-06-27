from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.abstractaggregator.abstractaggregator import AbstractAggregator


class Aggregator(AbstractAggregator):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("aggregate_consulta_2",
                                 _message_handler=self.handle_message)

    def create_consumer(self):
        return Consumer("aggregate_consulta_2", _message_handler=self.handle_message)

    def create_producer(self):
        return Producer("result")

    def process_message(self, client_id, message):
        movies = message.get("movies", [])
        partial_results = defaultdict(float)
        for movie in movies:
            if movie:
                try:
                    country = movie.get("country", "")
                    budget = float(movie.get("budget", 0))
                    if country and budget:
                        partial_results[country] += budget
                    else:
                        self.logger.debug(f"Pelicula sin pais o presupuesto: {movie}")
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"Error procesando película: {e}")
                    continue

        return partial_results

    def aggregate_message(self, client_id, result):
        if not self.results.get(client_id):
            self.results[client_id] = result
        else:
            for country, budget in result.items():
                if country not in self.results[client_id]:
                    self.results[client_id][country] = 0.0
                self.results[client_id][country] += budget

    def create_final_result(self, client_id):
        top_5_countries = self._get_top_5_countries(client_id)
        return {
            "result_number": 2,
            "type": "query_2_top_5",
            "result": top_5_countries,
            "client_id": client_id
        }

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


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()

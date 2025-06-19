from middleware.consumer.consumer import Consumer
from middleware.producer.publisher import Publisher
from worker.abstractaggregator.abstractaggregator import AbstractAggregator


class Aggregator(AbstractAggregator):
    def __init__(self):
        super().__init__()

    def create_consumer(self):
        return Consumer("20_century_batch_results", _message_handler=self.handle_message)

    def create_producer(self):
        return Publisher("20_century_arg_result")

    def process_message(self, client_id, message):
        return message.get("movies", [])

    def aggregate_message(self, client_id, result):
        if not self.results.get(client_id):
            self.results[client_id] = result
        else:
            self.results[client_id].extend(result)

    def create_final_result(self, client_id, batch_id):
        return {
            "type": "20_century_arg_total_result",
            "movies": self.results[client_id],
            "total_movies": len(self.results[client_id]),
            "client_id": client_id,
            "batch_id": batch_id
        }


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()

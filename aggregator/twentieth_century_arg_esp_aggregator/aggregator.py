from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.abstractaggregator.abstractaggregator import AbstractAggregator


class Aggregator(AbstractAggregator):
    def __init__(self):
        super().__init__()

    def create_consumer(self):
        return Consumer("aggregate_consulta_1", _message_handler=self.handle_message)

    def create_producer(self):
        return Producer("result")

    def process_message(self, client_id, message):
        return message.get("movies", [])

    def aggregate_message(self, client_id, result):
        if not self.results.get(client_id):
            self.results[client_id] = result
        else:
            self.results[client_id].extend(result)

    def create_final_result(self, client_id):
        return {
            "result_number": 1,
            "type": "query_1_arg_esp_2000",
            "result": self.results[client_id],
            "total_movies": len(self.results[client_id]),
            "client_id": client_id
        }


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()

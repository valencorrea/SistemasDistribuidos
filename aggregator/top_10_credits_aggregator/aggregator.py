import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker



from collections import Counter, defaultdict

class Aggregator(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("top_10_actors_from_batch", _message_handler=self.handle_message)
        self.producer = Producer("result")
        self.total_batches_per_client = defaultdict(int)
        self.received_batches_per_client = defaultdict(int)
        self.actor_counter_per_client = defaultdict(Counter)

    def close(self):
        self.logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            self.logger.error(f"Error al cerrar conexiones: {e}")

    def handle_message(self, message):
        self.logger.info(f"Mensaje de top 10 parcial recibido {message}")
        client_id = message.get("client_id")
        batch_id = message.get("batch_id")
        actors = message.get("actors")
        self.logger.info(f"Se obtuvieron {len(actors)}: {actors} actores.")

        if message.get("processed_batches") is not None and message.get("batch_size") != 0:
            self.received_batches_per_client[client_id] = self.received_batches_per_client[client_id] + int(message.get("processed_batches"))
            self.logger.info(f"Se actualiza la cantidad recibida: {self.received_batches_per_client[client_id]}, actual: {self.received_batches_per_client[client_id]}.")

        if message.get("total_batches") is not None and message.get("total_batches") != 0:
            self.total_batches_per_client[client_id] = int(message.get("total_batches"))
            self.logger.info(f"Se envia la cantidad total de batches: {self.total_batches_per_client[client_id]}.")

        for _, count in actors:
            self.logger.info(f"Se va a aumentar la cantidad de registros de un actor: {count}: {type(count)}.")
            self.actor_counter_per_client[client_id][count["name"]] += count["count"]

        if self.total_batches_per_client[client_id] is not None and self.total_batches_per_client[client_id] != 0 and self.received_batches_per_client[client_id] >= self.total_batches_per_client[client_id]:
            # Top 10 final encontrado
            final_top_10 = self.actor_counter_per_client[client_id].most_common(10)
            self.producer.enqueue({
                "result_number": 4,
                "type": "top_10_actors",
                "actors": final_top_10,
                "client_id": client_id,
                "batch_id": batch_id
            })
            self.logger.info("Top 10 actores agregados y enviados.")
            self.actor_counter_per_client.pop(client_id)
            self.received_batches_per_client.pop(client_id)
            self.total_batches_per_client.pop(client_id)


    def start(self):
        self.logger.info("Iniciando agregador")
        try:
            self.consumer.start_consuming()
        finally:
            self.close()


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()
import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker

logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')

from collections import Counter

class Aggregator(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("top_10_actors_from_batch", _message_handler=self.handle_message)
        self.producer = Producer("result")
        self.total_batches = None
        self.received_batches = 0
        self.actor_counter = Counter()

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

    def handle_message(self, message):
        logger.info(f"Mensaje de top 10 parcial recibido {message}")
        actors = message.get("actors")
        logger.info(f"Se obtuvieron {len(actors)}: {actors} actores.")

        if message.get("batch_size") is not None and message.get("batch_size") != 0:
            self.received_batches = self.received_batches + int(message.get("batch_size"))
            logger.info(f"Se actualiza la cantidad recibida: {self.received_batches}, actual: {self.received_batches}.")

        if message.get("total_batches") is not None and message.get("total_batches") != 0:
            self.total_batches = int(message.get("total_batches"))
            logger.info(f"Se envia la cantidad total de batches: {self.total_batches}.")

        for _, count in actors:
            logger.info(f"Se va a aumentar la cantidad de registros de un actor: {count}: {type(count)}.")
            self.actor_counter[count["name"]] += count["count"]

        if self.total_batches is not None and self.received_batches >= self.total_batches:
            # Top 10 final encontrado
            final_top_10 = self.actor_counter.most_common(10)
            self.producer.enqueue({
                "type": "top_10_actors",
                "actors": final_top_10
            })
            logger.info("Top 10 actores agregados y enviados.")


    def start(self):
        logger.info("Iniciando agregador")
        try:
            self.consumer.start_consuming()
        finally:
            self.close()


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()
import logging

from middleware.consumer.consumer import Consumer
from collections import defaultdict
from middleware.producer.producer import Producer
from middleware.producer.publisher import Publisher
from worker.worker import Worker


logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')

class Aggregator(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("20_century_batch_results",
                                 _message_handler=self.handle_message)
        self.producer = Publisher("20_century_arg_result")
        self.filtered_movies_per_client = defaultdict(list)
        self.total_batches_per_client = defaultdict(int)
        self.received_batches_per_client = defaultdict(int)

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

    def handle_message(self, message):
        client_id = message.get("client_id")
        logger.info(f"Mensaje de batch de peliculas filtradas recibido: {len(message.get('movies', None))} peliculas del cliente {client_id}")
        if message.get("type") == "batch_result":
            # Acumular las películas del batch
            self.filtered_movies_per_client[client_id].extend(message.get("movies", []))
            self.received_batches_per_client[client_id] += message.get("batch_size", 0)
            total_batches = message.get("total_batches", 0)

            if total_batches != 0:
                self.total_batches_per_client[client_id] = total_batches
                logger.info(f"Recibida la cant total: {total_batches} del cliente {client_id}")

            logger.info(f"Batch procesado. Películas acumuladas: {len(self.filtered_movies_per_client[client_id])}")
            logger.info(f"Batches recibidos: {self.received_batches_per_client[client_id]}/{self.total_batches_per_client[client_id]}")

            # Sí hemos recibido todos los batches, enviar el resultado final
            if self.total_batches_per_client[client_id] and 0 < self.total_batches_per_client[client_id] <= self.received_batches_per_client[client_id]:
                result_message = {
                    "type": "20_century_arg_total_result",
                    "movies": self.filtered_movies_per_client[client_id],
                    "total_movies": len(self.filtered_movies_per_client[client_id]),
                    "client_id": client_id
                }
                if self.producer.enqueue(result_message):
                    logger.info(f"Resultado final enviado con {len(self.filtered_movies_per_client[client_id])} películas")
                    self.filtered_movies_per_client[client_id] = []
                    self.received_batches_per_client[client_id] = 0
                    self.total_batches_per_client[client_id] = 0

    def start(self):
        logger.info("Iniciando agregador")
        self.consumer.start_consuming()

if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start() 
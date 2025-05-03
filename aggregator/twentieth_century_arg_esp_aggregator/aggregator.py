import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker


logger = logging.getLogger(__name__)

class Aggregator(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("aggregate_consulta_1",
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

    def handle_message(self, message):
        if message.get("type") == "batch_result":
            client_id = message.get("client_id")
            if client_id not in self.results.keys():
                self.results[client_id] = []
                self.control_batches_per_client[client_id] = 0

            # Acumular las películas del batch
            self.results[client_id].extend(message.get("movies", []))
            self.control_batches_per_client[client_id] += message.get("batch_size", 0)

            if message.get("total_batches"):
                self.total_batches_per_client[client_id] = message.get("total_batches")

            logger.info(f"Batch procesado. Películas acumuladas: {len(self.results[client_id])} cliente {client_id}")
            logger.info(f"Batches recibidos: {self.control_batches_per_client[client_id]}/{self.total_batches_per_client[client_id]} cliente {client_id}")

            # Sí hemos recibido todos los batches, enviar el resultado final
            if self.total_batches_per_client[client_id] and 0 < self.total_batches_per_client[client_id] >= self.control_batches_per_client[client_id]:
                result_message = {
                    "result_number": 1,
                    "type": "query_1_arg_esp_2000",
                    "result": self.results[client_id],
                    "total_movies": len(self.results[client_id]),
                    "client_id": message.get("client_id")
                }
                if self.producer.enqueue(result_message):
                    logger.info(f"Resultado final enviado con {len(self.results[client_id])} películas")
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
import logging

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker


logger = logging.getLogger(__name__)

class Aggregator(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("20_century_batch_results",
                                 _message_handler=self.handle_message)  # Lee de la cola de resultados filtrados
        self.producer = Producer("20_century_arg_result")  # Envía el resultado final
        self.filtered_movies = []  # Almacena las películas filtradas
        self.total_batches = None
        self.received_batches = 0

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
            # Acumular las películas del batch
            self.filtered_movies.extend(message.get("movies", []))
            self.received_batches += message.get("batch_size", 0)

            if message.get("total_batches"):
                self.total_batches = message.get("total_batches")

            logger.info(f"Batch procesado. Películas acumuladas: {len(self.filtered_movies)}")
            logger.info(f"Batches recibidos: {self.received_batches}/{self.total_batches}")

            # Sí hemos recibido todos los batches, enviar el resultado final
            if self.total_batches and 0 < self.total_batches <= self.received_batches:
                result_message = {
                    "type": "20_century_arg_total_result",
                    "movies": self.filtered_movies,
                    "total_movies": len(self.filtered_movies)
                }
                if self.producer.enqueue(result_message):
                    logger.info(f"Resultado final enviado con {len(self.filtered_movies)} películas")

    def start(self):
        logger.info("Iniciando agregador")
        try:
            self.consumer.start_consuming()
        finally:
            self.close()

if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start() 
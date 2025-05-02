import logging
from collections import defaultdict

from middleware.consumer.consumer import Consumer
from middleware.producer.producer import Producer
from worker.worker import Worker


logger = logging.getLogger(__name__)


class Aggregator(Worker):
    def __init__(self):
        super().__init__()
        self.consumer = Consumer("aggregate_consulta_5",
                                 _message_handler=self.handle_message)  # Lee de la cola de resultados filtrados
        self.producer = Producer("result")  # Envía el resultado final
        self.total_batches = None
        self.received_batches = 0
        self.sentiment_revenue = defaultdict(float)  # Diccionario para sumar ganancias por senitmiento
        self.sentiment_budget = defaultdict(float)  # Diccionario para sumar presupuestos por sentimiento

    def close(self):
        logger.info("Cerrando conexiones del worker...")
        try:
            self.consumer.close()
            self.producer.close()
            self.shutdown_consumer.close()
        except Exception as e:
            logger.error(f"Error al cerrar conexiones: {e}")

    def process_sentiment_revenue_budget(self, movies):
        for movie in movies:
            if not movie:  # Si movie es None
                continue
            try:
                sentiment = movie.get("sentiment", "")
                revenue = float(movie.get("revenue", 0))
                budget = float(movie.get("budget", 0))

                if sentiment:  # Solo procesar si hay un país
                    self.sentiment_budget[sentiment] += budget
                    self.sentiment_revenue[sentiment] += revenue
            except (ValueError, TypeError) as e:
                logger.warning(f"Error procesando película: {e}")
                continue

    def _get_sentiment_mean(self):
        sentiment_mean = {}
        for sentiment in self.sentiment_budget:
            sentiment_mean[sentiment] = {
                "revenue": self.sentiment_revenue[sentiment] / self.sentiment_budget[sentiment]
            }
        return sentiment_mean

    def handle_message(self, message):
        if message.get("type") == "batch_result":
            # Procesar las películas del batch
            self.process_sentiment_revenue_budget(message.get("movies", []))
            self.received_batches += message.get("batch_size", 0)

            if message.get("total_batches"):
                self.total_batches = message.get("total_batches")

            logger.info(f"Batches recibidos: {self.received_batches}/{self.total_batches}")

            # Sí hemos recibido todos los batches, enviar el resultado final
            if self.total_batches and 0 < self.total_batches <= self.received_batches:
                rate_revenue_budget = self._get_sentiment_mean()
                result_message = {
                    "result_number": 5,
                    "type": "query_5_sentiments",
                    "result": rate_revenue_budget,
                    "client_id": message.get("client_id")
                }
                if self.producer.enqueue(result_message):
                    logger.info("Resultado final enviado con top 5 países")
                self.shutdown_event.set() # Hace falta esto?

    def start(self):
        logger.info("Iniciando agregador")
        try:
            self.consumer.start_consuming()
        finally:
            self.close()


if __name__ == '__main__':
    aggregator = Aggregator()
    aggregator.start()
